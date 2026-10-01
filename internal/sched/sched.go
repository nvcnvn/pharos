// Package sched admits requests, keeps inflight counts and the per-key fair
// queue, and hands out leases (ARCHITECTURE §8). Within one instance, picking
// a target and taking a slot on it is one atomic step.
package sched

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/state"
)

var (
	ErrUnknownModel = errors.New("no backend serves this model")
	ErrUnavailable  = errors.New("no backend serving this model is up")
	ErrQueueFull    = errors.New("queue full")
)

type Config struct {
	Policy policy.Config
	// DefaultCapacity is the slots per target when neither the engine nor the
	// backend's config says. Conservative: a target also counts as full as soon
	// as its engine reports waiting requests.
	DefaultCapacity int // default 2
	MaxQueue        int // waiters per instance; default 1000
	// OnPrefix gets every prefix record and correction, for peers. It runs
	// under the scheduler lock and must not block.
	OnPrefix func(prefix.Op)
}

// Request is what the scheduler needs to know about one request.
type Request struct {
	Model        string
	Kind         engine.Kind // only backends of this kind ("" = any): the native Ollama API needs Ollama
	Chain        []prefix.Link
	PromptTokens int
	Avoid        []uint16 // targets not to use: a retry after a failure there
	Weight       int      // the fair-queue key's share; 0 = 1
}

type Sched struct {
	st   *state.State
	px   *prefix.Index
	cfg  Config
	seed func() uint64 // breaks exact ties between targets

	// ponytail: global sched lock; per-model locks if routing throughput ever matters (>~5k rps).
	mu       sync.Mutex
	inflight map[uint16]int
	queues   map[string]*list.List // key -> FIFO of *waiter
	keys     []string              // keys with waiters, in round-robin order
	turns    map[string]int        // grants the key at the front had in its current turn
	waiting  map[string]int        // waiters per model
	queued   int
	peers    map[uint64]peerGauges // origin -> its last report
}

// Gauges are one instance's requests in flight per target key and waiters
// per model, sent to peers every sync tick.
type Gauges struct {
	Inflight map[string]int
	Waiting  map[string]int
}

type peerGauges struct {
	g  Gauges
	at time.Time
}

// peerSilence is how long a peer's report counts without a new one.
const peerSilence = 2 * time.Second

type waiter struct {
	req Request
	el  *list.Element
	key string
	ch  chan result // buffered: the scheduler never blocks on it
}

type result struct {
	l   *Lease
	err error
}

func New(st *state.State, px *prefix.Index, cfg Config) *Sched {
	if cfg.DefaultCapacity <= 0 {
		cfg.DefaultCapacity = 2
	}
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 1000
	}
	return &Sched{
		st: st, px: px, cfg: cfg, seed: rand.Uint64,
		inflight: map[uint16]int{}, queues: map[string]*list.List{}, turns: map[string]int{}, waiting: map[string]int{}, peers: map[uint64]peerGauges{},
	}
}

// Export returns this instance's gauges.
func (s *Sched) Export() Gauges {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := Gauges{Inflight: make(map[string]int, len(s.inflight)), Waiting: maps.Clone(s.waiting)}
	for id, n := range s.inflight {
		if t := s.st.Target(id); t != nil {
			g.Inflight[t.Key] += n
		}
	}
	return g
}

// ClusterInflight returns requests in flight per target key: this instance's
// plus what peers reported within the last 2 s. It is the occupancy routing
// counts, before an engine's own Running raises it.
func (s *Sched) ClusterInflight() map[string]int {
	g := s.Export()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.st.Now()
	for _, p := range s.peers {
		if now.Sub(p.at) <= peerSilence {
			for k, n := range p.g.Inflight {
				g.Inflight[k] += n
			}
		}
	}
	return g.Inflight
}

// Merge replaces a peer's last report. Its requests count in occupancy and
// queue length until it goes silent for 2 s. A peer's freed slot re-runs the
// queue.
func (s *Sched) Merge(origin uint64, g Gauges) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.st.Now()
	s.peers[origin] = peerGauges{g, now}
	maps.DeleteFunc(s.peers, func(_ uint64, p peerGauges) bool { return now.Sub(p.at) > peerSilence })
	s.drainLocked()
}

// Forget drops a peer's report at once: it is leaving.
func (s *Sched) Forget(origin uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.peers, origin)
	s.drainLocked()
}

// Lease is a slot on a target for one request. Release it exactly once;
// later calls do nothing.
type Lease struct {
	Target *state.Target
	Reason string // why this target: for logs and the status page
	Why    string // Reason as a fixed category, for metrics (policy.Decision.Why)
	Cold   bool   // dispatched while the model wasn't loaded
	Queued bool   // waited in the fair queue for its slot
	// PrefixSource is where the prefix index's prediction came from
	// (prefix.Source: local, peer, restored); "" = no prediction.
	PrefixSource string

	s         *Sched
	req       Request
	predicted int // cached tokens the prefix index predicted
	released  bool
}

// Feedback is what the proxy learned from the request.
type Feedback struct {
	OK       bool // the engine served it; false: error, cancel or engine death
	Refused  bool // the engine answered an error status: it didn't take the prompt in
	Streamed bool
	TTFT     time.Duration // to the first body byte
	Duration time.Duration
	Usage    engine.Usage
}

// Acquire waits for a slot on the best target for r, in key's fair-queue turn.
// It fails at once when no up backend serves the model, and when ctx ends.
func (s *Sched) Acquire(ctx context.Context, key string, r Request) (*Lease, error) {
	w := &waiter{req: r, key: key, ch: make(chan result, 1)}
	s.mu.Lock()
	if s.queued >= s.cfg.MaxQueue {
		s.mu.Unlock()
		return nil, ErrQueueFull
	}
	q := s.queues[key]
	if q == nil {
		q = list.New()
		s.queues[key] = q
		s.keys = append(s.keys, key)
	}
	if len(r.Avoid) > 0 {
		w.el = q.PushFront(w) // a retry: it already waited its turn
	} else {
		w.el = q.PushBack(w)
	}
	s.waiting[r.Model]++
	s.queued++
	s.drainLocked()
	waited := w.el != nil
	s.mu.Unlock()

	select {
	case res := <-w.ch:
		if res.l != nil {
			res.l.Queued = waited
		}
		return res.l, res.err
	case <-ctx.Done():
		s.mu.Lock()
		queued := w.el != nil
		if queued {
			s.dequeueLocked(w)
		}
		s.mu.Unlock()
		if !queued { // granted meanwhile
			if res := <-w.ch; res.l != nil {
				res.l.Release(Feedback{})
			}
		}
		return nil, ctx.Err()
	}
}

// Release returns the slot and applies the request's feedback: speed
// estimates, and a prefix correction when the engine had far less cached than
// predicted. It returns how the prediction compared with the engine's cached
// tokens, for metrics: "predicted_hit", "wrong_prediction" (corrected),
// "unpredicted_hit", "miss" or "unknown" (not reported); "" when the request
// failed or the lease was already released.
func (l *Lease) Release(fb Feedback) (prefixOutcome string) {
	s := l.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.released {
		return ""
	}
	l.released = true
	if s.inflight[l.Target.ID]--; s.inflight[l.Target.ID] <= 0 {
		delete(s.inflight, l.Target.ID)
	}
	if fb.Refused && len(l.req.Chain) > 0 {
		// An engine that refuses a prompt would win it again by affinity: an
		// older record of it, refreshed by each success, would never age out.
		s.px.Remove(l.req.Chain, l.Target.ID)
		s.emit(prefix.Op{Target: l.Target.Key, Hashes: prefix.Hashes(l.req.Chain), Remove: true})
	}
	if fb.OK && len(l.req.Chain) > 0 {
		// The engine holds the prompt for another request only now: Ollama
		// and llama.cpp cache per slot, so a request sent while this one
		// still prefilled or decoded prefilled it again (ARCHITECTURE §6).
		now := s.st.Now()
		s.px.Record(l.req.Chain, l.Target.ID, l.Target.Gen(), now)
		s.emit(prefix.Op{Target: l.Target.Key, Hashes: prefix.Hashes(l.req.Chain), Used: now.UnixNano()})
	}
	if fb.OK {
		c := fb.Usage.CachedTokens
		switch {
		case !c.OK:
			prefixOutcome = "unknown"
		case l.predicted >= minCorrection && c.V < l.predicted/2:
			prefixOutcome = "wrong_prediction"
			s.px.Remove(l.req.Chain, l.Target.ID)
			s.emit(prefix.Op{Target: l.Target.Key, Hashes: prefix.Hashes(l.req.Chain), Remove: true})
		case l.predicted >= minCorrection:
			prefixOutcome = "predicted_hit"
		case c.V >= minCorrection:
			prefixOutcome = "unpredicted_hit"
		default:
			prefixOutcome = "miss"
		}
		l.Target.Observe(state.Observation{At: s.st.Now(), Cold: l.Cold, Streamed: fb.Streamed, TTFT: fb.TTFT, Duration: fb.Duration, Usage: fb.Usage})
	}
	s.drainLocked()
	return prefixOutcome
}

// minCorrection: a predicted match shorter than this isn't worth correcting.
const minCorrection = 64

// Kick re-runs the queue, e.g. after a scrape saw the engine free slots.
func (s *Sched) Kick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drainLocked()
}

// Waiting returns how many requests wait in the queue.
func (s *Sched) Waiting() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queued
}

func (s *Sched) dequeueLocked(w *waiter) {
	s.queues[w.key].Remove(w.el)
	w.el = nil
	if s.waiting[w.req.Model]--; s.waiting[w.req.Model] <= 0 {
		delete(s.waiting, w.req.Model)
	}
	s.queued--
}

// drainLocked serves waiters round-robin across keys, weighted: each pass
// gives each key at most one grant, the first of its waiters that can run,
// until a pass grants nothing. A key keeps going first until it had as many
// grants as its weight (deficit round-robin, with one slot freed at a time
// in mind). Waiters whose model has no up backend fail.
func (s *Sched) drainLocked() {
	for progress := true; progress && len(s.keys) > 0; {
		progress = false
		first := 0 // index of the key that goes first after this pass
		for i, key := range s.keys {
			for e := s.queues[key].Front(); e != nil; {
				w, next := e.Value.(*waiter), e.Next()
				l, err := s.tryLocked(w.req)
				if l == nil && err == nil {
					e = next
					continue // no slot for it yet
				}
				s.dequeueLocked(w)
				w.ch <- result{l, err}
				progress = true
				if l != nil {
					if s.turns[key]++; s.turns[key] < max(w.req.Weight, 1) {
						first = i
					} else {
						s.turns[key], first = 0, i+1
					}
					break
				}
				e = next
			}
		}
		// Keys without waiters leave the rotation.
		keys := append(slices.Clone(s.keys[first:]), s.keys[:first]...)
		s.keys = slices.DeleteFunc(keys, func(k string) bool {
			if s.queues[k].Len() > 0 {
				return false
			}
			delete(s.queues, k)
			delete(s.turns, k)
			return true
		})
	}
}

// tryLocked picks a target for r and takes a slot on it. It returns no lease
// and no error when r must wait: the best target is full, or every target is
// cold without memory to load while another model is busy.
func (s *Sched) tryLocked(r Request) (*Lease, error) {
	now := s.st.Now()
	targets := s.st.Targets(r.Model)
	if len(targets) == 0 {
		return nil, ErrUnknownModel
	}
	byID := map[uint16]*state.Target{}
	var cands []policy.Candidate
	for _, t := range targets {
		byID[t.ID] = t
	}
	// Peers' requests, from reports newer than peerSilence.
	peerWaiting := 0
	var fresh []Gauges
	for _, p := range s.peers {
		if now.Sub(p.at) <= peerSilence {
			fresh = append(fresh, p.g)
			peerWaiting += p.g.Waiting[r.Model]
		}
	}
	var matched map[uint16]prefix.Match
	if len(r.Chain) > 0 {
		matched = s.px.Lookup(r.Chain, func(id uint16) uint32 {
			if t, ok := byID[id]; ok {
				return t.Gen()
			}
			return math.MaxUint32 // not a candidate
		})
	}
	for _, t := range targets {
		v := t.View(now)
		if !v.Up || r.Kind != "" && v.Kind != r.Kind || slices.Contains(r.Avoid, t.ID) {
			continue
		}
		capacity := s.cfg.DefaultCapacity
		switch {
		case v.Capacity.OK && v.Capacity.V > 0:
			capacity = v.Capacity.V
		case t.Backend.Spec.Capacity > 0:
			capacity = t.Backend.Spec.Capacity
		}
		peerInflight := 0
		for _, g := range fresh {
			peerInflight += g.Inflight[t.Key]
		}
		occupied := s.inflight[t.ID] + peerInflight
		if v.Running.OK && v.Running.V > occupied {
			occupied = v.Running.V // clients that bypass Pharos, and peers cut off from us
		}
		free := max(capacity-occupied, 0)
		ahead := s.waiting[r.Model] - 1 + peerWaiting // other waiters for the model, here and on peers
		if v.Waiting.OK {
			ahead += v.Waiting.V
			if v.Waiting.V > 0 {
				free = 0
			}
		}
		prefill, load, service := t.Estimates()
		cands = append(cands, policy.Candidate{
			TargetID:      t.ID,
			Warm:          warm(v.Residency),
			Capacity:      capacity,
			FreeSlots:     free,
			QueueAhead:    max(ahead, 0),
			KVUsage:       policy.Opt[float64](v.KVUsage),
			MatchedTokens: matched[t.ID].Bytes / bytesPerToken,
			PrefillSecTok: policy.Opt[float64](prefill),
			LoadSec:       policy.Opt[float64](load),
			ServiceSec:    policy.Opt[float64](service),
			FitsIfCold:    s.fitsLocked(t, v, now),
			Loading:       v.Residency == engine.Loading || v.Residency == engine.Cold && occupied > 0,
		})
	}
	if len(cands) == 0 {
		return nil, ErrUnavailable
	}
	d := policy.Pick(policy.RouteReq{PromptTokens: r.PromptTokens, Seed: s.seed()}, cands, s.cfg.Policy)
	if !d.OK || d.Enqueue {
		return nil, nil // wait: for a slot, or for a busy model to become evictable
	}
	t := byID[d.TargetID]
	c := cands[slices.IndexFunc(cands, func(c policy.Candidate) bool { return c.TargetID == d.TargetID })]
	s.inflight[t.ID]++
	l := &Lease{Target: t, Reason: d.Reason, Why: d.Why, Cold: c.Warm.OK && !c.Warm.V, s: s, req: r, predicted: c.MatchedTokens}
	if l.predicted >= minCorrection {
		l.PrefixSource = matched[t.ID].Source.String()
	}
	return l, nil
}

func (s *Sched) emit(op prefix.Op) {
	if s.cfg.OnPrefix != nil {
		s.cfg.OnPrefix(op)
	}
}

// bytesPerToken converts matched prefix bytes to tokens. A known
// approximation; the cost model only needs relative values.
const bytesPerToken = 4

func warm(r engine.ResidencyState) policy.Opt[bool] {
	switch r {
	case engine.Loaded:
		return policy.Opt[bool]{V: true, OK: true}
	case engine.Loading, engine.Cold:
		return policy.Opt[bool]{V: false, OK: true}
	}
	return policy.Opt[bool]{} // not reported: the engine's model is always loaded
}

// fitsLocked says whether a cold model fits in its host's memory next to the
// models that are loaded and busy. Loaded models with nothing in flight can be
// evicted, so they don't count. Unknown without configured memory or sizes.
// ponytail: coarse memory model (model file sizes); refine with real VRAM math if users hit OOM or evictions.
func (s *Sched) fitsLocked(t *state.Target, v state.View, now time.Time) policy.Opt[bool] {
	mem := t.Backend.Spec.MemoryBytes
	if v.Residency != engine.Cold || mem <= 0 || !v.SizeBytes.OK {
		return policy.Opt[bool]{}
	}
	used := int64(0)
	for _, o := range t.Backend.Targets() {
		if o == t || s.inflight[o.ID] == 0 {
			continue
		}
		ov := o.View(now)
		if ov.Residency != engine.Loaded {
			continue
		}
		size := ov.SizeBytes
		if !size.OK {
			size = ov.VRAMBytes
		}
		if !size.OK {
			return policy.Opt[bool]{}
		}
		used += size.V
	}
	return policy.Opt[bool]{V: v.SizeBytes.V <= mem-used, OK: true}
}

// String is for debugging.
func (l *Lease) String() string { return fmt.Sprintf("%s (%s)", l.Target.Key, l.Reason) }

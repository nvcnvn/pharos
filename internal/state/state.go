// Package state holds what Pharos knows about each target: the latest engine
// signals from the scrape loop, generations, and speed estimates from request
// feedback (ARCHITECTURE §5). Readers never block: each backend publishes an
// immutable view through an atomic pointer.
package state

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
)

// BackendSpec is one engine endpoint to scrape and route to.
type BackendSpec struct {
	URL         string
	Kind        engine.Kind    // "" = engine.Auto
	Own         []engine.Probe // operator's own probes, ahead of the recipe
	MemoryBytes int64          // host memory for models; 0 = unknown (Ollama doesn't report it)
	Capacity    int            // slots per target when the engine doesn't report them; 0 = unset
}

type Options struct {
	Client  *http.Client
	Now     func() time.Time
	Fast    time.Duration // interval for Running, Waiting and KVUsage (default 1 s)
	Slow    time.Duration // interval for every other signal (default 5 s)
	Resolve time.Duration // re-resolve the plan this often (default 10 min)
	// OnUpdate runs after each scrape round, e.g. to let the scheduler's queue
	// see slots the engine freed.
	OnUpdate func()
}

type State struct {
	opts     Options
	backends []*Backend
	mu       sync.Mutex // guards targets and byKey
	targets  []*Target  // index = ID
	byKey    map[string]*Target
	ready    atomic.Int32 // backends that finished a first round
}

type Backend struct {
	Spec    BackendSpec
	view    atomic.Pointer[view]
	ejected atomic.Bool
	fastTTL time.Duration
	slowTTL time.Duration

	// Owned by the backend's scrape goroutine.
	plan, fastPlan engine.Plan
	resolved       time.Time
	round          int
	started        bool
	failing        bool // logged as failing; logged again once it answers
}

// view is one published state of a backend.
type view struct {
	kind    engine.Kind
	slow    engine.Snapshot // the last full round
	fast    engine.Snapshot // the last round that read Running, Waiting and KVUsage
	targets []*Target       // one per model the backend lists
}

// Target is one backend × model: the unit Pharos routes to.
type Target struct {
	ID      uint16
	Key     string // backend URL + model: the name peers use
	Backend *Backend
	Model   string
	gen     atomic.Uint32

	mu                     sync.Mutex
	prefill, load, service ewma
}

// staleAfter is how many intervals a scraped signal stays known without a new read.
const staleAfter = 3

func New(specs []BackendSpec, opts Options) *State {
	if opts.Client == nil {
		opts.Client = http.DefaultClient
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	opts.Fast = cmp(opts.Fast, time.Second)
	opts.Slow = max(cmp(opts.Slow, 5*time.Second), opts.Fast)
	opts.Resolve = cmp(opts.Resolve, 10*time.Minute)
	s := &State{opts: opts, byKey: map[string]*Target{}}
	for _, spec := range specs {
		if spec.Kind == "" {
			spec.Kind = engine.Auto
		}
		s.backends = append(s.backends, &Backend{Spec: spec, fastTTL: staleAfter * opts.Fast, slowTTL: staleAfter * opts.Slow})
	}
	return s
}

func cmp(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// Run scrapes every backend on its interval until ctx is done.
func (s *State) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range s.backends {
		wg.Go(func() {
			for {
				s.round(ctx, b)
				jitter := time.Duration(rand.Int64N(int64(s.opts.Fast)/10 + 1))
				select {
				case <-ctx.Done():
					return
				case <-time.After(s.opts.Fast + jitter):
				}
			}
		})
	}
	wg.Wait()
}

// ScrapeAll runs one round for every backend, in turn. Run does the same on a
// timer; tests call this to step the clock by hand.
func (s *State) ScrapeAll(ctx context.Context) {
	for _, b := range s.backends {
		s.round(ctx, b)
	}
}

// Ready reports whether every backend finished its first round, so routing
// isn't blind.
func (s *State) Ready() bool { return int(s.ready.Load()) == len(s.backends) }

// Now is the clock signals are stamped with.
func (s *State) Now() time.Time { return s.opts.Now() }

// Backends returns every configured backend.
func (s *State) Backends() []*Backend { return s.backends }

// round resolves the plan when due, else scrapes: every probe once per slow
// interval, and only the Running, Waiting and KVUsage probes in between.
func (s *State) round(ctx context.Context, b *Backend) {
	defer func() {
		if !b.started {
			b.started = true
			s.ready.Add(1)
		}
		if s.opts.OnUpdate != nil {
			s.opts.OnUpdate()
		}
	}()
	perSlow := int(s.opts.Slow / s.opts.Fast)
	full := b.round%perSlow == 0
	b.round++
	now := s.opts.Now()
	url := b.Spec.URL

	if b.resolved.IsZero() || now.Sub(b.resolved) >= s.opts.Resolve {
		ctx, cancel := context.WithTimeout(ctx, s.opts.Slow)
		defer cancel()
		plan, snap, err := engine.Resolve(ctx, s.opts.Client, url, b.Spec.Kind, b.Spec.Own, false)
		if err != nil {
			b.fail("resolve", err)
			return
		}
		b.plan, b.resolved = plan, now
		b.fastPlan = engine.Plan{Kind: plan.Kind, Version: plan.Version}
		for _, p := range plan.Active {
			if p.Signal == engine.Running || p.Signal == engine.Waiting || p.Signal == engine.KVUsage {
				b.fastPlan.Active = append(b.fastPlan.Active, p)
			}
		}
		snap.At = now
		s.publish(b, snap, snap)
		return
	}
	if !full && len(b.fastPlan.Active) == 0 {
		return
	}
	plan, ttl := b.plan, s.opts.Slow
	if !full {
		plan, ttl = b.fastPlan, s.opts.Fast
	}
	ctx, cancel := context.WithTimeout(ctx, ttl)
	defer cancel()
	snap, err := plan.Scrape(ctx, s.opts.Client, url)
	if err != nil {
		b.resolved = time.Time{} // a planned probe failed: resolve again next round
		if len(snap.From) == 0 {
			b.fail("scrape", err)
			return // unreachable: keep the last view until it goes stale
		}
	}
	snap.At = now
	if full {
		if snap.Version != b.plan.Version {
			b.resolved = time.Time{} // new engine version: resolve again next round
		}
		s.publish(b, snap, snap)
	} else if old := b.view.Load(); old != nil {
		s.publish(b, old.slow, snap)
	}
}

// fail logs the first of a run of failed rounds.
func (b *Backend) fail(what string, err error) {
	if !b.failing {
		b.failing = true
		slog.Warn(what+" failed; retrying every round", "backend", b.Spec.URL, "err", err)
	}
}

// publish swaps in a new view. A model that went cold or disappeared, or a new
// engine version, bumps the target's generation, which invalidates its prefix
// entries.
func (s *State) publish(b *Backend, slow, fast engine.Snapshot) {
	b.ejected.Store(false) // it answered
	if b.failing {
		b.failing = false
		slog.Info("backend answers again", "backend", b.Spec.URL)
	}
	v := &view{kind: b.plan.Kind, slow: slow, fast: fast}
	for model := range slow.Models {
		if t := s.target(b, model); t != nil {
			v.targets = append(v.targets, t)
		}
	}
	if old := b.view.Load(); old != nil && old.slow.At != slow.At {
		for _, t := range old.targets {
			was := old.slow.Models[t.Model].State
			m, listed := slow.Models[t.Model]
			if !listed || was != engine.Cold && m.State == engine.Cold || old.slow.Version != slow.Version {
				t.gen.Add(1)
			}
		}
	}
	b.view.Store(v)
}

// target returns the target for b × model, registering it on first sight.
// Targets are never removed, so IDs stay stable.
func (s *State) target(b *Backend, model string) *Target {
	key := b.Spec.URL + " " + model
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.byKey[key]; ok {
		return t
	}
	if len(s.targets) > 0xFFFF {
		slog.Error("too many targets", "key", key) // ponytail: IDs are uint16 and never reused
		return nil
	}
	t := &Target{ID: uint16(len(s.targets)), Key: key, Backend: b, Model: model}
	s.targets = append(s.targets, t)
	s.byKey[key] = t
	return t
}

// Targets returns every target of the model, up or not.
func (s *State) Targets(model string) []*Target {
	var ts []*Target
	for _, b := range s.backends {
		if v := b.view.Load(); v != nil {
			for _, t := range v.targets {
				if t.Model == model {
					ts = append(ts, t)
				}
			}
		}
	}
	return ts
}

// Target returns the target with id, or nil.
func (s *State) Target(id uint16) *Target {
	s.mu.Lock()
	defer s.mu.Unlock()
	if int(id) < len(s.targets) {
		return s.targets[id]
	}
	return nil
}

// Models lists each model an up backend serves, with the kinds serving it.
func (s *State) Models(now time.Time) map[string][]engine.Kind {
	models := map[string][]engine.Kind{}
	for _, b := range s.backends {
		v := b.view.Load()
		if v == nil || !b.up(v, now) {
			continue
		}
		for _, t := range v.targets {
			models[t.Model] = append(models[t.Model], v.kind)
		}
	}
	return models
}

// Targets returns one target per model the backend lists.
func (b *Backend) Targets() []*Target {
	if v := b.view.Load(); v != nil {
		return v.targets
	}
	return nil
}

// Eject takes the backend out of routing until it answers a scrape again.
func (b *Backend) Eject() { b.ejected.Store(true) }

// Kind is the backend's engine kind as resolved, or its configured kind before that.
func (b *Backend) Kind() engine.Kind {
	if v := b.view.Load(); v != nil {
		return v.kind
	}
	return b.Spec.Kind
}

func (b *Backend) up(v *view, now time.Time) bool {
	return !b.ejected.Load() && now.Sub(v.slow.At) <= b.slowTTL
}

// View is a target's signals as of now. A signal not read within 3 of its
// scrape intervals is unknown.
type View struct {
	Up        bool // listed by a backend that answered recently and isn't ejected
	Kind      engine.Kind
	Residency engine.ResidencyState
	VRAMBytes engine.Opt[int64]
	SizeBytes engine.Opt[int64]
	Running   engine.Opt[int]
	Waiting   engine.Opt[int]
	Capacity  engine.Opt[int]
	KVUsage   engine.Opt[float64]
}

func (t *Target) View(now time.Time) View {
	b := t.Backend
	v := b.view.Load()
	if v == nil {
		return View{Kind: b.Spec.Kind}
	}
	m, listed := v.slow.Models[t.Model]
	out := View{Kind: v.kind, Up: listed && b.up(v, now)}
	if !out.Up {
		return out
	}
	out.Residency, out.VRAMBytes, out.SizeBytes = m.State, m.VRAMBytes, m.SizeBytes
	out.Capacity = load(v.slow, t.Model).Capacity
	if now.Sub(v.fast.At) <= b.fastTTL {
		l := load(v.fast, t.Model)
		out.Running, out.Waiting, out.KVUsage = l.Running, l.Waiting, l.KVUsage
	}
	return out
}

// load is the model's occupancy, else the whole backend's.
// ponytail: a backend-wide value is given to each of its targets; split it
// when a multi-model engine (llama.cpp router mode) reports only backend totals.
func load(s engine.Snapshot, model string) engine.Load {
	if l, ok := s.Load[model]; ok {
		return l
	}
	return s.Load[""]
}

// Gen is the target's generation: bumped when its model is unloaded.
func (t *Target) Gen() uint32 { return t.gen.Load() }

// ewma is an exponentially weighted moving average. No samples = unknown.
type ewma struct {
	v float64
	n int
}

const alpha = 0.2

func (e *ewma) add(x float64) {
	if e.n == 0 {
		e.v = x
	} else {
		e.v += alpha * (x - e.v)
	}
	e.n++
}

func (e ewma) get() engine.Opt[float64] { return engine.Opt[float64]{V: e.v, OK: e.n > 0} }

// Observation is what one finished request tells about its target.
type Observation struct {
	Cold     bool // dispatched while the model wasn't loaded
	Streamed bool // TTFT is time to the first token, not to the whole reply
	TTFT     time.Duration
	Duration time.Duration
	Usage    engine.Usage
}

// minPrefillTokens: a nearly fully cached prompt measures fixed overhead, not
// prefill speed (llama.cpp reports 20–100 ms for 1 uncached token, captures).
const minPrefillTokens = 256

// Observe updates the target's speed estimates from a successful request.
func (t *Target) Observe(o Observation) {
	t.mu.Lock()
	defer t.mu.Unlock()
	u := o.Usage
	if o.Cold {
		switch {
		case u.LoadSec.OK:
			t.load.add(u.LoadSec.V)
		case o.Streamed:
			t.load.add(o.TTFT.Seconds())
		}
	} else {
		t.service.add(o.Duration.Seconds())
	}
	if !u.PromptTokens.OK || !u.CachedTokens.OK {
		return // uncached count unknown
	}
	uncached := float64(u.PromptTokens.V - u.CachedTokens.V)
	switch {
	case uncached < minPrefillTokens:
	case u.PrefillSec.OK:
		t.prefill.add(u.PrefillSec.V / uncached)
	case o.Streamed && !o.Cold:
		t.prefill.add(o.TTFT.Seconds() / uncached)
	}
}

// Estimates returns the target's measured prefill seconds per token, load
// seconds and request seconds, each unknown until a request measures it.
func (t *Target) Estimates() (prefillSecTok, loadSec, serviceSec engine.Opt[float64]) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.prefill.get(), t.load.get(), t.service.get()
}

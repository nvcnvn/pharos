// Package state holds what Pharos knows about each target: the latest engine
// signals from the scrape loop, generations, and speed estimates from request
// feedback (ARCHITECTURE §5). Readers never block: each backend publishes an
// immutable view through an atomic pointer.
package state

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"slices"
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
	Logs        string         // the log feed Options.OpenLogs opens (a Docker container); "" = none
}

// same reports whether two specs describe the same backend setup. Own probes
// compare by name.
func (a BackendSpec) same(b BackendSpec) bool {
	return a.URL == b.URL && a.Kind == b.Kind && a.MemoryBytes == b.MemoryBytes && a.Capacity == b.Capacity && a.Logs == b.Logs &&
		slices.EqualFunc(a.Own, b.Own, func(x, y engine.Probe) bool { return x.Name == y.Name })
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
	// OpenLogs opens a backend's log feed (BackendSpec.Logs), following it.
	// nil = no backend has a feed, so log probes drop.
	OpenLogs func(ctx context.Context, logs string) (io.ReadCloser, error)
}

type State struct {
	opts     Options
	backends atomic.Pointer[[]*Backend]
	changed  chan struct{} // SetBackends → Run
	ready    atomic.Bool   // latched once the first backends finished a round

	mu       sync.Mutex // guards targets, byKey, restored, gone and SetBackends
	gone     []*Backend // recently removed, oldest first: a container restart brings its backend back
	targets  []*Target  // index = ID
	byKey    map[string]*Target
	restored map[string]TargetStats // merged stats of targets not seen yet
}

type Backend struct {
	Spec    BackendSpec
	view    atomic.Pointer[view]
	logs    atomic.Pointer[engine.Snapshot] // values from the log feed; nil while it is disconnected
	logPlan atomic.Pointer[engine.Plan]     // the plan, while it has log probes
	ejected atomic.Bool
	removed atomic.Bool // left the backend set: no new leases, in-flight requests finish
	started atomic.Bool // finished a first round
	fastTTL time.Duration
	slowTTL time.Duration

	// Owned by the backend's scrape goroutine.
	plan, fastPlan engine.Plan
	resolved       time.Time
	round          int
	failing        bool // logged as failing; logged again once it answers
	following      bool // the log follower runs
	followers      sync.WaitGroup
}

// maxGone bounds how many removed backends are kept for a comeback.
const maxGone = 32

// view is one published state of a backend.
type view struct {
	kind    engine.Kind
	plan    engine.Plan
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
	s := &State{opts: opts, byKey: map[string]*Target{}, restored: map[string]TargetStats{}, changed: make(chan struct{}, 1)}
	s.backends.Store(&[]*Backend{})
	s.SetBackends(specs)
	return s
}

// SetBackends replaces the backend set, e.g. when discovery sees a container
// start or stop. A backend whose spec is unchanged keeps its state, also when
// it comes back after a removal (a container restart). A new one starts
// scraping; a removed one stops and takes no new leases, while its in-flight
// requests finish.
func (s *State) SetBackends(specs []BackendSpec) {
	s.mu.Lock()
	old := s.Backends()
	next := make([]*Backend, 0, len(specs))
	urls := map[string]bool{}
	for _, spec := range specs {
		if urls[spec.URL] {
			// Two backends on one URL would share target keys.
			slog.Warn("backend listed twice; keeping the first", "backend", spec.URL)
			continue
		}
		urls[spec.URL] = true
		if spec.Kind == "" {
			spec.Kind = engine.Auto
		}
		same := func(b *Backend) bool { return b.Spec.same(spec) }
		if i := slices.IndexFunc(old, same); i >= 0 {
			next = append(next, old[i])
		} else if i := slices.IndexFunc(s.gone, same); i >= 0 {
			b := s.gone[i]
			s.gone = slices.Delete(s.gone, i, i+1)
			b.removed.Store(false)
			next = append(next, b)
		} else {
			next = append(next, &Backend{Spec: spec, fastTTL: staleAfter * s.opts.Fast, slowTTL: staleAfter * s.opts.Slow})
		}
	}
	for _, b := range old {
		if !slices.Contains(next, b) {
			b.removed.Store(true)
			s.gone = append(s.gone, b)
		}
	}
	if len(s.gone) > maxGone {
		s.gone = slices.Delete(s.gone, 0, len(s.gone)-maxGone)
	}
	s.backends.Store(&next)
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func cmp(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// Run scrapes every backend on its interval until ctx is done, starting and
// stopping a goroutine per backend as SetBackends changes the set.
func (s *State) Run(ctx context.Context) {
	type worker struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	running, stopping := map[*Backend]worker{}, map[*Backend]worker{}
	for {
		want := s.Backends()
		for _, b := range want {
			if _, ok := running[b]; ok {
				continue
			}
			if w, ok := stopping[b]; ok {
				<-w.done // back after a removal: its old goroutines own its scrape state until they end
				delete(stopping, b)
			}
			bctx, cancel := context.WithCancel(ctx)
			w := worker{cancel, make(chan struct{})}
			running[b] = w
			wg.Go(func() {
				defer close(w.done)
				defer func() { b.followers.Wait(); b.following = false }()
				for {
					s.round(bctx, b)
					jitter := time.Duration(rand.Int64N(int64(s.opts.Fast)/10 + 1))
					select {
					case <-bctx.Done():
						return
					case <-time.After(s.opts.Fast + jitter):
					}
				}
			})
		}
		for b, w := range running {
			if !slices.Contains(want, b) {
				w.cancel()
				delete(running, b)
				stopping[b] = w
			}
		}
		for b, w := range stopping {
			select {
			case <-w.done:
				delete(stopping, b)
			default:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-s.changed:
		}
	}
}

// ScrapeAll runs one round for every backend, in turn. Run does the same on a
// timer; tests call this to step the clock by hand. A log follower started by
// a round lives until ctx is done.
func (s *State) ScrapeAll(ctx context.Context) {
	for _, b := range s.Backends() {
		s.round(ctx, b)
	}
}

// Ready reports whether the backends finished a first round, so routing isn't
// blind. Once true it stays true: a backend discovered later doesn't make the
// instance unready.
func (s *State) Ready() bool {
	if s.ready.Load() {
		return true
	}
	for _, b := range s.Backends() {
		if !b.started.Load() {
			return false
		}
	}
	s.ready.Store(true)
	return true
}

// Now is the clock signals are stamped with.
func (s *State) Now() time.Time { return s.opts.Now() }

// Backends returns the current backend set.
func (s *State) Backends() []*Backend { return *s.backends.Load() }

// round resolves the plan when due, else scrapes: every probe once per slow
// interval, and only the Running, Waiting and KVUsage probes in between.
func (s *State) round(ctx context.Context, b *Backend) {
	defer func() {
		b.started.Store(true)
		s.Ready() // latches
		if s.opts.OnUpdate != nil {
			s.opts.OnUpdate()
		}
	}()
	defer func() {
		// Also after a comeback: the follower ended with the old scrape goroutine.
		if !b.following && b.logPlan.Load() != nil {
			b.following = true
			b.followers.Go(func() { s.follow(ctx, b) })
		}
	}()
	perSlow := int(s.opts.Slow / s.opts.Fast)
	full := b.round%perSlow == 0
	b.round++
	now := s.opts.Now()
	url := b.Spec.URL

	if b.resolved.IsZero() || now.Sub(b.resolved) >= s.opts.Resolve {
		rctx, cancel := context.WithTimeout(ctx, s.opts.Slow)
		defer cancel()
		feed := b.Spec.Logs != "" && s.opts.OpenLogs != nil
		plan, snap, err := engine.Resolve(rctx, s.opts.Client, url, b.Spec.Kind, b.Spec.Own, feed)
		if err != nil {
			b.fail("resolve", err)
			return
		}
		b.plan, b.resolved = plan, now
		if slices.ContainsFunc(plan.Active, func(p engine.Probe) bool { return p.Feed.Log }) {
			b.logPlan.Store(&plan)
		} else {
			b.logPlan.Store(nil)
		}
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
	v := &view{kind: b.plan.Kind, plan: b.plan, slow: slow, fast: fast}
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
	if t, ok := s.byKey[key]; ok && t.Backend == b {
		return t
	}
	if len(s.targets) > 0xFFFF {
		slog.Error("too many targets", "key", key) // ponytail: IDs are uint16 and never reused, also not when a backend comes back with a new spec
		return nil
	}
	t := &Target{ID: uint16(len(s.targets)), Key: key, Backend: b, Model: model}
	if r, ok := s.restored[key]; ok {
		t.restore(r)
		delete(s.restored, key)
	}
	s.targets = append(s.targets, t)
	s.byKey[key] = t
	return t
}

// TargetByKey returns the target a peer names by key, or nil.
func (s *State) TargetByKey(key string) *Target {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byKey[key]
}

// Targets returns every target of the model, up or not.
func (s *State) Targets(model string) []*Target {
	var ts []*Target
	for _, b := range s.Backends() {
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
	for _, b := range s.Backends() {
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

// Info is a backend as the status page shows it.
type Info struct {
	Up         bool        // answered recently and isn't ejected
	Kind       engine.Kind // as resolved, or as configured before that
	Plan       engine.Plan // zero until the first resolve
	Slow, Fast engine.Snapshot
	LogFeed    bool // the log feed is connected
}

func (b *Backend) Info(now time.Time) Info {
	v := b.view.Load()
	if v == nil {
		return Info{Kind: b.Spec.Kind}
	}
	return Info{Up: b.up(v, now), Kind: v.kind, Plan: v.plan, Slow: v.slow, Fast: v.fast, LogFeed: b.logs.Load() != nil}
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
	return !b.ejected.Load() && !b.removed.Load() && now.Sub(v.slow.At) <= b.slowTTL
}

// View is a target's signals as of now. A signal not read within 3 of its
// scrape intervals is unknown. A signal no scraped probe knows comes from the
// log feed while it is connected.
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
	var lm engine.ModelInfo
	var ll engine.Load
	if lg := b.logs.Load(); lg != nil {
		lm, ll = lg.Models[t.Model], load(*lg, t.Model)
	}
	out.Residency, out.VRAMBytes, out.SizeBytes = m.State, m.VRAMBytes, m.SizeBytes
	if out.Residency == engine.Unknown {
		out.Residency = lm.State
	}
	out.Capacity = or(load(v.slow, t.Model).Capacity, ll.Capacity)
	var fl engine.Load
	if now.Sub(v.fast.At) <= b.fastTTL {
		fl = load(v.fast, t.Model)
	}
	out.Running, out.Waiting, out.KVUsage = or(fl.Running, ll.Running), or(fl.Waiting, ll.Waiting), or(fl.KVUsage, ll.KVUsage)
	return out
}

func or[T any](a, b engine.Opt[T]) engine.Opt[T] {
	if a.OK {
		return a
	}
	return b
}

// load is the model's occupancy, each signal falling back to the whole
// backend's (SGLang reports Running per model and Capacity per backend).
// ponytail: a backend-wide value is given to each of its targets; split it
// when a multi-model engine (llama.cpp router mode) reports only backend totals.
func load(s engine.Snapshot, model string) engine.Load {
	l, all := s.Load[model], s.Load[""]
	return engine.Load{Running: or(l.Running, all.Running), Waiting: or(l.Waiting, all.Waiting),
		Capacity: or(l.Capacity, all.Capacity), KVUsage: or(l.KVUsage, all.KVUsage)}
}

// Gen is the target's generation: bumped when its model is unloaded.
func (t *Target) Gen() uint32 { return t.gen.Load() }

// ewma is an exponentially weighted moving average. With no samples it is the
// value restored from a peer or the state file, else unknown.
type ewma struct {
	v float64
	n int
	r engine.Opt[float64]
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

func (e ewma) get() engine.Opt[float64] {
	if e.n == 0 {
		return e.r
	}
	return engine.Opt[float64]{V: e.v, OK: true}
}

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

// TargetStats are one target's speed estimates, for peer snapshots and the
// state file.
type TargetStats struct {
	Key                                string
	PrefillSecTok, LoadSec, ServiceSec engine.Opt[float64]
}

// ExportStats returns the estimates of every target that has one, including
// merged ones of targets this instance hasn't seen.
func (s *State) ExportStats() []TargetStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Collect(maps.Values(s.restored))
	for key, t := range s.byKey {
		p, l, sv := t.Estimates()
		if p.OK || l.OK || sv.OK {
			out = append(out, TargetStats{Key: key, PrefillSecTok: p, LoadSec: l, ServiceSec: sv})
		}
	}
	return out
}

// MergeStats takes estimates from a peer or the state file. They count only
// for a target without samples of its own; a target not seen yet gets them
// when it is.
func (s *State) MergeStats(stats []TargetStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range stats {
		if t, ok := s.byKey[st.Key]; ok {
			t.restore(st)
		} else {
			s.restored[st.Key] = st
		}
	}
}

func (t *Target) restore(st TargetStats) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, f := range []struct {
		e *ewma
		v engine.Opt[float64]
	}{{&t.prefill, st.PrefillSecTok}, {&t.load, st.LoadSec}, {&t.service, st.ServiceSec}} {
		if f.v.OK {
			f.e.r = f.v
		}
	}
}

// follow runs the plan's log probes on the backend's log feed until ctx is
// done, reconnecting with backoff. Its values hold until a newer line replaces
// them and are unknown while the feed is disconnected. Log lines are matched
// in memory and dropped, never logged.
func (s *State) follow(ctx context.Context, b *Backend) {
	backoff := time.Second
	for {
		start := time.Now()
		err := s.followOnce(ctx, b)
		b.logs.Store(nil)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			backoff = time.Second // it was up a while: a fresh failure
		}
		slog.Warn("log feed ended; reconnecting", "backend", b.Spec.URL, "feed", b.Spec.Logs, "in", backoff, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

func (s *State) followOnce(ctx context.Context, b *Backend) error {
	plan := b.logPlan.Load()
	if plan == nil {
		return nil // the plan lost its log probes; check again after the backoff
	}
	rc, err := s.opts.OpenLogs(ctx, b.Spec.Logs)
	if err != nil {
		return err
	}
	defer rc.Close()
	defer context.AfterFunc(ctx, func() { rc.Close() })() // unblocks a pending read
	acc := engine.Snapshot{Models: map[string]engine.ModelInfo{}, Load: map[string]engine.Load{}, From: map[engine.Signal]string{}}
	return plan.Follow(ctx, rc, func(sn engine.Snapshot) {
		next := engine.Snapshot{Models: maps.Clone(acc.Models), Load: maps.Clone(acc.Load), From: maps.Clone(acc.From)}
		for m, info := range sn.Models {
			if info.State == engine.Cold && acc.Models[m].State != engine.Cold {
				for _, t := range b.Targets() {
					if t.Model == m {
						t.gen.Add(1) // unloaded: invalidate its prefix entries
					}
				}
			}
			next.Models[m] = engine.ModelInfo{State: info.State}
		}
		for k, l := range sn.Load {
			o := next.Load[k]
			next.Load[k] = engine.Load{Running: or(l.Running, o.Running), Waiting: or(l.Waiting, o.Waiting),
				Capacity: or(l.Capacity, o.Capacity), KVUsage: or(l.KVUsage, o.KVUsage)}
		}
		maps.Copy(next.From, sn.From)
		acc = next
		b.logs.Store(&next)
	})
}

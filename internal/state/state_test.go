package state

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
)

var t0 = time.Unix(1_790_000_000, 0)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time      { return c.now }
func (c *clock) Add(d time.Duration) { c.now = c.now.Add(d) }

func newState(c *clock, engines ...*fakeengine.Engine) *State {
	var specs []BackendSpec
	for _, e := range engines {
		specs = append(specs, BackendSpec{URL: e.URL(), Kind: engine.Auto})
	}
	return New(specs, Options{Now: c.Now, Fast: time.Second, Slow: 5 * time.Second})
}

// chat sends one streamed chat request and reads the whole reply.
func chat(url, model string) error {
	body := `{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// rounds steps the clock by the fast interval n times, with a round each time.
// Round 0, 5, 10, … is a full round.
func rounds(c *clock, s *State, n int) {
	for range n {
		c.Add(time.Second)
		s.ScrapeAll(context.Background())
	}
}

func only(t *testing.T, s *State, model string) *Target {
	t.Helper()
	ts := s.Targets(model)
	if len(ts) != 1 {
		t.Fatalf("%s: %d targets", model, len(ts))
	}
	return ts[0]
}

func TestOllamaTargetPerModel(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.Ollama, Models: []fakeengine.Model{{Name: "a", SizeBytes: 1 << 30}, {Name: "b", SizeBytes: 2 << 30}}})
	defer e.Close()
	c := &clock{t0}
	s := newState(c, e)
	if s.Ready() {
		t.Fatal("ready before any round")
	}
	s.ScrapeAll(context.Background())
	if !s.Ready() {
		t.Fatal("not ready after the first round")
	}
	a, b := only(t, s, "a"), only(t, s, "b")
	if a.ID == b.ID {
		t.Error("two models, one target")
	}
	v := b.View(c.now)
	if !v.Up || v.Kind != engine.Ollama || v.Residency != engine.Cold || v.SizeBytes != (engine.Opt[int64]{V: 2 << 30, OK: true}) {
		t.Errorf("b: %+v", v)
	}
	// Ollama reports no occupancy, and capacity only in its log.
	if v.Running.OK || v.Waiting.OK || v.Capacity.OK {
		t.Errorf("unreported signals must be unknown, not 0: %+v", v)
	}
	if got := s.Models(c.now); len(got) != 2 || got["a"][0] != engine.Ollama {
		t.Errorf("models %v", got)
	}
}

func TestUnloadBumpsGeneration(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.Ollama, Models: []fakeengine.Model{{Name: "a", SizeBytes: 1 << 30}}})
	defer e.Close()
	c := &clock{t0}
	s := newState(c, e)
	s.ScrapeAll(context.Background())
	tg := only(t, s, "a")
	if err := chat(e.URL(), "a"); err != nil {
		t.Fatal(err)
	}
	rounds(c, s, 5) // up to the next full round
	if v := tg.View(c.now); v.Residency != engine.Loaded {
		t.Fatalf("after a request: %v", v.Residency)
	}
	gen := tg.Gen()
	e.Unload("a")
	rounds(c, s, 4) // fast rounds: Ollama has no fast probes, residency unchanged
	if v := tg.View(c.now); v.Residency != engine.Loaded || tg.Gen() != gen {
		t.Errorf("before the full round: %v, gen %d → %d", v.Residency, gen, tg.Gen())
	}
	rounds(c, s, 1)
	if v := tg.View(c.now); v.Residency != engine.Cold || tg.Gen() != gen+1 {
		t.Errorf("after unload: %v, gen %d → %d", v.Residency, gen, tg.Gen())
	}
	rounds(c, s, 5)
	if tg.Gen() != gen+1 {
		t.Error("still cold must not bump again")
	}
}

func TestLoadSignalsFreshAndStale(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}, Slots: 2})
	defer e.Close()
	c := &clock{t0}
	s := newState(c, e)
	ctx := context.Background()
	s.ScrapeAll(ctx)
	tg := only(t, s, "m")
	if v := tg.View(c.now); v.Running != (engine.Opt[int]{V: 0, OK: true}) || v.Residency != engine.Unknown {
		t.Fatalf("idle: %+v", v)
	}

	release := e.Hold()
	done := make(chan error, 3)
	for range 3 {
		go func() { done <- chat(e.URL(), "m") }()
	}
	defer func() {
		release()
		for range 3 {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}()
	if err := e.WaitFor(func(c fakeengine.Counters) bool { return c.Running == 2 && c.Waiting == 1 }, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	rounds(c, s, 1) // a fast round
	v := tg.View(c.now)
	if v.Running.V != 2 || v.Waiting.V != 1 || !v.KVUsage.OK {
		t.Errorf("fast round: %+v", v)
	}

	c.Add(3*time.Second + time.Millisecond) // no round for more than 3 fast intervals
	if v := tg.View(c.now); v.Running.OK || v.Waiting.OK || v.KVUsage.OK || !v.Up {
		t.Errorf("stale load signals must be unknown, backend still up: %+v", v)
	}
	c.Add(12 * time.Second) // and past 3 slow intervals
	if v := tg.View(c.now); v.Up {
		t.Errorf("stale backend must be down: %+v", v)
	}
}

// decisions records Options.OnDecision calls as "stage=outcome".
type decisions struct {
	mu  sync.Mutex
	got []string
}

func (d *decisions) on(backend, stage, outcome string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.got = append(d.got, stage+"="+outcome)
}

// take returns the decisions since the last take.
func (d *decisions) take() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	got := d.got
	d.got = nil
	return got
}

func (d *decisions) has(want string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Contains(d.got, want)
}

// Every branch a background round takes reaches the hook: why the plan was
// (re)resolved, how each scrape went, and why a generation was bumped.
func TestBackgroundRoundsReportTheirDecisions(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.Ollama, Models: []fakeengine.Model{{Name: "a", SizeBytes: 1 << 30}}})
	c := &clock{t0}
	var d decisions
	s := New([]BackendSpec{{URL: e.URL()}}, Options{Now: c.Now, Fast: time.Second, Slow: 5 * time.Second, OnDecision: d.on})
	step := func(name string, want ...string) {
		t.Helper()
		if got := d.take(); !slices.Equal(got, want) {
			t.Errorf("%s: %v, want %v", name, got, want)
		}
	}
	s.ScrapeAll(context.Background())
	step("first round", "resolve=new_plan")
	if err := chat(e.URL(), "a"); err != nil {
		t.Fatal(err)
	}
	rounds(c, s, 5) // Ollama has no fast probes: only the full round scrapes
	step("model loaded", "scrape=ok")
	e.Unload("a")
	rounds(c, s, 5)
	step("model unloaded", "scrape=ok", "generation=unloaded")
	c.Add(10 * time.Minute)
	rounds(c, s, 1)
	step("plan due again", "resolve=same_plan")
	e.Close()
	rounds(c, s, 4)
	step("engine gone", "scrape=unreachable")
	rounds(c, s, 1)
	step("resolving after a failed scrape", "resolve=failed")
}

func TestUnreachableBackend(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}})
	c := &clock{t0}
	s := newState(c, e)
	ctx := context.Background()
	s.ScrapeAll(ctx)
	tg := only(t, s, "m")

	tg.Backend.Eject()
	if tg.View(c.now).Up {
		t.Error("ejected backend is up")
	}
	c.Add(time.Second)
	s.ScrapeAll(ctx)
	if !tg.View(c.now).Up {
		t.Error("a backend that answers again is back")
	}

	e.Close()
	c.Add(time.Second)
	s.ScrapeAll(ctx) // fails: the last view holds until stale
	if !tg.View(c.now).Up {
		t.Error("one failed round must not take the backend down")
	}
	c.Add(15 * time.Second)
	s.ScrapeAll(ctx)
	if tg.View(c.now).Up {
		t.Error("backend gone for 3 slow intervals is still up")
	}
}

func TestObserve(t *testing.T) {
	cached := func(prompt, cached int) engine.Usage {
		return engine.Usage{PromptTokens: engine.Opt[int]{V: prompt, OK: true}, CachedTokens: engine.Opt[int]{V: cached, OK: true}}
	}
	sec := func(f float64) engine.Opt[float64] { return engine.Opt[float64]{V: f, OK: true} }
	cases := []struct {
		name                   string
		obs                    Observation
		prefill, load, service engine.Opt[float64]
	}{
		{"engine_prefill_time_per_uncached_token",
			Observation{Duration: 2 * time.Second, Usage: func() engine.Usage { u := cached(1200, 200); u.PrefillSec = sec(5); return u }()},
			sec(0.005), engine.Opt[float64]{}, sec(2)},
		{"streamed_ttft_when_engine_reports_no_prefill_time",
			Observation{Streamed: true, TTFT: time.Second, Duration: 3 * time.Second, Usage: cached(1000, 0)},
			sec(0.001), engine.Opt[float64]{}, sec(3)},
		{"nearly_fully_cached_measures_overhead_not_prefill",
			Observation{Streamed: true, TTFT: 100 * time.Millisecond, Duration: time.Second, Usage: cached(3209, 3208)},
			engine.Opt[float64]{}, engine.Opt[float64]{}, sec(1)},
		{"unknown_cached_count_is_no_prefill_sample",
			Observation{Streamed: true, TTFT: time.Second, Duration: time.Second, Usage: engine.Usage{PromptTokens: engine.Opt[int]{V: 3209, OK: true}}},
			engine.Opt[float64]{}, engine.Opt[float64]{}, sec(1)},
		{"cold_dispatch_measures_load_not_service",
			Observation{Cold: true, Streamed: true, TTFT: 8 * time.Second, Duration: 9 * time.Second, Usage: func() engine.Usage { u := cached(10, 0); u.LoadSec = sec(7); return u }()},
			engine.Opt[float64]{}, sec(7), engine.Opt[float64]{}},
		{"cold_dispatch_without_load_time_uses_ttft",
			Observation{Cold: true, Streamed: true, TTFT: 8 * time.Second, Duration: 9 * time.Second},
			engine.Opt[float64]{}, sec(8), engine.Opt[float64]{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var tg Target
			tg.Observe(c.obs)
			p, l, s := tg.Estimates()
			if p != c.prefill || l != c.load || s != c.service {
				t.Errorf("prefill %v load %v service %v; want %v %v %v", p, l, s, c.prefill, c.load, c.service)
			}
		})
	}
	t.Run("moving_average", func(t *testing.T) {
		var tg Target
		tg.Observe(Observation{Duration: 10 * time.Second})
		tg.Observe(Observation{Duration: 20 * time.Second})
		// 10 + 0.2 × (20 − 10) = 12
		if _, _, s := tg.Estimates(); s.V != 12 {
			t.Errorf("service %v", s)
		}
	})
}

// eventually waits for cond without sleeping, failing after 5 s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// feeds hands out one pipe per OpenLogs call, so a test writes log lines and
// ends the stream by closing the writer.
type feeds struct {
	mu     sync.Mutex
	opened []*io.PipeWriter
}

func (f *feeds) open(ctx context.Context, logs string) (io.ReadCloser, error) {
	r, w := io.Pipe()
	f.mu.Lock()
	f.opened = append(f.opened, w)
	f.mu.Unlock()
	return r, nil
}

func (f *feeds) n() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.opened)
}

func (f *feeds) last() *io.PipeWriter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opened[len(f.opened)-1]
}

// The Ollama 0.34.4 startup line (testdata/ollama/v0.34.4/engine.log), cut to
// the fields around OLLAMA_NUM_PARALLEL.
const serverConfig = `time=2026-09-27T08:38:03.846Z level=INFO source=routes.go:2005 msg="server config" env="map[OLLAMA_MULTIUSER_CACHE:false OLLAMA_NEW_ENGINE:false OLLAMA_NOHISTORY:false OLLAMA_NOPRUNE:false OLLAMA_NUM_PARALLEL:2 OLLAMA_ORIGINS:[]]"` + "\n"

func TestLogFeedFillsSignalsWhileConnected(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.Ollama, Models: []fakeengine.Model{{Name: "a", SizeBytes: 1 << 30}}})
	defer e.Close()
	c := &clock{t0}
	var f feeds
	var d decisions
	s := New([]BackendSpec{{URL: e.URL(), Logs: "ollama-1"}}, Options{Now: c.Now, OpenLogs: f.open, OnDecision: d.on})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.ScrapeAll(ctx)
	tg := only(t, s, "a")
	eventually(t, "the feed opened", func() bool { return f.n() == 1 })
	eventually(t, "log_feed=connected", func() bool { return d.has("log_feed=connected") })
	if v := tg.View(c.now); v.Capacity.OK {
		t.Fatalf("capacity before any log line: %v", v.Capacity)
	}

	io.WriteString(f.last(), "time=2026-09-27T08:38:03.845Z level=INFO source=images.go:477 msg=\"total blobs: 0\"\n")
	io.WriteString(f.last(), serverConfig)
	eventually(t, "capacity from the log", func() bool { return tg.View(c.now).Capacity == engine.Opt[int]{V: 2, OK: true} })
	rounds(c, s, 20) // log values don't go stale with the scrape interval
	if v := tg.View(c.now); v.Capacity.V != 2 {
		t.Errorf("after 20 rounds: %v", v.Capacity)
	}

	f.last().Close()
	eventually(t, "capacity unknown after the feed dropped", func() bool { return !tg.View(c.now).Capacity.OK })
	eventually(t, "log_feed=ended", func() bool { return d.has("log_feed=ended") })
	eventually(t, "a reconnect", func() bool { return f.n() == 2 }) // after the 1 s backoff
	io.WriteString(f.last(), serverConfig)                          // the feed replays from the container's start
	eventually(t, "capacity back", func() bool { return tg.View(c.now).Capacity.V == 2 })

	cancel()
	eventually(t, "the feed closed with the backend", func() bool {
		_, err := io.WriteString(f.last(), "x\n")
		return err != nil
	})
}

func TestBackendWithoutFeedDropsLogProbes(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.Ollama, Models: []fakeengine.Model{{Name: "a"}}})
	defer e.Close()
	var f feeds
	s := New([]BackendSpec{{URL: e.URL()}}, Options{OpenLogs: f.open})
	s.ScrapeAll(context.Background())
	if f.n() != 0 || s.Backends()[0].logPlan.Load() != nil {
		t.Error("a backend without a log feed opened one")
	}
}

func TestLogResidencyColdBumpsGeneration(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}})
	defer e.Close()
	// An own probe (synthetic line: own probes are the operator's to verify).
	unloaded, err := engine.LogLine("my-unloaded", engine.Residency, `unloaded model (?P<model>\S+)`, "cold")
	if err != nil {
		t.Fatal(err)
	}
	c := &clock{t0}
	var f feeds
	var d decisions
	s := New([]BackendSpec{{URL: e.URL(), Logs: "vllm-1", Own: []engine.Probe{unloaded}}}, Options{Now: c.Now, OpenLogs: f.open, OnDecision: d.on})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.ScrapeAll(ctx)
	tg := only(t, s, "m")
	gen := tg.Gen()
	eventually(t, "the feed opened", func() bool { return f.n() == 1 })
	io.WriteString(f.last(), "unloaded model m\n")
	eventually(t, "cold from the log", func() bool { return tg.View(c.now).Residency == engine.Cold })
	if tg.Gen() != gen+1 || !d.has("generation=unloaded") {
		t.Errorf("gen %d → %d, want one bump, reported: %v", gen, tg.Gen(), d.take())
	}
	io.WriteString(f.last(), "unloaded model m\n")
	io.WriteString(f.last(), "unloaded model other\n")
	eventually(t, "both lines read", func() bool { return s.Backends()[0].logs.Load().Models["other"].State == engine.Cold })
	if tg.Gen() != gen+1 {
		t.Error("still cold must not bump again")
	}
}

func TestSetBackends(t *testing.T) {
	a := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}})
	defer a.Close()
	b := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}})
	defer b.Close()
	c := &clock{t0}
	s := New([]BackendSpec{{URL: a.URL()}}, Options{Now: c.Now})
	ctx := context.Background()
	s.ScrapeAll(ctx)
	ta := only(t, s, "m")
	ta.Observe(Observation{Duration: time.Second})

	s.SetBackends([]BackendSpec{{URL: a.URL(), Kind: engine.Auto}, {URL: b.URL()}})
	if !s.Ready() {
		t.Error("a discovered backend made the instance unready")
	}
	s.ScrapeAll(ctx)
	ts := s.Targets("m")
	if len(ts) != 2 || ts[0] != ta {
		t.Fatalf("targets %v: the unchanged backend must keep its target", ts)
	}
	if _, _, sv := ta.Estimates(); !sv.OK {
		t.Error("the unchanged backend lost its estimates")
	}

	s.SetBackends([]BackendSpec{{URL: b.URL()}})
	if ts := s.Targets("m"); len(ts) != 1 || ts[0].Backend.Spec.URL != b.URL() {
		t.Errorf("after removing a: %v", ts)
	}
	if ta.View(c.now).Up {
		t.Error("a removed backend's target is up")
	}

	// A URL listed twice (a static backend that is also labeled) is one backend.
	s.SetBackends([]BackendSpec{{URL: b.URL()}, {URL: b.URL(), Capacity: 9}})
	if bs := s.Backends(); len(bs) != 1 || bs[0].Spec.Capacity != 0 {
		t.Errorf("duplicate URL: %d backends", len(bs))
	}

	// Back with the same spec (a container restart): its old state returns.
	s.SetBackends([]BackendSpec{{URL: b.URL()}, {URL: a.URL()}})
	s.ScrapeAll(ctx)
	if s.TargetByKey(ta.Key) != ta || !ta.View(c.now).Up {
		t.Error("a backend back with the same spec lost its target")
	}

	// Same URL, new spec: a new backend, whose target replaces the old one by key.
	s.SetBackends([]BackendSpec{{URL: b.URL()}, {URL: a.URL(), Capacity: 4}})
	s.ScrapeAll(ctx)
	na := s.TargetByKey(ta.Key)
	if na == nil || na == ta || na.Backend.Spec.Capacity != 4 || !na.View(c.now).Up {
		t.Errorf("re-added backend: %+v", na)
	}
}

func TestRunFollowsTheBackendSet(t *testing.T) {
	a := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}})
	defer a.Close()
	s := New(nil, Options{Fast: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	s.SetBackends([]BackendSpec{{URL: a.URL()}})
	eventually(t, "the new backend scraped", func() bool { return len(s.Targets("m")) == 1 })
	tg := s.Targets("m")[0]
	s.SetBackends(nil)
	eventually(t, "the removed backend gone", func() bool { return len(s.Targets("m")) == 0 })
	s.SetBackends([]BackendSpec{{URL: a.URL()}})
	eventually(t, "the backend back, scraped again", func() bool {
		ts := s.Targets("m")
		return len(ts) == 1 && ts[0] == tg && tg.View(time.Now()).Up
	})
	cancel()
	<-done
}

func TestStatsMerge(t *testing.T) {
	e := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}})
	defer e.Close()
	s := New([]BackendSpec{{URL: e.URL()}}, Options{})
	sec := func(f float64) engine.Opt[float64] { return engine.Opt[float64]{V: f, OK: true} }
	key := e.URL() + " m"
	// Merged before the target is seen: it gets them on registration.
	s.MergeStats([]TargetStats{{Key: key, ServiceSec: sec(4), LoadSec: sec(9)}, {Key: "http://elsewhere m", ServiceSec: sec(1)}})
	s.ScrapeAll(context.Background())
	tg := only(t, s, "m")
	if _, l, sv := tg.Estimates(); sv != sec(4) || l != sec(9) {
		t.Errorf("restored: load %v service %v", l, sv)
	}
	// A local sample replaces the restored value.
	tg.Observe(Observation{Duration: 2 * time.Second})
	s.MergeStats([]TargetStats{{Key: key, ServiceSec: sec(100)}})
	if _, _, sv := tg.Estimates(); sv != sec(2) {
		t.Errorf("service %v: a merged value must not override local samples", sv)
	}
	// Export passes on stats of targets this instance never saw.
	got := map[string]TargetStats{}
	for _, st := range s.ExportStats() {
		got[st.Key] = st
	}
	if got[key].ServiceSec != sec(2) || got[key].LoadSec != sec(9) || got["http://elsewhere m"].ServiceSec != sec(1) || len(got) != 2 {
		t.Errorf("export %+v", got)
	}
}

// SGLang reports Running per model and Capacity for the whole backend.
func TestViewFallsBackToBackendWideSignalBySignal(t *testing.T) {
	c := &clock{t0}
	s := New([]BackendSpec{{URL: "http://sglang"}}, Options{Now: c.Now})
	b := s.Backends()[0]
	one := func(v int) engine.Opt[int] { return engine.Opt[int]{V: v, OK: true} }
	snap := engine.Snapshot{At: c.now, Models: map[string]engine.ModelInfo{"m": {}}, Load: map[string]engine.Load{
		"m": {Running: one(1), Waiting: one(2)},
		"":  {Capacity: one(2), Running: one(9)},
	}}
	s.publish(b, snap, snap)
	v := only(t, s, "m").View(c.now)
	if v.Capacity != one(2) || v.Running != one(1) || v.Waiting != one(2) || v.KVUsage.OK {
		t.Errorf("%+v", v)
	}
}

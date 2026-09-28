package state

import (
	"context"
	"io"
	"net/http"
	"strings"
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

package policy

import (
	"strings"
	"testing"
)

func known[T any](v T) Opt[T] { return Opt[T]{v, true} }

// idle is a warm target with 2 free slots and measured estimates: 5 ms per
// prompt token, 10 s to load, 4 s per request.
func idle(id uint16) Candidate {
	return Candidate{
		TargetID: id, Warm: known(true), Capacity: 2, FreeSlots: 2,
		PrefillSecTok: known(0.005), LoadSec: known(10.0), ServiceSec: known(4.0),
	}
}

func with(c Candidate, f func(*Candidate)) Candidate { f(&c); return c }

func cold(c *Candidate) { c.Warm = known(false) }
func full(c *Candidate) { c.FreeSlots = 0 }
func cached(n int) func(*Candidate) {
	return func(c *Candidate) { c.MatchedTokens = n }
}

func TestPick(t *testing.T) {
	cases := []struct {
		name   string
		prompt int
		cands  []Candidate
		want   uint16
		queue  bool
	}{
		// 2000 tokens: warm prefill 10 s; cold adds 10 s of load.
		{"warm_beats_cold", 2000, []Candidate{with(idle(1), cold), idle(2)}, 2, false},
		// Warm but full with 3 ahead on 2 slots: wait (3+1)/2 × 4 s = 8 s, plus
		// 1 s prefill = 9 s. Cold: 10 s load + 1 s prefill = 11 s. Warm, queued.
		{"short_queue_on_warm_beats_cold_load", 200, []Candidate{
			with(with(idle(1), full), func(c *Candidate) { c.QueueAhead = 3 }), with(idle(2), cold),
		}, 1, true},
		// Same, 9 ahead: wait 20 s > 11 s cold. The cold target wins.
		{"long_queue_on_warm_loses_to_cold", 200, []Candidate{
			with(with(idle(1), full), func(c *Candidate) { c.QueueAhead = 9 }), with(idle(2), cold),
		}, 2, false},
		{"longest_prefix_match_wins", 3000, []Candidate{
			with(idle(1), cached(1000)), with(idle(2), cached(2800)), idle(3),
		}, 2, false},
		// The cached target is full with 1 ahead: wait (1+1)/2 × 4 = 4 s + 1 s
		// prefill = 5 s, vs 15 s prefill elsewhere: still worth waiting. With 8
		// ahead, waiting costs 18 s: go elsewhere. Herding corrects itself.
		{"cache_worth_a_short_wait", 3000, []Candidate{
			with(with(with(idle(1), full), cached(2800)), func(c *Candidate) { c.QueueAhead = 1 }), idle(2),
		}, 1, true},
		{"cache_not_worth_a_long_wait", 3000, []Candidate{
			with(with(with(idle(1), full), cached(2800)), func(c *Candidate) { c.QueueAhead = 8 }), idle(2),
		}, 2, false},
		{"cold_that_does_not_fit_is_never_picked", 100, []Candidate{
			with(with(idle(1), cold), func(c *Candidate) { c.FitsIfCold = known(false) }),
			with(with(idle(2), full), func(c *Candidate) { c.QueueAhead = 50 }),
		}, 2, true},
		{"cold_with_unknown_fit_is_allowed", 100, []Candidate{
			with(idle(1), cold), with(with(idle(2), full), func(c *Candidate) { c.QueueAhead = 50 }),
		}, 1, false},
		{"all_full_enqueues_on_best", 100, []Candidate{
			with(with(idle(1), full), func(c *Candidate) { c.QueueAhead = 4 }), with(idle(2), full),
		}, 2, true},
		// KV above 0.9: the 2000-token match counts as 1000.
		// Target 1: (2200-1000) × 5 ms = 6 s. Target 2: (2200-1500) × 5 ms = 3.5 s.
		{"kv_pressure_trusts_the_match_less", 2200, []Candidate{
			with(with(idle(1), cached(2000)), func(c *Candidate) { c.KVUsage = known(0.95) }),
			with(idle(2), cached(1500)),
		}, 2, false},
		{"kv_at_threshold_is_not_pressure", 2200, []Candidate{
			with(with(idle(1), cached(2000)), func(c *Candidate) { c.KVUsage = known(0.9) }),
			with(idle(2), cached(1500)),
		}, 1, false},
		{"equal_cost_goes_to_lower_utilization", 100, []Candidate{
			with(idle(1), func(c *Candidate) { c.FreeSlots = 1 }), idle(2),
		}, 2, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := Pick(RouteReq{PromptTokens: c.prompt, Seed: 1}, c.cands, Defaults)
			if !d.OK || d.TargetID != c.want || d.Enqueue != c.queue {
				t.Errorf("got target %d (ok %v, enqueue %v), want %d (enqueue %v); %s; %+v", d.TargetID, d.OK, d.Enqueue, c.want, c.queue, d.Reason, d.Scores)
			}
		})
	}
}

func TestPickUnknowns(t *testing.T) {
	t.Run("no_candidates", func(t *testing.T) {
		if d := Pick(RouteReq{}, nil, Defaults); d.OK {
			t.Error("picked from nothing")
		}
	})
	t.Run("nothing_fits", func(t *testing.T) {
		c := with(with(idle(1), cold), func(c *Candidate) { c.FitsIfCold = known(false) })
		if d := Pick(RouteReq{}, []Candidate{c}, Defaults); d.OK || !d.Scores[0].Infeasible {
			t.Errorf("got %+v", d)
		}
	})
	t.Run("missing_estimate_uses_fleet_median_not_zero", func(t *testing.T) {
		unmeasured := with(idle(1), func(c *Candidate) { c.PrefillSecTok = Opt[float64]{} })
		d := Pick(RouteReq{PromptTokens: 1000, Seed: 1}, []Candidate{unmeasured, idle(2)}, Defaults)
		// Fleet median 5 ms/token → 5 s, same as target 2.
		if d.Scores[0].Prefill != d.Scores[1].Prefill || d.Scores[0].Prefill == 0 {
			t.Errorf("prefill %v vs %v", d.Scores[0].Prefill, d.Scores[1].Prefill)
		}
	})
	t.Run("no_samples_anywhere_uses_config_default", func(t *testing.T) {
		c := Candidate{TargetID: 1, Warm: known(false), Capacity: 1}
		d := Pick(RouteReq{PromptTokens: 100}, []Candidate{c}, Defaults)
		if s := d.Scores[0]; s.Load != Defaults.LoadSec || s.Prefill != 100*Defaults.PrefillSecTok {
			t.Errorf("got %+v", s)
		}
	})
	t.Run("unknown_residency_pays_no_load", func(t *testing.T) {
		// vLLM and single-model llama.cpp don't report residency: their one model is always loaded.
		c := with(idle(1), func(c *Candidate) { c.Warm = Opt[bool]{} })
		if d := Pick(RouteReq{PromptTokens: 10}, []Candidate{c}, Defaults); d.Scores[0].Load != 0 {
			t.Errorf("got %+v", d.Scores[0])
		}
	})
}

func TestPickSpreadsTies(t *testing.T) {
	cands := []Candidate{idle(1), idle(2), idle(3)}
	picked := map[uint16]int{}
	for seed := range uint64(300) {
		picked[Pick(RouteReq{PromptTokens: 100, Seed: seed}, cands, Defaults).TargetID]++
	}
	for id := uint16(1); id <= 3; id++ {
		if picked[id] < 50 {
			t.Errorf("identical targets should share requests, got %v", picked)
		}
	}
}

func TestLeastLoad(t *testing.T) {
	cfg := Defaults
	cfg.Policy = LeastLoad
	// The cached target is busier; least-load ignores the cache.
	cands := []Candidate{with(with(idle(1), cached(3000)), func(c *Candidate) { c.FreeSlots = 1 }), idle(2)}
	d := Pick(RouteReq{PromptTokens: 3000, Seed: 1}, cands, cfg)
	if d.TargetID != 2 || !strings.Contains(d.Reason, "least-load") {
		t.Errorf("got %d: %s", d.TargetID, d.Reason)
	}
}

func TestReasonNamesTheDecidingFactor(t *testing.T) {
	d := Pick(RouteReq{PromptTokens: 2000, Seed: 1}, []Candidate{with(idle(1), cold), with(idle(2), cached(1500))}, Defaults)
	for _, want := range []string{"warm", "1500 cached tok", "next"} {
		if !strings.Contains(d.Reason, want) {
			t.Errorf("reason %q lacks %q", d.Reason, want)
		}
	}
}

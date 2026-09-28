// Package policy picks a target for a request (ARCHITECTURE §7). Pick is a
// pure function: no I/O, no clock, no shared state.
package policy

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

// Opt is a value plus a known flag, like engine.Opt; policy imports nothing
// from Pharos, and the two convert to each other.
type Opt[T any] struct {
	V  T
	OK bool
}

const (
	Cost      = "cost"       // estimated time to first token (the default)
	LeastLoad = "least-load" // utilization only, for homogeneous vLLM or SGLang fleets
)

type Config struct {
	Policy string // Cost or LeastLoad; "" = Cost
	// Stand-ins for an estimate that neither the target nor any other
	// candidate has a sample of yet. See Defaults for where they come from.
	PrefillSecTok, LoadSec, ServiceSec float64
}

// Defaults are placeholders until feedback measures each target.
//   - PrefillSecTok: 3.1–15 ms per token was measured for a cold 3,209-token
//     prompt on CPU with Qwen2.5-0.5B (llama.cpp b6602–v0.5.0, llama-swap;
//     timings.prompt_ms in the recorded streams, 2026-09-27). 5 ms is in that range.
//   - LoadSec: not measured on a real model. The 0.5B test model loads in
//     0.1 s (Ollama load_duration, v0.12.4 capture); a 7–8B model is expected to take seconds [U].
//     10 s keeps a cold target from looking cheap before one is measured.
//   - ServiceSec: not measured for real chat turns [U]; the captures' 10-token
//     replies take 0.2–1 s.
var Defaults = Config{Policy: Cost, PrefillSecTok: 0.005, LoadSec: 10, ServiceSec: 5}

// RouteReq is what the policy knows about the request.
type RouteReq struct {
	PromptTokens int    // estimated
	Seed         uint64 // breaks exact ties; random per request so equal targets share load
}

// Candidate is one target that serves the requested model.
type Candidate struct {
	TargetID      uint16
	Warm          Opt[bool] // model loaded; unknown on engines that don't report residency (always loaded)
	Capacity      int       // slots: engine-reported, configured or the scheduler's fallback
	FreeSlots     int       // capacity minus occupied; 0 when the engine reports waiting requests
	QueueAhead    int       // router waiters for the model plus engine-reported waiting
	KVUsage       Opt[float64]
	MatchedTokens int // prefix the target recently served, from the prefix index
	PrefillSecTok Opt[float64]
	LoadSec       Opt[float64]
	ServiceSec    Opt[float64]
	FitsIfCold    Opt[bool] // memory headroom for a cold load
}

// Score is one candidate's evaluation, for Reason, debugging and tests.
type Score struct {
	TargetID            uint16
	Cost                float64 // seconds to first token (cost) or utilization (least-load)
	Wait, Load, Prefill float64
	Util                float64
	Infeasible          bool // cold and doesn't fit in memory
}

type Decision struct {
	OK       bool // false: no candidate, or none fits
	TargetID uint16
	Enqueue  bool // the best target has no free slot: wait in the fair queue
	Reason   string
	Scores   []Score
}

// kvPressure is where the engine's KV cache is likely to evict: a match on a
// target above it counts half. ponytail: a step, make it gradual if simulation shows cliffs.
const kvPressure = 0.9

// tie is how close two costs must be to count as equal, in the cost's unit.
const tie = 1e-3

// Pick scores every candidate and returns the cheapest feasible one. Ties go to
// lower utilization, then to a per-request random order.
func Pick(r RouteReq, cands []Candidate, cfg Config) Decision {
	if len(cands) == 0 {
		return Decision{Reason: "no candidate"}
	}
	prefillRate := fleet(cands, func(c Candidate) Opt[float64] { return c.PrefillSecTok }, cfg.PrefillSecTok)
	loadSec := fleet(cands, func(c Candidate) Opt[float64] { return c.LoadSec }, cfg.LoadSec)
	serviceSec := fleet(cands, func(c Candidate) Opt[float64] { return c.ServiceSec }, cfg.ServiceSec)

	scores := make([]Score, len(cands))
	best := -1
	for i, c := range cands {
		capacity := max(c.Capacity, 1)
		s := Score{TargetID: c.TargetID, Util: float64(capacity-max(c.FreeSlots, 0)+c.QueueAhead) / float64(capacity)}
		if c.FreeSlots <= 0 {
			s.Wait = float64(c.QueueAhead+1) / float64(capacity) * or(c.ServiceSec, serviceSec)
		}
		if c.Warm.OK && !c.Warm.V {
			s.Load = or(c.LoadSec, loadSec)
			s.Infeasible = c.FitsIfCold.OK && !c.FitsIfCold.V
		}
		matched := float64(c.MatchedTokens)
		if c.KVUsage.OK && c.KVUsage.V > kvPressure {
			matched /= 2
		}
		s.Prefill = math.Max(float64(r.PromptTokens)-matched, 0) * or(c.PrefillSecTok, prefillRate)
		s.Cost = s.Wait + s.Load + s.Prefill
		if cfg.Policy == LeastLoad {
			s.Cost = s.Util
		}
		scores[i] = s
		if !s.Infeasible && (best < 0 || better(s, scores[best], r.Seed)) {
			best = i
		}
	}
	if best < 0 {
		return Decision{Reason: "no target fits: every candidate is cold without memory to load", Scores: scores}
	}
	c, s := cands[best], scores[best]
	return Decision{OK: true, TargetID: c.TargetID, Enqueue: c.FreeSlots <= 0, Scores: scores, Reason: reason(cfg, c, s, scores)}
}

func better(a, b Score, seed uint64) bool {
	if d := a.Cost - b.Cost; math.Abs(d) > tie {
		return d < 0
	}
	if a.Util != b.Util {
		return a.Util < b.Util
	}
	return mix(seed^uint64(a.TargetID)) < mix(seed^uint64(b.TargetID))
}

// mix is splitmix64's finalizer: an even spread for a seed and target pair.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ x>>31
}

// fleet is the median of the candidates' known values, else def. It stands in
// for a candidate that has no sample yet; a missing estimate is never 0.
func fleet(cands []Candidate, get func(Candidate) Opt[float64], def float64) float64 {
	var known []float64
	for _, c := range cands {
		if v := get(c); v.OK {
			known = append(known, v.V)
		}
	}
	if len(known) == 0 {
		return def
	}
	slices.Sort(known)
	return known[len(known)/2]
}

func or(o Opt[float64], def float64) float64 {
	if o.OK {
		return o.V
	}
	return def
}

func reason(cfg Config, c Candidate, s Score, all []Score) string {
	var b strings.Builder
	switch {
	case !c.Warm.OK:
		b.WriteString("always loaded")
	case c.Warm.V:
		b.WriteString("warm")
	default:
		b.WriteString("cold")
	}
	fmt.Fprintf(&b, ", %d cached tok", c.MatchedTokens)
	if cfg.Policy == LeastLoad {
		fmt.Fprintf(&b, ", least-load util %.2f", s.Util)
	} else {
		fmt.Fprintf(&b, ", est %.2fs (wait %.2f, load %.2f, prefill %.2f)", s.Cost, s.Wait, s.Load, s.Prefill)
	}
	next := math.Inf(1)
	for _, o := range all {
		if o.TargetID != s.TargetID && !o.Infeasible {
			next = math.Min(next, o.Cost)
		}
	}
	if !math.IsInf(next, 1) {
		fmt.Fprintf(&b, " vs %.2f next", next)
	}
	if c.FreeSlots <= 0 {
		b.WriteString(", queued: no free slot")
	}
	return b.String()
}

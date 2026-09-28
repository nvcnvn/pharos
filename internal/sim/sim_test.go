// Package sim is the layer-5 routing simulator (ARCHITECTURE §14): synthetic
// multi-user traces against the layer-3 fake engines, comparing policies on
// prefix-cache hit rate, model loads and p95 time to first token.
//
//	PHAROS_SIM=1 go test -v -run TestSimulate ./internal/sim
//	SIM_USERS=48 SIM_SEED=2   load and trace (default 24 users, seed 1)
//
// Everything runs in real time, sped up: one real second is Speed simulated
// seconds, for the engines' latency model and for Pharos's scrape intervals.
// The fleet, the latency model and the traces are synthetic; the numbers
// compare policies with each other, not with any real deployment.
package sim

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/proxy"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
)

const (
	speed = 200.0 // simulated seconds per real second
	hosts = 3
	gb    = int64(1) << 30
)

// The fleet: three hosts that each hold about two of the three models, like
// 24 GB consumer GPUs running Ollama with 2 parallel slots.
var models = []fakeengine.Model{{Name: "chat-8b", SizeBytes: 5 * gb}, {Name: "code-7b", SizeBytes: 9 * gb / 2}, {Name: "small-3b", SizeBytes: 2 * gb}}

var fleet = fakeengine.Config{
	Kind: engine.Ollama, Models: models, Slots: 2, MemoryBytes: 10 * gb,
	PrefillSecTok: 0.0005, DecodeSecTok: 0.03, LoadSec: 8, CacheTokens: 32_000, Speed: speed,
}

// conversation is one user's session: a model, a system prompt (shared by
// every user of the same agent) and a number of turns with think time between.
type conversation struct {
	model  string
	system string
	turns  []string // user messages
	think  []float64
	out    int // tokens per reply
}

// workload builds the users' conversations. Model popularity 60/30/10; each
// model's agent has a ~3000-token system prompt.
func workload(seed uint64, users int) []conversation {
	r := rand.New(rand.NewPCG(seed, seed))
	words := func(n int) string {
		var b strings.Builder
		for range n {
			fmt.Fprintf(&b, "w%d ", r.IntN(5000))
		}
		return b.String()
	}
	systems := map[string]string{}
	for _, m := range models {
		systems[m.Name] = words(2000)
	}
	var cs []conversation
	for range users {
		m := models[0].Name
		switch p := r.Float64(); {
		case p > 0.9:
			m = models[2].Name
		case p > 0.6:
			m = models[1].Name
		}
		c := conversation{model: m, system: systems[m], out: 50 + r.IntN(150)}
		for range 4 + r.IntN(6) {
			c.turns = append(c.turns, words(40+r.IntN(200)))
			c.think = append(c.think, 2+r.Float64()*10)
		}
		cs = append(cs, c)
	}
	return cs
}

type result struct {
	requests, failed           int
	promptTokens, cachedTokens int
	loads                      int
	p50, p95                   float64 // TTFT, simulated seconds
}

func (r result) hitRate() float64 { return float64(r.cachedTokens) / float64(r.promptTokens) }

// run plays the workload against a fresh fleet. mode "round-robin" sends
// straight to the engines in turn; any other mode is a Pharos policy.
func run(t *testing.T, mode string, convs []conversation) result {
	var engines []*fakeengine.Engine
	for range hosts {
		e := fakeengine.New(fleet)
		defer e.Close()
		engines = append(engines, e)
	}
	var next atomic.Int64
	base := func() string { return engines[next.Add(1)%hosts].URL() }
	if mode != "round-robin" {
		var specs []state.BackendSpec
		for _, e := range engines {
			specs = append(specs, state.BackendSpec{URL: e.URL(), Kind: engine.Ollama, MemoryBytes: fleet.MemoryBytes, Capacity: fleet.Slots})
		}
		var sc *sched.Sched
		scaled := func(d time.Duration) time.Duration { return time.Duration(float64(d) / speed) }
		st := state.New(specs, state.Options{Fast: scaled(time.Second), Slow: scaled(5 * time.Second), OnUpdate: func() { sc.Kick() }})
		pc := policy.Config{Policy: mode, PrefillSecTok: policy.Defaults.PrefillSecTok / speed, LoadSec: policy.Defaults.LoadSec / speed, ServiceSec: policy.Defaults.ServiceSec / speed}
		sc = sched.New(st, prefix.New(200_000), sched.Config{Policy: pc})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go st.Run(ctx)
		for !st.Ready() {
			time.Sleep(time.Millisecond)
		}
		srv := httptest.NewServer(proxy.New(st, sc, proxy.Options{}))
		defer srv.Close()
		base = func() string { return srv.URL }
	}

	var mu sync.Mutex
	var ttfts []float64
	failed := 0
	var wg sync.WaitGroup
	for _, c := range convs {
		wg.Go(func() {
			msgs := []map[string]string{{"role": "system", "content": c.system}}
			for i, turn := range c.turns {
				time.Sleep(time.Duration(c.think[i] / speed * float64(time.Second)))
				msgs = append(msgs, map[string]string{"role": "user", "content": turn})
				ttft, reply, err := chat(base(), c.model, msgs, c.out)
				mu.Lock()
				if err != nil {
					failed++
				} else {
					ttfts = append(ttfts, ttft*speed)
				}
				mu.Unlock()
				msgs = append(msgs, map[string]string{"role": "assistant", "content": reply})
			}
		})
	}
	wg.Wait()

	res := result{requests: len(ttfts) + failed, failed: failed}
	for _, e := range engines {
		c := e.Counters()
		res.promptTokens += c.PromptTokens
		res.cachedTokens += c.CachedTokens
		res.loads += c.Loads
	}
	slices.Sort(ttfts)
	if len(ttfts) > 0 {
		res.p50, res.p95 = ttfts[len(ttfts)/2], ttfts[len(ttfts)*95/100]
	}
	return res
}

// chat sends one streamed turn and returns the time to the first token (real
// seconds) and the reply text.
func chat(base, model string, msgs []map[string]string, out int) (float64, string, error) {
	body, _ := json.Marshal(map[string]any{"model": model, "stream": true, "max_tokens": out, "messages": msgs})
	start := time.Now()
	resp, err := http.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(string(body)))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var ttft float64
	var reply strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		line, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok || json.Unmarshal([]byte(line), &chunk) != nil || len(chunk.Choices) == 0 {
			continue
		}
		if ttft == 0 {
			ttft = time.Since(start).Seconds()
		}
		reply.WriteString(chunk.Choices[0].Delta.Content)
	}
	return ttft, reply.String(), sc.Err()
}

func TestSimulate(t *testing.T) {
	if os.Getenv("PHAROS_SIM") == "" {
		t.Skip("layer 5, sped-up real time: PHAROS_SIM=1 go test -v -run TestSimulate ./internal/sim")
	}
	var seed uint64 = 1
	users := 24
	fmt.Sscan(os.Getenv("SIM_SEED"), &seed)
	fmt.Sscan(os.Getenv("SIM_USERS"), &users)
	convs := workload(seed, users)
	modes := []string{"round-robin", policy.LeastLoad, policy.Cost}
	res := map[string]result{}
	t.Logf("%-12s %9s %7s %9s %6s %9s %9s", "policy", "requests", "failed", "cache hit", "loads", "p50 TTFT", "p95 TTFT")
	for _, m := range modes {
		r := run(t, m, convs)
		res[m] = r
		t.Logf("%-12s %9d %7d %8.0f%% %6d %8.2fs %8.2fs", m, r.requests, r.failed, 100*r.hitRate(), r.loads, r.p50, r.p95)
	}
	// Asserted: what held on every run of 12, 24 and 48 users × seeds 1–3
	// (2026-09-27). TTFT is reported, not asserted: at light load p95 is the
	// first cold loads under every policy, and cost's p50 is often worse at 24
	// users because it queues on a warm host rather than load a second copy
	// (ARCHITECTURE §16, question 5).
	rr, cost := res["round-robin"], res[policy.Cost]
	if cost.failed > 0 {
		t.Errorf("cost: %d failed requests", cost.failed)
	}
	if cost.hitRate() <= rr.hitRate() || cost.loads >= rr.loads {
		t.Errorf("cost should beat round-robin on prefix-cache hits and model loads")
	}
}

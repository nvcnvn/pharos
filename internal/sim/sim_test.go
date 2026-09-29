// Package sim is the layer-5 routing simulator (ARCHITECTURE §14): synthetic
// multi-user traces against the layer-3 fake engines, comparing policies on
// prefix-cache hit rate, model loads and p95 time to first token.
//
//	PHAROS_SIM=1 go test -v -run TestSimulate ./internal/sim
//	SIM_USERS=48 SIM_SEED=2   load and trace (default 24 users, seed 1)
//	SIM_CLASSIFIERS=8         add agents making near-prefill-only calls (default 0; §16 question 10)
//	SIM_CONVERSATIONS=file    real user turns from this JSONL instead of generated words; write it
//	                          with `uv run --with duckdb examples/mac-native/replay.py --fetch`
//
// Half of the users send JSON-escaped bodies (\uXXXX for non-ASCII), as Open
// WebUI and Python's requests do, and half UTF-8, as current OpenAI SDKs do.
// Generated words are ASCII, so that matters only with real conversations.
//
// Everything runs in real time, sped up: one real second is Speed simulated
// seconds, for the engines' latency model and for Pharos's scrape intervals.
// The fleet and the latency model are synthetic (ARCHITECTURE §14, assumptions
// ledger), and so are the traces unless SIM_CONVERSATIONS gives real ones; the
// numbers compare policies with each other, not with any real deployment.
package sim

import (
	"bufio"
	"bytes"
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
	"unicode/utf16"

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
// A classifier's turns are independent calls (no history) that generate one
// token, like an agent choosing from a constrained set.
type conversation struct {
	model    string
	system   string
	turns    []string // user messages
	think    []float64
	out      int // tokens per reply
	classify bool
	escaped  bool // the client sends non-ASCII as \uXXXX
}

// recorded is one real conversation from SIM_CONVERSATIONS, in the format
// examples/mac-native/replay.py caches WildChat-1M in.
type recorded struct {
	Lang  string `json:"lang"`
	Turns []struct {
		User string `json:"user"`
	} `json:"turns"`
}

// conversations reads SIM_CONVERSATIONS, keeping those whose first 10 user
// turns fit in the fleet's 32k-token cache at 4 bytes per token.
func conversations(t *testing.T) []recorded {
	path := os.Getenv("SIM_CONVERSATIONS")
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []recorded
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 64<<20)
	for sc.Scan() {
		var c recorded
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, turn := range c.Turns[:min(len(c.Turns), 10)] {
			n += len(turn.User)
		}
		if len(c.Turns) > 0 && n <= 20_000 {
			out = append(out, c)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// workload builds the users' conversations. Model popularity 60/30/10; each
// model's agent has a ~3000-token system prompt. With real conversations, user
// u replays real[u] (wrapping) for up to its first 10 turns; otherwise its
// turns are generated words. Classifiers, drawn after the
// users so the users' trace doesn't change, share one ~1000-token prompt per
// model and call every 1–4 s.
func workload(seed uint64, users, classifiers int, real []recorded) []conversation {
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
	for u := range users {
		m := models[0].Name
		switch p := r.Float64(); {
		case p > 0.9:
			m = models[2].Name
		case p > 0.6:
			m = models[1].Name
		}
		c := conversation{model: m, system: systems[m], out: 50 + r.IntN(150), escaped: u%2 == 1}
		if len(real) > 0 {
			for _, turn := range real[u%len(real)].Turns[:min(len(real[u%len(real)].Turns), 10)] {
				c.turns = append(c.turns, turn.User)
				c.think = append(c.think, 2+r.Float64()*10)
			}
		} else {
			for range 4 + r.IntN(6) {
				c.turns = append(c.turns, words(40+r.IntN(200)))
				c.think = append(c.think, 2+r.Float64()*10)
			}
		}
		cs = append(cs, c)
	}
	for i := range classifiers {
		m := models[i%2].Name // the two busiest models
		c := conversation{model: m, system: "Classify. " + systems[m][:len(systems[m])/3], out: 1, classify: true}
		for range 10 + r.IntN(10) {
			c.turns = append(c.turns, words(20+r.IntN(60)))
			c.think = append(c.think, 1+r.Float64()*3)
		}
		cs = append(cs, c)
	}
	return cs
}

type result struct {
	requests, failed           int
	promptTokens, cachedTokens int
	loads                      int
	p50, p95                   float64 // TTFT of chat turns, simulated seconds
	classifyP50, classifyP95   float64 // TTFT of classifier calls
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
	var ttfts, classify []float64
	failed := 0
	var wg sync.WaitGroup
	for _, c := range convs {
		wg.Go(func() {
			msgs := []map[string]string{{"role": "system", "content": c.system}}
			for i, turn := range c.turns {
				time.Sleep(time.Duration(c.think[i] / speed * float64(time.Second)))
				if c.classify {
					msgs = msgs[:1]
				}
				msgs = append(msgs, map[string]string{"role": "user", "content": turn})
				ttft, reply, err := chat(base(), c.model, msgs, c.out, c.escaped)
				mu.Lock()
				switch {
				case err != nil:
					failed++
				case c.classify:
					classify = append(classify, ttft*speed)
				default:
					ttfts = append(ttfts, ttft*speed)
				}
				mu.Unlock()
				msgs = append(msgs, map[string]string{"role": "assistant", "content": reply})
			}
		})
	}
	wg.Wait()

	res := result{requests: len(ttfts) + len(classify) + failed, failed: failed}
	for _, e := range engines {
		c := e.Counters()
		res.promptTokens += c.PromptTokens
		res.cachedTokens += c.CachedTokens
		res.loads += c.Loads
	}
	res.p50, res.p95 = percentiles(ttfts)
	res.classifyP50, res.classifyP95 = percentiles(classify)
	return res
}

func percentiles(xs []float64) (p50, p95 float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	slices.Sort(xs)
	return xs[len(xs)/2], xs[len(xs)*95/100]
}

// escapeNonASCII writes every non-ASCII character as \uXXXX (UTF-16), as
// Python's json.dumps does by default.
func escapeNonASCII(b []byte) []byte {
	var out bytes.Buffer
	for _, r := range string(b) {
		if r < 0x80 {
			out.WriteRune(r)
			continue
		}
		for _, u := range utf16.Encode([]rune{r}) {
			fmt.Fprintf(&out, `\u%04x`, u)
		}
	}
	return out.Bytes()
}

// chat sends one streamed turn and returns the time to the first token (real
// seconds) and the reply text.
func chat(base, model string, msgs []map[string]string, out int, escaped bool) (float64, string, error) {
	body, _ := json.Marshal(map[string]any{"model": model, "stream": true, "max_tokens": out, "messages": msgs})
	if escaped {
		body = escapeNonASCII(body)
	}
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
	users, classifiers := 24, 0
	fmt.Sscan(os.Getenv("SIM_SEED"), &seed)
	fmt.Sscan(os.Getenv("SIM_USERS"), &users)
	fmt.Sscan(os.Getenv("SIM_CLASSIFIERS"), &classifiers)
	convs := workload(seed, users, classifiers, conversations(t))
	modes := []string{"round-robin", policy.LeastLoad, policy.Cost}
	res := map[string]result{}
	t.Logf("%-12s %9s %7s %9s %6s %9s %9s %13s %13s", "policy", "requests", "failed", "cache hit", "loads", "p50 TTFT", "p95 TTFT", "classify p50", "classify p95")
	for _, m := range modes {
		r := run(t, m, convs)
		res[m] = r
		t.Logf("%-12s %9d %7d %8.0f%% %6d %8.2fs %8.2fs %12.2fs %12.2fs", m, r.requests, r.failed, 100*r.hitRate(), r.loads, r.p50, r.p95, r.classifyP50, r.classifyP95)
	}
	// Asserted: what held on every run of 12, 24 and 48 users × seeds 1–3,
	// with 0 and 8 classifiers (2026-09-28): cost is never worse than
	// round-robin on prefix-cache hits or model loads. It is strictly better on
	// most runs, but 48 users with seed 3 ties on loads (9 and 9) and nearly on
	// hits (78–79% and 76–78%), also on the code of 2026-09-27. TTFT is
	// reported, not asserted: at light load p95 is the first cold loads under
	// every policy, and cost's p50 is often worse at 24 users because it queues
	// on a warm host rather than load a second copy (ARCHITECTURE §16,
	// questions 5 and 10).
	rr, cost := res["round-robin"], res[policy.Cost]
	if cost.failed > 0 {
		t.Errorf("cost: %d failed requests", cost.failed)
	}
	if cost.hitRate() < rr.hitRate() || cost.loads > rr.loads {
		t.Errorf("cost should be no worse than round-robin on prefix-cache hits and model loads")
	}
}

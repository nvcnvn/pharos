package sched

import (
	"context"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/state"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

type env struct {
	c  *clock
	st *state.State
	px *prefix.Index
	s  *Sched
}

func setup(t *testing.T, specs ...state.BackendSpec) *env {
	t.Helper()
	c := &clock{time.Unix(1_790_000_000, 0)}
	st := state.New(specs, state.Options{Now: c.Now})
	px := prefix.New(1000)
	s := New(st, px, Config{Policy: policy.Defaults})
	s.seed = func() uint64 { return 1 }
	st.ScrapeAll(context.Background())
	return &env{c, st, px, s}
}

// round steps the clock one fast interval and scrapes.
func (e *env) round() {
	e.c.now = e.c.now.Add(time.Second)
	e.st.ScrapeAll(context.Background())
	e.s.Kick()
}

func vllm(t *testing.T, slots int) *fakeengine.Engine {
	e := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}, Slots: slots})
	t.Cleanup(e.Close)
	return e
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

type grant struct {
	name string
	l    *Lease
	err  error
}

// acquire starts Acquire in the background and waits until it is queued or granted.
func (e *env) acquire(t *testing.T, ctx context.Context, key, name string, r Request, out chan<- grant) {
	t.Helper()
	before := e.s.Waiting()
	started := make(chan struct{})
	go func() {
		close(started)
		l, err := e.s.Acquire(ctx, key, r)
		out <- grant{name, l, err}
	}()
	<-started
	eventually(t, name+" queued or granted", func() bool { return e.s.Waiting() > before || len(out) > 0 })
}

func TestRaceForTheLastSlot(t *testing.T) {
	e := setup(t, state.BackendSpec{URL: vllm(t, 4).URL(), Capacity: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grants := make(chan grant, 10)
	for range 10 {
		go func() {
			l, err := e.s.Acquire(ctx, "k", Request{Model: "m"})
			grants <- grant{"", l, err}
		}()
	}
	eventually(t, "9 waiting", func() bool { return e.s.Waiting() == 9 })
	if len(grants) != 1 {
		t.Fatalf("%d granted on 1 slot", len(grants))
	}
	(<-grants).l.Release(Feedback{OK: true})
	if g := <-grants; g.l == nil {
		t.Fatalf("released slot not handed on: %v", g.err)
	}
	if e.s.Waiting() != 8 {
		t.Errorf("waiting %d, want 8", e.s.Waiting())
	}
	cancel()
	eventually(t, "cancelled waiters gone", func() bool { return e.s.Waiting() == 0 })
}

func TestFairQueueRoundRobinAcrossKeys(t *testing.T) {
	e := setup(t, state.BackendSpec{URL: vllm(t, 4).URL(), Capacity: 1})
	ctx := context.Background()
	hold, err := e.s.Acquire(ctx, "x", Request{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	grants := make(chan grant, 4)
	// Key a queues three requests before key b queues one.
	for _, name := range []string{"a1", "a2", "a3", "b1"} {
		e.acquire(t, ctx, name[:1], name, Request{Model: "m"}, grants)
	}
	hold.Release(Feedback{OK: true})
	var order []string
	for range 4 {
		g := <-grants
		order = append(order, g.name)
		g.l.Release(Feedback{OK: true})
	}
	if got := strings.Join(order, " "); got != "a1 b1 a2 a3" {
		t.Errorf("grant order %s, want a1 b1 a2 a3", got)
	}
}

func TestLeaseAccounting(t *testing.T) {
	e := setup(t, state.BackendSpec{URL: vllm(t, 4).URL(), Capacity: 1})
	ctx := context.Background()

	t.Run("cancelled_waiter_leaves_the_queue", func(t *testing.T) {
		hold, _ := e.s.Acquire(ctx, "k", Request{Model: "m"})
		cctx, cancel := context.WithCancel(ctx)
		grants := make(chan grant, 1)
		e.acquire(t, cctx, "k", "w", Request{Model: "m"}, grants)
		cancel()
		if g := <-grants; !errors.Is(g.err, context.Canceled) || e.s.Waiting() != 0 {
			t.Errorf("got %v, waiting %d", g.err, e.s.Waiting())
		}
		hold.Release(Feedback{})
	})
	t.Run("double_release_frees_one_slot", func(t *testing.T) {
		l, _ := e.s.Acquire(ctx, "k", Request{Model: "m"})
		l.Release(Feedback{OK: true})
		l.Release(Feedback{OK: true})
		first, _ := e.s.Acquire(ctx, "k", Request{Model: "m"})
		cctx, cancel := context.WithCancel(ctx)
		grants := make(chan grant, 1)
		e.acquire(t, cctx, "k", "second", Request{Model: "m"}, grants)
		if len(grants) != 0 {
			t.Error("two leases on one slot: inflight went negative")
		}
		cancel()
		<-grants
		first.Release(Feedback{})
	})
	t.Run("queue_is_bounded", func(t *testing.T) {
		s := New(e.st, e.px, Config{Policy: policy.Defaults, MaxQueue: 1})
		hold, _ := s.Acquire(ctx, "k", Request{Model: "m"})
		defer hold.Release(Feedback{})
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go s.Acquire(cctx, "k", Request{Model: "m"})
		eventually(t, "one waiting", func() bool { return s.Waiting() == 1 })
		if _, err := s.Acquire(ctx, "k", Request{Model: "m"}); !errors.Is(err, ErrQueueFull) {
			t.Errorf("got %v", err)
		}
	})
}

func TestNoTarget(t *testing.T) {
	e := setup(t, state.BackendSpec{URL: vllm(t, 2).URL()})
	ctx := context.Background()
	if _, err := e.s.Acquire(ctx, "k", Request{Model: "nope"}); !errors.Is(err, ErrUnknownModel) {
		t.Errorf("unknown model: %v", err)
	}
	if _, err := e.s.Acquire(ctx, "k", Request{Model: "m", Kind: engine.Ollama}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("native Ollama API on a vLLM backend: %v", err)
	}
	if e.s.Waiting() != 0 {
		t.Error("failed requests left waiters")
	}
}

// chat sends a request straight to the engine, as a client that bypasses Pharos.
func chat(url, model string) error {
	resp, err := http.Post(url+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"`+model+`","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

func TestOccupancyCountsClientsThatBypassPharos(t *testing.T) {
	cases := []struct {
		name            string
		engineSlots     int // the engine's own
		capacity        int // what Pharos is told
		running, queued int // what the bypassing clients cause
	}{
		// Pharos has nothing in flight, but the engine runs 2 of 2.
		{"engine_running_fills_capacity", 4, 2, 2, 0},
		// Capacity says 4, but the engine queues: it is full, whatever the config says.
		{"engine_waiting_means_full", 1, 4, 1, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := vllm(t, c.engineSlots)
			e := setup(t, state.BackendSpec{URL: fake.URL(), Capacity: c.capacity})
			release := fake.Hold()
			n := c.running + c.queued
			done := make(chan error, n)
			for range n {
				go func() { done <- chat(fake.URL(), "m") }()
			}
			if err := fake.WaitFor(func(f fakeengine.Counters) bool { return f.Running == c.running && f.Waiting == c.queued }, 5*time.Second); err != nil {
				t.Fatal(err)
			}
			e.round()
			grants := make(chan grant, 1)
			e.acquire(t, context.Background(), "k", "w", Request{Model: "m"}, grants)
			if len(grants) != 0 {
				t.Fatalf("granted a slot while the engine reports %d running, %d waiting", c.running, c.queued)
			}
			release()
			for range n {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			e.round() // the scrape sees the engine idle; the kick serves the queue
			g := <-grants
			if g.l == nil {
				t.Fatal(g.err)
			}
			g.l.Release(Feedback{OK: true})
		})
	}
}

func TestUnknownCapacityFallsBackToTwo(t *testing.T) {
	e := setup(t, state.BackendSpec{URL: vllm(t, 8).URL()}) // vLLM doesn't report capacity; none configured
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grants := make(chan grant, 3)
	for _, n := range []string{"1", "2", "3"} {
		e.acquire(t, ctx, "k", n, Request{Model: "m"}, grants)
	}
	if len(grants) != 2 || e.s.Waiting() != 1 {
		t.Errorf("granted %d, waiting %d; want 2 and 1", len(grants), e.s.Waiting())
	}
}

func TestNeverEvictABusyModel(t *testing.T) {
	fake := fakeengine.New(fakeengine.Config{Kind: engine.Ollama, Models: []fakeengine.Model{{Name: "a", SizeBytes: 1 << 30}, {Name: "b", SizeBytes: 1 << 30}}})
	defer fake.Close()
	// Room for one model at a time.
	e := setup(t, state.BackendSpec{URL: fake.URL(), MemoryBytes: 3 << 29})
	if err := chat(fake.URL(), "a"); err != nil { // load a
		t.Fatal(err)
	}
	for range 5 {
		e.round() // up to the full round that reads /api/ps
	}
	ctx := context.Background()
	la, err := e.s.Acquire(ctx, "k", Request{Model: "a"})
	if err != nil || la.Cold {
		t.Fatalf("a: %v, cold %v", err, la != nil && la.Cold)
	}
	grants := make(chan grant, 1)
	e.acquire(t, ctx, "k", "b", Request{Model: "b"}, grants)
	if len(grants) != 0 {
		t.Fatal("b was dispatched cold while loading it would evict a, which has a request in flight")
	}
	la.Release(Feedback{OK: true})
	g := <-grants
	if g.l == nil || !g.l.Cold {
		t.Fatalf("b after a finished: %+v", g)
	}
	g.l.Release(Feedback{})
}

func TestPrefixAffinityAndCorrection(t *testing.T) {
	e := setup(t, state.BackendSpec{URL: vllm(t, 2).URL()}, state.BackendSpec{URL: vllm(t, 2).URL()})
	ctx := context.Background()
	system := []byte("system\x00" + strings.Repeat("a long shared system prompt. ", 400)) // ~3000 tokens
	turn1 := prefix.Chain("m", nil, [][]byte{system, []byte("user\x00q1")})
	turn2 := prefix.Chain("m", nil, [][]byte{system, []byte("user\x00q1"), []byte("assistant\x00a1"), []byte("user\x00q2")})
	req := func(c []prefix.Link) Request {
		return Request{Model: "m", Chain: c, PromptTokens: c[len(c)-1].Bytes / bytesPerToken}
	}

	l1, err := e.s.Acquire(ctx, "k", req(turn1))
	if err != nil {
		t.Fatal(err)
	}
	l1.Release(Feedback{OK: true})
	for seed := range uint64(20) { // whatever the tie-break says
		e.s.seed = func() uint64 { return seed }
		l2, err := e.s.Acquire(ctx, "k", req(turn2))
		if err != nil {
			t.Fatal(err)
		}
		if l2.Target != l1.Target {
			t.Fatalf("turn 2 went to %s, turn 1 to %s: %s", l2.Target.Key, l1.Target.Key, l2.Reason)
		}
		l2.Release(Feedback{OK: true})
	}

	// The engine reports it had nothing cached: forget the target for this prefix.
	l3, _ := e.s.Acquire(ctx, "k", req(turn2))
	l3.Release(Feedback{OK: true, Usage: engine.Usage{PromptTokens: engine.Opt[int]{V: 3000, OK: true}, CachedTokens: engine.Opt[int]{V: 0, OK: true}}})
	if got := e.px.Lookup(turn2, func(uint16) uint32 { return l1.Target.Gen() }); got[l1.Target.ID] != 0 {
		t.Errorf("wrong prediction kept: %v", got)
	}
}

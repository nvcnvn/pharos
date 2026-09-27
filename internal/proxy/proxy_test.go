package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
)

// Layer 3: proxy + sched + policy + prefix + state against fake engines.

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

type env struct {
	c   *clock
	st  *state.State
	srv *httptest.Server
}

// start wires Pharos to the backends. The state doesn't scrape until round.
func start(t *testing.T, specs ...state.BackendSpec) *env {
	t.Helper()
	c := &clock{time.Unix(1_790_000_000, 0)}
	var sc *sched.Sched
	st := state.New(specs, state.Options{Now: c.Now, OnUpdate: func() { sc.Kick() }})
	sc = sched.New(st, prefix.New(1000), sched.Config{Policy: policy.Defaults})
	srv := httptest.NewServer(New(st, sc, nil))
	t.Cleanup(srv.Close)
	return &env{c, st, srv}
}

// rounds steps the clock by the fast interval n times, with a scrape each time.
// Round 0, 5, 10, … is a full round.
func (e *env) rounds(n int) {
	for range n {
		e.st.ScrapeAll(context.Background())
		e.c.now = e.c.now.Add(time.Second)
	}
}

// fake starts a fake engine. Prefill costs 10 µs per uncached token (synthetic),
// so a cached prefix saves measurable time: at zero cost the cost model rightly
// sees no reason to prefer the engine that holds it.
func fake(t *testing.T, kind engine.Kind, models ...string) *fakeengine.Engine {
	var ms []fakeengine.Model
	for _, m := range models {
		ms = append(ms, fakeengine.Model{Name: m, SizeBytes: 1 << 30})
	}
	f := fakeengine.New(fakeengine.Config{Kind: kind, Models: ms, CacheTokens: 100_000, PrefillSecTok: 1e-5, Speed: 1})
	t.Cleanup(f.Close)
	return f
}

func chatBody(model string, msgs ...string) string {
	var m []map[string]string
	for i, content := range msgs {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		m = append(m, map[string]string{"role": role, "content": content})
	}
	b, _ := json.Marshal(map[string]any{"model": model, "stream": true, "stream_options": map[string]any{"include_usage": true}, "messages": m})
	return string(b)
}

func post(ctx context.Context, t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// chat sends a request through Pharos, reads the whole reply and returns its usage.
func (e *env) chat(t *testing.T, path, body string) (int, engine.Usage) {
	t.Helper()
	resp := post(context.Background(), t, e.srv.URL+path, body)
	defer resp.Body.Close()
	var tp tap
	b, _ := io.ReadAll(resp.Body)
	tp.write(b)
	tp.end()
	return resp.StatusCode, tp.usage
}

var long = strings.Repeat("A long system prompt that every turn of this conversation repeats. ", 60)

func TestStreamsTokenByToken(t *testing.T) {
	f := fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: f.URL()})
	e.rounds(1)
	release := f.Hold()
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := post(ctx, t, e.srv.URL+"/v1/chat/completions", chatBody("m", "hi"))
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || !strings.Contains(line, `"content"`) {
		t.Fatalf("first token didn't reach the client while the engine was mid-reply: %q, %v", line, err)
	}
}

func TestPrefixAffinityAcrossTurns(t *testing.T) {
	a, b := fake(t, engine.VLLM, "m"), fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: a.URL()}, state.BackendSpec{URL: b.URL()})
	e.rounds(1)
	code, u1 := e.chat(t, "/v1/chat/completions", chatBody("m", long+"q1"))
	if code != http.StatusOK {
		t.Fatal(code)
	}
	code, u2 := e.chat(t, "/v1/chat/completions", chatBody("m", long+"q1", "a1", "q2"))
	if code != http.StatusOK {
		t.Fatal(code)
	}
	ra, rb := a.Counters().Requests, b.Counters().Requests
	if ra+rb != 2 || ra != 2 && rb != 2 {
		t.Errorf("turns split across engines: %d and %d", ra, rb)
	}
	if u1.CachedTokens.V != 0 || u2.CachedTokens.V < 500 {
		t.Errorf("cached tokens: turn 1 %v, turn 2 %v", u1.CachedTokens, u2.CachedTokens)
	}
	// Feedback reached the target's estimates.
	for _, tg := range e.st.Targets("m") {
		if _, _, service := tg.Estimates(); service.OK != (tg.Backend.Spec.URL == a.URL() && ra == 2 || tg.Backend.Spec.URL == b.URL() && rb == 2) {
			t.Errorf("%s: service estimate %v", tg.Key, service)
		}
	}
}

func TestWarmTargetPreferredOverCold(t *testing.T) {
	warm, cold := fake(t, engine.Ollama, "m"), fake(t, engine.Ollama, "m")
	e := start(t, state.BackendSpec{URL: warm.URL()}, state.BackendSpec{URL: cold.URL()})
	if code, _ := (&env{srv: &httptest.Server{URL: warm.URL()}}).chat(t, "/v1/chat/completions", chatBody("m", "load it")); code != http.StatusOK {
		t.Fatal(code)
	}
	e.rounds(1)
	for i := range 5 {
		if code, _ := e.chat(t, "/v1/chat/completions", chatBody("m", "question", "answer", string(rune('a'+i)))); code != http.StatusOK {
			t.Fatal(code)
		}
	}
	if w, c := warm.Counters(), cold.Counters(); w.Requests != 6 || c.Loads != 0 {
		t.Errorf("warm served %d of 6, cold loaded %d times", w.Requests, c.Loads)
	}
}

func TestNativeOllamaAPIOnlyToOllama(t *testing.T) {
	o, v := fake(t, engine.Ollama, "m"), fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: v.URL()}, state.BackendSpec{URL: o.URL()})
	e.rounds(1)
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}]}` // native: streams by default
	for range 4 {
		code, u := e.chat(t, "/api/chat", body)
		if code != http.StatusOK || !u.PromptTokens.OK {
			t.Fatalf("status %d, usage %+v", code, u)
		}
	}
	if o.Counters().Requests != 4 || v.Counters().Requests != 0 {
		t.Errorf("ollama %d, vllm %d", o.Counters().Requests, v.Counters().Requests)
	}
}

func TestRetryAndEjectOnConnectionRefused(t *testing.T) {
	a, b := fake(t, engine.VLLM, "m"), fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: a.URL()}, state.BackendSpec{URL: b.URL()})
	e.rounds(1)
	body := chatBody("m", long+"q")
	if code, _ := e.chat(t, "/v1/chat/completions", body); code != http.StatusOK {
		t.Fatal(code)
	}
	dead, live := a, b // the prefix now points at dead
	if b.Counters().Requests == 1 {
		dead, live = b, a
	}
	dead.Close() // state still thinks it's up
	for range 3 {
		if code, _ := e.chat(t, "/v1/chat/completions", body); code != http.StatusOK {
			t.Fatalf("status %d", code)
		}
	}
	if live.Counters().Requests != 3 {
		t.Errorf("live served %d of 3", live.Counters().Requests)
	}
	for _, tg := range e.st.Targets("m") {
		if tg.Backend.Spec.URL == dead.URL() && tg.View(e.c.now).Up {
			t.Error("refused backend not ejected")
		}
	}
}

func TestEngineDeathMidStream(t *testing.T) {
	a, b := fake(t, engine.VLLM, "m"), fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: a.URL()}, state.BackendSpec{URL: b.URL()})
	e.rounds(1)
	body := chatBody("m", long+"q")
	if code, _ := e.chat(t, "/v1/chat/completions", body); code != http.StatusOK {
		t.Fatal(code)
	}
	victim, other := a, b
	if b.Counters().Requests == 1 {
		victim, other = b, a
	}
	victim.Hold()
	resp := post(context.Background(), t, e.srv.URL+"/v1/chat/completions", body) // prefix affinity: the victim again
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	if _, err := r.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	victim.Close()
	rest, _ := io.ReadAll(r)
	if strings.Contains(string(rest), "[DONE]") {
		t.Error("stream completed although the engine died")
	}
	if code, _ := e.chat(t, "/v1/chat/completions", body); code != http.StatusOK || other.Counters().Requests != 1 {
		t.Errorf("next request: status %d, other engine served %d", code, other.Counters().Requests)
	}
}

func TestClientCancelReleasesLease(t *testing.T) {
	f := fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: f.URL(), Capacity: 1})
	e.rounds(1)
	release := f.Hold()
	ctx, cancel := context.WithCancel(context.Background())
	resp := post(ctx, t, e.srv.URL+"/v1/chat/completions", chatBody("m", "hi"))
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	cancel()
	resp.Body.Close()
	if err := f.WaitFor(func(c fakeengine.Counters) bool { return c.Running == 0 }, 5*time.Second); err != nil {
		t.Fatalf("engine still generating for a client that left: %v", err)
	}
	release()
	// One slot: this only gets through if the cancelled request's lease was released.
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp = post(ctx, t, e.srv.URL+"/v1/chat/completions", chatBody("m", "again"))
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d", resp.StatusCode)
	}
}

func TestModelListsAndHealth(t *testing.T) {
	o, v := fake(t, engine.Ollama, "a", "b"), fake(t, engine.VLLM, "c")
	e := start(t, state.BackendSpec{URL: o.URL()}, state.BackendSpec{URL: v.URL()})
	get := func(path string) (int, string) {
		resp, err := http.Get(e.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := get("/healthz"); code != http.StatusServiceUnavailable {
		t.Errorf("healthz before the first scrape: %d", code)
	}
	e.rounds(1)
	if code, _ := get("/healthz"); code != http.StatusOK {
		t.Errorf("healthz after: %d", code)
	}
	var models struct{ Data []struct{ ID string } }
	_, body := get("/v1/models")
	json.Unmarshal([]byte(body), &models)
	if len(models.Data) != 3 || models.Data[0].ID != "a" || models.Data[2].ID != "c" {
		t.Errorf("/v1/models: %s", body)
	}
	var tags struct{ Models []struct{ Name string } }
	_, body = get("/api/tags")
	json.Unmarshal([]byte(body), &tags)
	if len(tags.Models) != 2 {
		t.Errorf("/api/tags: %s", body)
	}
	if code, _ := e.chat(t, "/v1/chat/completions", chatBody("nope", "hi")); code != http.StatusNotFound {
		t.Errorf("unknown model: %d", code)
	}
	if code, _ := e.chat(t, "/v1/chat/completions", `{"messages":[]}`); code != http.StatusBadRequest {
		t.Errorf("no model: %d", code)
	}
}

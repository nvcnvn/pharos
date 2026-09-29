package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/config"
	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
	"github.com/nvcnvn/pharos/internal/usage"
)

// Layer 3: proxy + sched + policy + prefix + state against fake engines.

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

type env struct {
	c   *clock
	st  *state.State
	sc  *sched.Sched
	p   *Proxy
	u   *usage.Counter
	srv *httptest.Server
	// done receives each finished request, as obs would. Nothing reads it
	// unless a test does; a full channel drops.
	done chan Done
}

// start wires Pharos to the backends. The state doesn't scrape until round.
func start(t testing.TB, specs ...state.BackendSpec) *env {
	t.Helper()
	c := &clock{time.Unix(1_790_000_000, 0)}
	var sc *sched.Sched
	st := state.New(specs, state.Options{Now: c.Now, OnUpdate: func() { sc.Kick() }})
	sc = sched.New(st, prefix.New(1000), sched.Config{Policy: policy.Defaults})
	u := usage.New(usage.Config{Now: c.Now})
	done := make(chan Done, 100)
	p := New(st, sc, Options{Usage: u, OnDone: func(d Done) {
		select {
		case done <- d:
		default:
		}
	}})
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return &env{c, st, sc, p, u, srv, done}
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
func fake(t testing.TB, kind engine.Kind, models ...string) *fakeengine.Engine {
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

func post(ctx context.Context, t testing.TB, url, body string) *http.Response {
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
	tp.close()
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

func TestWithUsage(t *testing.T) {
	cases := []struct {
		name, in string
		rewrote  bool
		opts     string // stream_options after the rewrite
	}{
		{"absent_is_added", `{"model":"m","stream":true}`, true, `{"include_usage":true}`},
		{"null_is_replaced", `{"model":"m","stream_options":null}`, true, `{"include_usage":true}`},
		{"false_is_turned_on_and_other_options_kept", `{"model":"m","stream_options":{"include_usage":false,"continuous_usage_stats":true}}`, true, `{"continuous_usage_stats":true,"include_usage":true}`},
		{"already_asked_is_untouched", `{"model":"m","stream_options":{"include_usage":true}}`, false, ""},
		{"not_an_object_is_left_to_the_engine", `{"model":"m","stream_options":"x"}`, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, rewrote := withUsage([]byte(c.in))
			if rewrote != c.rewrote {
				t.Fatalf("rewrote %v, want %v", rewrote, c.rewrote)
			}
			if !rewrote {
				if string(out) != c.in {
					t.Errorf("body changed: %s", out)
				}
				return
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(out, &got); err != nil || string(got["model"]) != `"m"` || string(got["stream_options"]) != c.opts {
				t.Errorf("got %s, %v", out, err)
			}
		})
	}
	t.Run("content_bytes_are_not_html_escaped", func(t *testing.T) {
		out, _ := withUsage([]byte(`{"model":"m","messages":[{"role":"user","content":"<b>&</b>"}]}`))
		if !strings.Contains(string(out), `"<b>&</b>"`) {
			t.Errorf("got %s", out)
		}
	})
}

// A client that streams without include_usage gets the stream it asked for,
// with no usage chunk, while Pharos still learns from the usage: the warm
// request's prefill speed becomes known.
func TestUsageAskedForAndStripped(t *testing.T) {
	f := fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: f.URL()})
	e.rounds(1)
	for i := range 2 {
		body, _ := json.Marshal(map[string]any{"model": "m", "stream": true,
			"messages": []map[string]string{{"role": "user", "content": strings.Repeat(fmt.Sprint(i), 4000)}}})
		resp := post(context.Background(), t, e.srv.URL+"/v1/chat/completions", string(body))
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || strings.Contains(string(b), "prompt_tokens") || !strings.HasSuffix(string(b), "data: [DONE]\n\n") {
			t.Fatalf("request %d: %d\n%s", i, resp.StatusCode, b)
		}
	}
	if p, _, _ := e.st.Targets("m")[0].Estimates(); !p.OK {
		t.Error("prefill speed unknown: the tap read no usage")
	}
}

// send posts body with an optional bearer key and returns the status, the
// error code of an error body and the Retry-After header.
func (e *env) send(t *testing.T, path, key, body string) (status int, code, retryAfter string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var eb struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	b, _ := io.ReadAll(resp.Body)
	json.Unmarshal(b, &eb)
	return resp.StatusCode, eb.Error.Code, resp.Header.Get("Retry-After")
}

func TestKeys(t *testing.T) {
	f := fake(t, engine.VLLM, "m", "other")
	e := start(t, state.BackendSpec{URL: f.URL()})
	e.rounds(1)
	e.p.SetKeys([]config.Key{
		{Name: "alice", SHA256: config.HashKey("alice-key"), Weight: 1, Models: []string{"m"}, RPM: 2},
		{Name: "bob", SHA256: config.HashKey("bob-key"), Weight: 1, TokensPerDay: 1},
		{Name: "ops", SHA256: config.HashKey("ops-key"), Weight: 1, Admin: true},
	})
	body := chatBody("m", "hi")

	t.Run("a_missing_or_unknown_key_is_401", func(t *testing.T) {
		for _, key := range []string{"", "nope"} {
			if status, code, _ := e.send(t, "/v1/chat/completions", key, body); status != 401 || code != "invalid_api_key" {
				t.Errorf("key %q: %d %s", key, status, code)
			}
		}
	})
	t.Run("usage_is_recorded_under_the_key_name", func(t *testing.T) {
		if status, _, _ := e.send(t, "/v1/chat/completions", "alice-key", body); status != 200 {
			t.Fatalf("status %d", status)
		}
		rows := e.u.History(e.c.now, e.c.now, usage.ByKey)
		if len(rows) != 1 || rows[0].Name != "alice" || rows[0].Requests != 1 || rows[0].PromptTok == 0 || rows[0].CompletionTok == 0 || rows[0].Unmetered != 0 {
			t.Errorf("rows %+v", rows)
		}
	})
	t.Run("a_model_outside_the_allow_list_is_404_and_not_listed", func(t *testing.T) {
		if status, code, _ := e.send(t, "/v1/chat/completions", "alice-key", chatBody("other", "hi")); status != 404 || code != "model_not_found" {
			t.Errorf("%d %s", status, code)
		}
		req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/v1/models", nil)
		req.Header.Set("Authorization", "Bearer alice-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(b), `"m"`) || strings.Contains(string(b), `"other"`) {
			t.Errorf("models: %s", b)
		}
	})
	t.Run("over_the_rpm_is_429_rate_limit_exceeded_with_retry_after", func(t *testing.T) {
		e.send(t, "/v1/chat/completions", "alice-key", body) // 2nd this minute
		status, code, retry := e.send(t, "/v1/chat/completions", "alice-key", body)
		if status != 429 || code != "rate_limit_exceeded" || retry == "" || retry == "0" {
			t.Errorf("%d %s retry-after %q", status, code, retry)
		}
	})
	t.Run("out_of_tokens_is_429_insufficient_quota_until_midnight", func(t *testing.T) {
		if status, _, _ := e.send(t, "/v1/chat/completions", "bob-key", body); status != 200 {
			t.Fatalf("first request: %d", status)
		}
		status, code, retry := e.send(t, "/v1/chat/completions", "bob-key", body)
		if want := fmt.Sprint(int(e.c.now.Truncate(24 * time.Hour).Add(24 * time.Hour).Sub(e.c.now).Seconds())); status != 429 || code != "insufficient_quota" || retry != want {
			t.Errorf("%d %s retry-after %s, want %s", status, code, retry, want)
		}
	})
	t.Run("only_admin_keys_pass_the_admin_gate", func(t *testing.T) {
		h := e.p.Admin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		for key, want := range map[string]int{"ops-key": 200, "alice-key": 403, "": 401} {
			req := httptest.NewRequest(http.MethodGet, "/status", nil)
			if key != "" {
				req.Header.Set("Authorization", "Bearer "+key)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != want {
				t.Errorf("key %q: %d, want %d", key, rec.Code, want)
			}
		}
	})
	t.Run("a_reload_replaces_the_keys", func(t *testing.T) {
		e.p.SetKeys([]config.Key{{Name: "carol", SHA256: config.HashKey("carol-key"), Weight: 1}})
		if status, _, _ := e.send(t, "/v1/chat/completions", "ops-key", body); status != 401 {
			t.Errorf("removed key: %d", status)
		}
		if status, _, _ := e.send(t, "/v1/chat/completions", "carol-key", body); status != 200 {
			t.Errorf("new key: %d", status)
		}
	})
}

func TestDrainFailsHealthzButServes(t *testing.T) {
	f := fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: f.URL()})
	e.rounds(1)
	health := func() int {
		resp, err := http.Get(e.srv.URL + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if h := health(); h != 200 {
		t.Fatalf("healthz %d before drain", h)
	}
	e.p.Drain()
	if h := health(); h != 503 {
		t.Errorf("healthz %d while draining, want 503", h)
	}
	if status, _, _ := e.send(t, "/v1/chat/completions", "", chatBody("m", "hi")); status != 200 {
		t.Errorf("request while draining: %d", status)
	}
}

// next returns the next finished request's decisions as "stage/outcome".
func (e *env) next(t *testing.T) (Done, []string) {
	t.Helper()
	select {
	case d := <-e.done:
		var out []string
		for _, x := range d.Decisions {
			out = append(out, x.Stage+"/"+x.Outcome)
		}
		return d, out
	case <-time.After(5 * time.Second):
		t.Fatal("no request finished")
		return Done{}, nil
	}
}

// Every branch a request takes is recorded on its Done, for metrics and logs.
func TestDecisionsAreRecorded(t *testing.T) {
	a, b := fake(t, engine.VLLM, "m"), fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: a.URL()}, state.BackendSpec{URL: b.URL()})
	e.rounds(1)
	e.p.SetKeys([]config.Key{
		{Name: "k", SHA256: config.HashKey("sk"), Weight: 1, Models: []string{"m"}, RPM: 3},
		{Name: "k2", SHA256: config.HashKey("sk2"), Weight: 1},
	})
	noUsage := `{"model":"m","stream":true,"messages":[{"role":"user","content":"` + long + `"}]}`
	check := func(t *testing.T, want ...string) Done {
		t.Helper()
		d, got := e.next(t)
		if strings.Join(got, " ") != strings.Join(want, " ") {
			t.Errorf("decisions %v, want %v", got, want)
		}
		return d
	}

	t.Run("a_served_stream", func(t *testing.T) {
		e.send(t, "/v1/chat/completions", "sk", noUsage)
		d := check(t, "admission/ok", "usage/pharos_asked", "route/least_loaded", "queue/immediate", "prefix/miss", "upstream/ok")
		if d.Overhead <= 0 || d.Overhead > d.Duration {
			t.Errorf("overhead %v of %v", d.Overhead, d.Duration)
		}
	})
	t.Run("the_next_turn_follows_its_prefix", func(t *testing.T) {
		e.send(t, "/v1/chat/completions", "sk", noUsage)
		check(t, "admission/ok", "usage/pharos_asked", "route/least_loaded", "queue/immediate", "prefix/predicted_hit", "upstream/ok")
	})
	t.Run("admission_refusals", func(t *testing.T) {
		e.send(t, "/v1/chat/completions", "wrong", noUsage)
		check(t, "admission/unauthorized")
		e.send(t, "/v1/chat/completions", "sk", "{")
		check(t, "admission/bad_request")
		e.send(t, "/v1/chat/completions", "sk", chatBody("other", "hi"))
		check(t, "admission/model_not_allowed")
		e.send(t, "/v1/chat/completions", "sk", chatBody("m", "hi")) // 3rd this minute
		check(t, "admission/ok", "usage/client_asked", "route/least_loaded", "queue/immediate", "prefix/miss", "upstream/ok")
		e.send(t, "/v1/chat/completions", "sk", chatBody("m", "hi"))
		check(t, "admission/rate_limited")
	})
	t.Run("a_refused_connection_is_retried_elsewhere", func(t *testing.T) {
		dead := a
		if b.Counters().Requests > a.Counters().Requests { // the prefix points at b
			dead = b
		}
		dead.Close()
		e.send(t, "/v1/chat/completions", "sk2", noUsage)
		check(t, "admission/ok", "usage/pharos_asked", "route/least_loaded", "queue/immediate", "upstream/connect_failed",
			"route/only_choice", "queue/immediate", "prefix/miss", "upstream/ok")
	})
}

// STRATEGY §1: one user can hog a host, because the engine's own queue is
// first in, first out. Behind Pharos, keys waiting for a saturated target take
// turns for each freed slot, as many turns per pass as their weight in config.
func TestKeysTakeTurnsForASaturatedTarget(t *testing.T) {
	f := fake(t, engine.VLLM, "m")
	e := start(t, state.BackendSpec{URL: f.URL(), Capacity: 1})
	e.rounds(1)
	e.p.SetKeys([]config.Key{
		{Name: "hog", SHA256: config.HashKey("hog-key"), Weight: 1},
		{Name: "alice", SHA256: config.HashKey("alice-key"), Weight: 1},
		{Name: "team", SHA256: config.HashKey("team-key"), Weight: 2},
	})
	// served sends first, which takes the one slot, then queues the others in
	// order, and returns the keys of the queued requests in the order they got
	// the slot. Each request stops after its first token until the test lets
	// the next one in, so the order doesn't depend on timing.
	served := func(t *testing.T, first string, queued ...string) []string {
		t.Helper()
		started := make(chan string, len(queued)+1)
		send := func(key string) {
			go func() {
				req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/chat/completions", strings.NewReader(chatBody("m", "hi")))
				req.Header.Set("Authorization", "Bearer "+key+"-key")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					started <- "error: " + err.Error()
					return
				}
				defer resp.Body.Close()
				r := bufio.NewReader(resp.Body)
				if _, err := r.ReadString('\n'); err != nil || resp.StatusCode != http.StatusOK {
					started <- fmt.Sprintf("status %d: %v", resp.StatusCode, err)
					return
				}
				started <- key
				io.Copy(io.Discard, r)
			}()
		}
		next := func() string {
			select {
			case k := <-started:
				return k
			case <-time.After(5 * time.Second):
				t.Fatal("no request got the slot")
				return ""
			}
		}
		release := f.Hold()
		send(first)
		if k := next(); k != first {
			t.Fatalf("first request: %s", k)
		}
		for i, key := range queued {
			send(key)
			for deadline := time.Now().Add(5 * time.Second); e.sc.Waiting() < i+1; runtime.Gosched() {
				if time.Now().After(deadline) {
					t.Fatalf("%s's request never queued", key)
				}
			}
		}
		var order []string
		for range queued {
			hold := f.Hold()
			release()
			order = append(order, next())
			release = hold
		}
		release()
		return order
	}
	count := func(keys []string, key string) (n int) {
		for _, k := range keys {
			if k == key {
				n++
			}
		}
		return n
	}

	t.Run("a_key_behind_a_hog_waits_for_at_most_one_of_its_requests", func(t *testing.T) {
		// First in, first out, alice would wait for all three.
		order := served(t, "hog", "hog", "hog", "hog", "alice")
		if count(order[:2], "alice") != 1 {
			t.Errorf("served %v", order)
		}
	})
	t.Run("a_key_with_weight_2_gets_two_turns_per_pass", func(t *testing.T) {
		order := served(t, "hog", "hog", "hog", "hog", "team", "team", "team")
		if count(order[:3], "team") != 2 {
			t.Errorf("served %v; want team to have 2 of the first 3 slots", order)
		}
	})
}

// Clients send more than streamed chat: internal apps call without streaming,
// RAG apps embed, and Ollama clients use its native generate and embed. Each is
// forwarded as the engine recorded it (streamed or not, the reply untouched)
// and its tokens are counted for quotas: none of these is unmetered.
func TestEveryRecordedReplyShapeIsServedAndMetered(t *testing.T) {
	cases := []struct {
		name, path, body string
		streamed         bool // the reply comes as several lines
		completion       bool // it generates tokens; embeddings count 0
	}{
		{"non_streamed_chat", "/v1/chat/completions", `{"model":"m","stream":false,"messages":[{"role":"user","content":"hi"}]}`, false, true},
		{"completion", "/v1/completions", `{"model":"m","prompt":"hi"}`, false, true},
		{"embeddings", "/v1/embeddings", `{"model":"m","input":"hi"}`, false, false},
		{"native_chat_not_streamed", "/api/chat", `{"model":"m","stream":false,"messages":[{"role":"user","content":"hi"}]}`, false, true},
		{"native_generate_streams_by_default", "/api/generate", `{"model":"m","prompt":"hi"}`, true, true},
		{"native_embed", "/api/embed", `{"model":"m","input":"hi"}`, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := fake(t, engine.Ollama, "m")
			e := start(t, state.BackendSpec{URL: f.URL()})
			e.rounds(1)
			resp := post(context.Background(), t, e.srv.URL+c.path, c.body)
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d: %s", resp.StatusCode, b)
			}
			if lines := strings.Count(strings.TrimSpace(string(b)), "\n") + 1; c.streamed != (lines > 1) {
				t.Errorf("streamed %v, got %d lines", c.streamed, lines)
			}
			if !c.streamed && !json.Valid(b) {
				t.Errorf("reply isn't the engine's JSON body: %s", b)
			}
			rows := e.u.History(e.c.now, e.c.now, usage.ByKey)
			if len(rows) != 1 || rows[0].Requests != 1 || rows[0].PromptTok == 0 || rows[0].Unmetered != 0 || c.completion != (rows[0].CompletionTok > 0) {
				t.Errorf("usage %+v", rows)
			}
		})
	}
	t.Run("an_engine_that_refuses_embeddings_answers_the_client", func(t *testing.T) {
		f := fake(t, engine.VLLM, "m") // a generative model: vLLM has no embeddings route (capture: 404)
		e := start(t, state.BackendSpec{URL: f.URL()})
		e.rounds(1)
		if code, _ := e.chat(t, "/v1/embeddings", `{"model":"m","input":"hi"}`); code != http.StatusNotFound {
			t.Errorf("status %d, want the engine's 404", code)
		}
	})
}

// STRATEGY §1: a small team's fleet mixes engines. One Pharos fronts Ollama,
// llama.cpp and vLLM at once: it lists every model, reads each engine's own
// usage fields, keeps Ollama's native API on Ollama, and fills each engine up
// to its own capacity, however that is known (llama.cpp reports its slots,
// vLLM's comes from config).
func TestMixedEngineFleet(t *testing.T) {
	o := fake(t, engine.Ollama, "llama3", "gemma")
	l := fakeengine.New(fakeengine.Config{Kind: engine.LlamaCpp, Models: []fakeengine.Model{{Name: "shared"}}, Slots: 3})
	v := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "shared"}}, Slots: 4})
	t.Cleanup(l.Close)
	t.Cleanup(v.Close)
	e := start(t, state.BackendSpec{URL: o.URL()}, state.BackendSpec{URL: l.URL()}, state.BackendSpec{URL: v.URL(), Capacity: 1})
	e.rounds(1)
	get := func(path string) string {
		resp, err := http.Get(e.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	names := func(body, list, field string) string {
		var m map[string][]map[string]any
		json.Unmarshal([]byte(body), &m)
		var out []string
		for _, x := range m[list] {
			out = append(out, fmt.Sprint(x[field]))
		}
		return strings.Join(out, " ")
	}

	t.Run("every_engines_models_are_listed", func(t *testing.T) {
		if got := names(get("/v1/models"), "data", "id"); got != "gemma llama3 shared" {
			t.Errorf("/v1/models: %s", got)
		}
		if got := names(get("/api/tags"), "models", "name"); got != "llama3 gemma" {
			t.Errorf("/api/tags lists Ollama's models only: %s", got)
		}
	})
	t.Run("each_engines_usage_is_metered", func(t *testing.T) {
		for _, m := range []string{"llama3", "shared", "shared"} {
			if code, _ := e.chat(t, "/v1/chat/completions", chatBody(m, "hi "+m)); code != http.StatusOK {
				t.Fatalf("%s: %d", m, code)
			}
		}
		for _, row := range e.u.History(e.c.now, e.c.now, usage.ByModel) {
			if row.PromptTok == 0 || row.CompletionTok == 0 || row.Unmetered != 0 {
				t.Errorf("%+v", row)
			}
		}
	})
	t.Run("the_native_api_stays_on_ollama", func(t *testing.T) {
		if code, _ := e.chat(t, "/api/chat", `{"model":"gemma","messages":[{"role":"user","content":"hi"}]}`); code != http.StatusOK {
			t.Errorf("gemma: %d", code)
		}
		before := l.Counters().Requests + v.Counters().Requests
		if code, _ := e.chat(t, "/api/chat", `{"model":"shared","messages":[{"role":"user","content":"hi"}]}`); code != http.StatusServiceUnavailable {
			t.Errorf("a model no Ollama serves: %d, want 503", code)
		}
		if n := l.Counters().Requests + v.Counters().Requests - before; n != 0 {
			t.Errorf("native request reached %d non-Ollama engines", n)
		}
	})
	t.Run("each_engine_fills_to_its_own_capacity", func(t *testing.T) {
		releaseL, releaseV := sync.OnceFunc(l.Hold()), sync.OnceFunc(v.Hold())
		defer releaseL()
		defer releaseV()
		done := make(chan int, 5)
		for i := range 5 {
			go func() {
				code, _ := e.chat(t, "/v1/chat/completions", chatBody("shared", fmt.Sprintf("request %d", i)))
				done <- code
			}()
		}
		if err := l.WaitFor(func(c fakeengine.Counters) bool { return c.Running == 3 }, 5*time.Second); err != nil {
			t.Fatalf("llama.cpp, 3 slots reported: %v", err)
		}
		if err := v.WaitFor(func(c fakeengine.Counters) bool { return c.Running == 1 }, 5*time.Second); err != nil {
			t.Fatalf("vLLM, capacity 1 in config: %v", err)
		}
		for deadline := time.Now().Add(5 * time.Second); e.sc.Waiting() != 1; runtime.Gosched() {
			if time.Now().After(deadline) {
				t.Fatalf("waiting in Pharos: %d, want the 5th request", e.sc.Waiting())
			}
		}
		if c := l.Counters(); c.Waiting != 0 {
			t.Errorf("llama.cpp queued %d beyond its slots", c.Waiting)
		}
		if c := v.Counters(); c.Waiting != 0 {
			t.Errorf("vLLM queued %d beyond its configured capacity", c.Waiting)
		}
		releaseL()
		releaseV()
		for range 5 {
			if code := <-done; code != http.StatusOK {
				t.Errorf("status %d", code)
			}
		}
	})
}

// The ollama CLI works through Pharos: it sends HEAD / before every command
// and reads /api/version (0.32.15, spike 2026-09-29). Model management isn't
// Pharos's job and says so in Ollama's error shape, which the CLI prints.
func TestOllamaClientRoutes(t *testing.T) {
	o, v := fake(t, engine.Ollama, "a"), fake(t, engine.VLLM, "org/c")
	e := start(t, state.BackendSpec{URL: o.URL()}, state.BackendSpec{URL: v.URL()})
	e.rounds(1)
	do := func(method, path, body string) (int, string) {
		req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	ollamaErr := func(body string) string {
		var e struct{ Error string }
		json.Unmarshal([]byte(body), &e)
		return e.Error
	}
	cases := []struct {
		name, method, path, body string
		status                   int
		want                     func(body string) bool
	}{
		{"heartbeat", "HEAD", "/", "", 200, func(string) bool { return true }},
		{"root", "GET", "/", "", 200, func(b string) bool { return b == "Ollama is running" }},
		{"unknown_path_is_still_404", "GET", "/nope", "", 404, func(string) bool { return true }},
		{"version_of_the_ollama_backend", "GET", "/api/version", "", 200, func(b string) bool { return strings.Contains(b, `"version":"0.34.4"`) }},
		{"show_unknown_model_lets_the_cli_offer_a_pull", "POST", "/api/show", `{"model":"nope"}`, 404, func(b string) bool { return ollamaErr(b) != "" }},
		{"show_of_a_non_ollama_model", "POST", "/api/show", `{"model":"org/c"}`, 404, func(b string) bool { return ollamaErr(b) != "" }},
		{"pull_is_not_ours", "POST", "/api/pull", `{"model":"a"}`, 501, func(b string) bool { return strings.Contains(ollamaErr(b), "/api/pull") }},
		{"delete_is_not_ours", "DELETE", "/api/delete", `{"model":"a"}`, 501, func(b string) bool { return ollamaErr(b) != "" }},
		{"openai_model_by_id", "GET", "/v1/models/org/c", "", 200, func(b string) bool { return strings.Contains(b, `"id":"org/c"`) }},
		{"openai_unknown_model_by_id", "GET", "/v1/models/nope", "", 404, func(b string) bool { return strings.Contains(b, "model_not_found") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if status, body := do(c.method, c.path, c.body); status != c.status || !c.want(body) {
				t.Errorf("%s %s: %d %s", c.method, c.path, status, body)
			}
		})
	}
	t.Run("no_ollama_backend_up_has_no_version", func(t *testing.T) {
		e := start(t, state.BackendSpec{URL: v.URL()})
		e.rounds(1)
		resp, err := http.Get(e.srv.URL + "/api/version")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("status %d", resp.StatusCode)
		}
	})
}

func TestOlderVersion(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"0.32.15", "0.34.4", true},
		{"0.34.4", "0.32.15", false},
		{"0.9.0", "0.10.0", true}, // not by string
		{"0.34.4", "0.34.4", false},
		{"0.34", "0.34.1", true},
	} {
		if got := older(c.a, c.b); got != c.want {
			t.Errorf("older(%s, %s) = %v", c.a, c.b, got)
		}
	}
}

// `ollama rm` and `ollama stop` send a generate with no prompt and keep_alive
// 0 (0.32.15, live). Ollama answers at once: learned as a cold dispatch, it
// read as a load time of ~0 and made every cold host look free to load.
func TestOllamaLoadAndUnloadRequestsTeachNothing(t *testing.T) {
	o := fake(t, engine.Ollama, "a")
	e := start(t, state.BackendSpec{URL: o.URL()})
	e.rounds(1)
	for _, body := range []string{`{"model":"a","keep_alive":0}`, `{"model":"a","prompt":""}`} {
		if code, _ := e.chat(t, "/api/generate", body); code != http.StatusOK {
			t.Fatalf("%s: %d", body, code)
		}
		d, got := e.next(t)
		if !slices.Contains(got, "load/control") || d.Status != http.StatusOK {
			t.Errorf("%s: decisions %v, status %d", body, got, d.Status)
		}
	}
	tg := e.st.Targets("a")[0]
	if p, l, s := tg.Estimates(); p.OK || l.OK || s.OK {
		t.Errorf("estimates prefill %v load %v service %v, want all unknown", p, l, s)
	}
	if code, _ := e.chat(t, "/api/generate", `{"model":"a","prompt":"hi"}`); code != http.StatusOK {
		t.Fatal(code)
	}
	if _, got := e.next(t); slices.Contains(got, "load/control") {
		t.Errorf("a prompt is inference: %v", got)
	}
}

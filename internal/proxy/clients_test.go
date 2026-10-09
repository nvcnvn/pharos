package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
	"github.com/nvcnvn/pharos/internal/state"
)

// Layer 3 with client captures: requests exactly as a real client sent them
// (testdata/clients/<client>/<version>/, recorded by examples/mac-native/webui.py).

// capture is one recorded request.
type capture struct {
	Method, Path string
	Headers      map[string]string
	Body         string
}

func captures(t *testing.T, dir string) []capture {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join("testdata/clients", dir, "*.json"))
	if len(files) == 0 {
		t.Fatalf("no captures in %s", dir)
	}
	var out []capture
	for _, f := range files {
		b, err := os.ReadFile(f)
		var c capture
		if err == nil {
			err = json.Unmarshal(b, &c)
		}
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		out = append(out, c)
	}
	return out
}

// Open WebUI v0.11.4 typed a three-turn chat: \u-escaped bodies, its 35
// built-in tools (~23 KB) on every chat request, a tool call and its result in
// the history, and title, tag and follow-up requests beside the chat. Every
// request is answered, and each chat request after the first finds the
// conversation cached where the previous one ran.
func TestOpenWebUIChatReplays(t *testing.T) {
	a, b := fake(t, engine.VLLM, "qwen2.5:3b"), fake(t, engine.VLLM, "qwen2.5:3b")
	e := start(t, state.BackendSpec{URL: a.URL()}, state.BackendSpec{URL: b.URL()})
	e.rounds(1)
	type turn struct{ target, prefix string }
	var chat []turn
	for _, c := range captures(t, "open-webui/v0.11.4-openai") {
		req, _ := http.NewRequest(c.Method, e.srv.URL+c.Path, strings.NewReader(c.Body))
		req.Header.Set("Content-Type", c.Headers["Content-Type"])
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: %d", c.Method, c.Path, resp.StatusCode)
		}
		if c.Method != http.MethodPost {
			continue
		}
		d, decisions := e.next(t)
		var body struct{ Stream bool }
		json.Unmarshal([]byte(c.Body), &body)
		if !body.Stream { // a title, tag or follow-up task
			continue
		}
		tr := turn{target: d.Target}
		for _, x := range decisions {
			if o, ok := strings.CutPrefix(x, "prefix/"); ok {
				tr.prefix = o
			}
		}
		chat = append(chat, tr)
	}
	if len(chat) != 4 {
		t.Fatalf("%d streamed chat requests, want 4 (three turns and the reply to a tool result)", len(chat))
	}
	for i, tr := range chat[1:] {
		if tr.target != chat[i].target || tr.prefix != "predicted_hit" {
			t.Errorf("chat request %d: on %s with prefix %s, want predicted_hit on %s", i+2, tr.target, tr.prefix, chat[i].target)
		}
	}
}

// llama.cpp b6890 started without --jinja answers every request with tools
// 500 at once, and Open WebUI sends tools with every chat. A slot that fails
// at once looks free: the refusal must be retried on another engine, and the
// refused prompt must not pull the next requests back by prefix affinity.
// The client never sees the refusal, so the log says what the engine wants.
func TestEngineRefusingToolsIsRetriedElsewhere(t *testing.T) {
	var logs syncBuffer
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	const model = "qwen2.5:3b"
	good := fake(t, engine.VLLM, model)
	refusing := fakeengine.New(fakeengine.Config{Kind: engine.LlamaCpp, Models: []fakeengine.Model{{Name: model, SizeBytes: 1 << 30}}, RefuseTools: true})
	t.Cleanup(refusing.Close)
	e := start(t, state.BackendSpec{URL: good.URL(), Capacity: 1}, state.BackendSpec{URL: refusing.URL()})
	var body string
	for _, c := range captures(t, "open-webui/v0.11.4-openai") {
		if strings.Contains(c.Body, `"tools"`) {
			body = c.Body
			break
		}
	}
	// Fill the good engine's one slot from outside Pharos, so the first try
	// goes to the engine that refuses.
	release := good.Hold()
	busy := post(context.Background(), t, good.URL()+"/v1/chat/completions", chatBody(model, "elsewhere"))
	defer busy.Body.Close()
	if err := good.WaitFor(func(c fakeengine.Counters) bool { return c.Running == 1 }, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	e.rounds(1)
	code := make(chan int, 1)
	go func() {
		resp, err := http.Post(e.srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			code <- 0
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		code <- resp.StatusCode
	}()
	if err := refusing.WaitFor(func(c fakeengine.Counters) bool { return c.Refused == 1 }, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	release()
	io.Copy(io.Discard, busy.Body)
	e.rounds(1) // Pharos sees the good engine free again
	if c := <-code; c != http.StatusOK {
		t.Fatalf("status %d, want the refusal retried on the other engine", c)
	}
	for range 4 {
		if c, _ := e.chat(t, "/v1/chat/completions", body); c != http.StatusOK {
			t.Fatalf("status %d", c)
		}
	}
	if n := refusing.Counters().Refused; n != 1 {
		t.Errorf("the refusing engine was tried %d times, want once", n)
	}
	if l := logs.String(); !strings.Contains(l, "level=WARN") || !strings.Contains(l, "tools param requires --jinja flag") {
		t.Errorf("no warning with the engine's message in the log:\n%s", l)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// With no other engine for the model, the client gets the refusing engine's
// own answer, which names the fix, not "no backend serving this model is up".
func TestARefusalNoOtherEngineTakesReachesTheClient(t *testing.T) {
	const model = "qwen2.5:3b"
	refusing := fakeengine.New(fakeengine.Config{Kind: engine.LlamaCpp, Models: []fakeengine.Model{{Name: model, SizeBytes: 1 << 30}}, RefuseTools: true})
	t.Cleanup(refusing.Close)
	e := start(t, state.BackendSpec{URL: refusing.URL()})
	e.rounds(1)
	body := `{"model":"qwen2.5:3b","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"f","parameters":{}}}]}`
	resp := post(t.Context(), t, e.srv.URL+"/v1/chat/completions", body)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError || !strings.Contains(string(b), "--jinja") {
		t.Errorf("status %d: %s", resp.StatusCode, b)
	}
}

// Claude Code 2.1.284 sends Anthropic's /v1/messages?beta=true. Pharos routes
// it like a chat to an engine that serves the route (Ollama 0.32.15's reply is
// replayed here). llama.cpp b6890 answers it 404: Pharos retries elsewhere,
// and with nowhere else to go tells the client which backend lacks the route,
// in a 400, whose message Claude Code shows (a 404 it reads as a bad model).
func TestCodingAgentRoutes(t *testing.T) {
	var cc capture
	for _, c := range captures(t, "claude-code/v2.1.284") {
		if c.Method == http.MethodPost {
			cc = c
		}
	}
	var req struct{ Model string }
	json.Unmarshal([]byte(cc.Body), &req)
	reply, err := os.ReadFile("testdata/engines/ollama-v0.32.15-messages.sse")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var paths []string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"object":"list","data":[{"id":%q}]}`, req.Model)
	})
	mux.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.String())
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write(reply)
	})
	serving := httptest.NewServer(mux)
	t.Cleanup(serving.Close)
	lacking := fakeengine.New(fakeengine.Config{Kind: engine.LlamaCpp, Models: []fakeengine.Model{{Name: req.Model, SizeBytes: 1 << 30}}})
	t.Cleanup(lacking.Close)

	send := func(e *env) (int, string) {
		resp := post(t.Context(), t, e.srv.URL+cc.Path, cc.Body)
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	t.Run("only_an_engine_without_the_route", func(t *testing.T) {
		e := start(t, state.BackendSpec{URL: lacking.URL()})
		e.rounds(1)
		code, body := send(e)
		if code != http.StatusBadRequest || !strings.Contains(body, "route_not_served") || !strings.Contains(body, lacking.URL()+" doesn't serve /v1/messages") {
			t.Errorf("status %d: %s", code, body)
		}
	})
	t.Run("an_engine_with_the_route_serves_every_request", func(t *testing.T) {
		e := start(t, state.BackendSpec{URL: serving.URL, Kind: engine.OpenAI}, state.BackendSpec{URL: lacking.URL()})
		e.rounds(1)
		for i := range 4 {
			if code, body := send(e); code != http.StatusOK || !strings.HasSuffix(body, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n") {
				t.Fatalf("request %d: status %d: %.200s", i, code, body)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if len(paths) != 4 || paths[0] != "/v1/messages?beta=true" {
			t.Errorf("the engine with the route got %q", paths)
		}
	})
}

package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
func TestEngineRefusingToolsIsRetriedElsewhere(t *testing.T) {
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
}

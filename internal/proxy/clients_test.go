package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nvcnvn/pharos/internal/engine"
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

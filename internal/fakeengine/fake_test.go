package fakeengine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
)

// serveCapture serves a recorded capture state as the engine served it (see
// engine's replay test); paths the capture lacks are 404.
func serveCapture(t *testing.T, dir string) *httptest.Server {
	t.Helper()
	tsv, err := os.ReadFile(filepath.Join(dir, "paths.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]int{}
	for line := range strings.Lines(string(tsv)) {
		f := strings.Split(strings.TrimSpace(line), "\t")
		status[f[0]], _ = strconv.Atoi(f[1])
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status[r.URL.Path] != http.StatusOK {
			http.NotFound(w, r)
			return
		}
		body, _ := os.ReadFile(filepath.Join(dir, strings.ReplaceAll(strings.TrimPrefix(r.URL.Path, "/"), "/", "_")))
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func active(p engine.Plan) []string {
	var names []string
	for _, pr := range p.Active {
		names = append(names, pr.Name)
	}
	return names
}

// The fake is only useful if Pharos reads it as it reads the real engine: the
// same detected kind and the same probes answering.
func TestFakeResolvesLikeItsCapture(t *testing.T) {
	ctx := context.Background()
	for kind, rel := range captures {
		t.Run(string(kind), func(t *testing.T) {
			real := serveCapture(t, filepath.Join(loadTemplates(kind).dir, "busy"))
			want, _, err := engine.Resolve(ctx, http.DefaultClient, real.URL, engine.Auto, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			e := New(Config{Kind: kind, Models: []Model{{Name: "m", SizeBytes: 1 << 30}}})
			defer e.Close()
			got, s, err := engine.Resolve(ctx, http.DefaultClient, e.URL(), engine.Auto, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if got.Kind != want.Kind || !slices.Equal(active(got), active(want)) {
				t.Errorf("fake: %s %v; capture %s: %s %v", got.Kind, active(got), rel, want.Kind, active(want))
			}
			if _, ok := s.Models["m"]; !ok {
				t.Errorf("fake's model not listed: %s", s.Show(engine.Models))
			}
		})
	}
}

// The recorded usage fields carry the fake's counts, streamed or not, and its
// prefix cache serves a repeated prompt.
func TestFakeReportsCachedPrefix(t *testing.T) {
	for kind := range captures {
		for _, stream := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/stream=%v", kind, stream), func(t *testing.T) {
				e := New(Config{Kind: kind, Models: []Model{{Name: "m"}}, CacheTokens: 100_000})
				defer e.Close()
				body := fmt.Sprintf(`{"model":"m","stream":%v,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"%s"}]}`, stream, strings.Repeat("long shared prefix ", 50))
				var got []engine.Usage
				for range 2 {
					resp, err := http.Post(e.URL()+"/v1/chat/completions", "application/json", strings.NewReader(body))
					if err != nil {
						t.Fatal(err)
					}
					b, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					u, _ := usageOf(b)
					got = append(got, u)
				}
				if !got[0].CachedTokens.OK || got[0].CachedTokens.V != 0 || got[1].CachedTokens.V <= 200 || got[1].PromptTokens != got[0].PromptTokens {
					t.Errorf("first %+v, second %+v", got[0], got[1])
				}
			})
		}
	}
}

func TestFakeQueuesBeyondSlots(t *testing.T) {
	e := New(Config{Kind: engine.VLLM, Models: []Model{{Name: "m"}}, Slots: 1})
	defer e.Close()
	release := e.Hold()
	body := `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	done := make(chan struct{})
	for range 2 {
		go func() {
			resp, err := http.Post(e.URL()+"/v1/chat/completions", "application/json", strings.NewReader(body))
			if err == nil {
				resp.Body.Close()
			}
			done <- struct{}{}
		}()
	}
	if err := e.WaitFor(func(c Counters) bool { return c.Running == 1 && c.Waiting == 1 }, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	release()
	<-done
	<-done
}

// usageOf reads a reply the way the stream tap does: the last usage line wins.
func usageOf(body []byte) (engine.Usage, bool) {
	var last engine.Usage
	found := false
	for line := range bytes.Lines(body) {
		if u, ok := engine.ParseUsage(line); ok {
			last, found = u, true
		}
	}
	return last, found
}

// known lists which usage values a reply reports, not their numbers.
func known(u engine.Usage) string {
	return fmt.Sprintf("prompt=%v cached=%v out=%v prefill=%v load=%v", u.PromptTokens.OK, u.CachedTokens.OK, u.CompletionTokens.OK, u.PrefillSec.OK, u.LoadSec.OK)
}

// Every reply the capture recorded, the fake serves with the recorded status
// and the same usage fields, so the proxy meets the engine's reply shapes.
func TestFakeServesEveryRecordedReply(t *testing.T) {
	for kind := range captures {
		tpl := loadTemplates(kind)
		for path, files := range replyFiles {
			for i, file := range files {
				rec, err := os.ReadFile(filepath.Join(tpl.dir, "streams", file))
				if file == "" || err != nil {
					continue // not recorded, or not for this kind
				}
				stream := i == 0
				t.Run(string(kind)+path+map[bool]string{true: "/stream", false: ""}[stream], func(t *testing.T) {
					e := New(Config{Kind: kind, Models: []Model{{Name: "m"}}})
					defer e.Close()
					body := fmt.Sprintf(`{"model":"m","stream":%v,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}],"prompt":"hi","input":"hi"}`, stream)
					resp, err := http.Post(e.URL()+path, "application/json", strings.NewReader(body))
					if err != nil {
						t.Fatal(err)
					}
					got, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					if want := recordedStatus(tpl.replies[file].status); !stream && resp.StatusCode != want {
						t.Fatalf("status %d, recorded %d", resp.StatusCode, want)
					}
					wantU, _ := usageOf(rec)
					gotU, _ := usageOf(got)
					if known(gotU) != known(wantU) {
						t.Errorf("fake reports %s, recording %s", known(gotU), known(wantU))
					}
				})
			}
		}
	}
}

// recordedStatus is a recorded reply's status; streams have none recorded and are 200.
func recordedStatus(s int) int {
	if s == 0 {
		return http.StatusOK
	}
	return s
}

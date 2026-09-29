package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/config"
	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
)

// Keys and static backends apply live on a reload; the other fields are
// reported as needing a restart.
func TestRestartNeeded(t *testing.T) {
	parse := func(yaml string) config.Config {
		c, err := config.Parse([]byte(yaml))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a := parse("listen: :8080\nbackends: [{url: 'http://a:1'}]")
	live := parse("listen: :8080\nbackends: [{url: 'http://b:1'}]\nkeys: [{name: k, sha256: 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08}]")
	if got := restartNeeded(a, live); len(got) != 0 {
		t.Errorf("keys and backends: %v", got)
	}
	restart := parse("listen: :9090\nusage: {timezone: Asia/Tokyo}\ndrain: {grace: 1s}\npeers: {listen: ':8081', secret_file: s, members: [a:8081]}")
	if got := restartNeeded(a, restart); !slices.Equal(got, []string{"listen", "peers", "usage", "drain"}) {
		t.Errorf("got %v", got)
	}
}

// An operator edits the config of a running instance: a new backend takes
// traffic, a removed one finishes its streams and gets no more, new keys are
// enforced at once, and a file saved half-edited leaves the running config in
// place. No restart, no dropped stream.
func TestConfigEditsApplyWhileServing(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // static backends only
	defer func(d time.Duration) { configEvery = d }(configEvery)
	configEvery = 10 * time.Millisecond
	logs := captureLogs(t)

	a := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "a"}}})
	b := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "b"}}})
	defer a.Close()
	defer b.Close()
	addr := freePorts(t, 1)[0]
	cfg := filepath.Join(t.TempDir(), "pharos.yaml")
	write := func(yaml string) {
		t.Helper()
		if err := os.WriteFile(cfg, []byte(fmt.Sprintf("listen: %s\ndrain: {grace: 10ms, timeout: 10s}\n%s", addr, yaml)), 0o600); err != nil {
			t.Fatal(err)
		}
		// The watcher compares modification times; don't depend on the
		// filesystem's resolution.
		later := time.Now().Add(time.Duration(len(yaml)) * time.Second)
		os.Chtimes(cfg, later, later)
	}
	write(fmt.Sprintf("backends: [{url: %q}]\n", a.URL()))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, []string{"-config", cfg}, io.Discard) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	waitFor(t, "healthy", func() bool { return healthy(addr) })
	if status, _ := send(t, addr, "", "b"); status != http.StatusNotFound {
		t.Fatalf("model b before the edit: %d", status)
	}

	// A stream on a, still running when a leaves the config.
	release := a.Hold()
	stream, err := post(addr, "", "a")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	r := bufio.NewReader(stream.Body)
	if _, err := r.ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	sum := config.HashKey("alice-key")
	write(fmt.Sprintf("backends: [{url: %q}]\nkeys: [{name: alice, sha256: %s}]\n", b.URL(), hex.EncodeToString(sum[:])))
	logs.wait(t, "config reloaded")
	t.Run("a_new_key_is_enforced_at_once", func(t *testing.T) {
		if status, _ := send(t, addr, "", "b"); status != http.StatusUnauthorized {
			t.Errorf("no key: %d", status)
		}
	})
	t.Run("an_added_backend_takes_traffic", func(t *testing.T) {
		waitFor(t, "model b served", func() bool { status, _ := send(t, addr, "alice-key", "b"); return status == http.StatusOK })
	})
	t.Run("a_removed_backend_finishes_its_stream", func(t *testing.T) {
		release()
		rest, err := io.ReadAll(r)
		if err != nil || !strings.Contains(string(rest), "[DONE]") {
			t.Errorf("stream cut: %v, %q", err, rest)
		}
	})
	t.Run("a_removed_backend_gets_no_more_requests", func(t *testing.T) {
		before := a.Counters().Requests
		if status, _ := send(t, addr, "alice-key", "a"); status == http.StatusOK || a.Counters().Requests != before {
			t.Errorf("model a after its backend left: status %d, a served %d more", status, a.Counters().Requests-before)
		}
	})
	t.Run("a_broken_file_keeps_the_running_config", func(t *testing.T) {
		write("backends: [{url: \n")
		logs.wait(t, "doesn't load")
		if status, _ := send(t, addr, "alice-key", "b"); status != http.StatusOK {
			t.Errorf("model b with alice's key: %d", status)
		}
		if status, _ := send(t, addr, "", "b"); status != http.StatusUnauthorized {
			t.Errorf("no key: %d", status)
		}
	})
}

// With -log-level debug, each request logs one line saying where it went and
// why, carrying the client's X-Request-Id so a harness can pair the line with
// the request it sent.
func TestDebugLogAttributesEachRequest(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	logs := make(logLines, 1000)
	log.SetOutput(logs) // serve's own slog default, not captureLogs' handler
	t.Cleanup(func() { log.SetOutput(os.Stderr); slog.SetLogLoggerLevel(slog.LevelInfo) })

	a := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "a"}}})
	defer a.Close()
	addr := freePorts(t, 1)[0]
	cfg := filepath.Join(t.TempDir(), "pharos.yaml")
	if err := os.WriteFile(cfg, fmt.Appendf(nil, "listen: %s\ndrain: {grace: 10ms}\nbackends: [{url: %q}]\n", addr, a.URL()), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, []string{"-config", cfg, "-log-level", "debug"}, io.Discard) }()
	defer func() { cancel(); <-done }()
	waitFor(t, "healthy", func() bool { return healthy(addr) })

	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions",
		strings.NewReader(`{"model":"a","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("X-Request-Id", "replay-7")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if line := logs.wait(t, "request_id=replay-7"); !strings.Contains(line, `target="`+a.URL()) {
		t.Errorf("no target on %q", line)
	}
}

// post sends a streamed chat for model through the instance at addr.
func post(addr, key, model string) (*http.Response, error) {
	body := fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, model)
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return http.DefaultClient.Do(req)
}

// send posts a chat and reads the whole reply.
func send(t *testing.T, addr, key, model string) (int, string) {
	t.Helper()
	resp, err := post(addr, key, model)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// logLines receives serve's log messages, so a test can wait for one instead
// of sleeping.
type logLines chan string

func captureLogs(t *testing.T) logLines {
	ch := make(logLines, 1000)
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(ch, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return ch
}

func (l logLines) Write(p []byte) (int, error) {
	select {
	case l <- string(p):
	default:
	}
	return len(p), nil
}

// wait returns the first line containing text.
func (l logLines) wait(t *testing.T, text string) string {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case line := <-l:
			if strings.Contains(line, text) {
				return line
			}
		case <-deadline:
			t.Fatalf("no log line with %q", text)
		}
	}
}
func freePorts(t *testing.T, n int) []string {
	var out []string
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, l.Addr().String())
		defer l.Close()
	}
	return out
}

func healthy(addr string) bool {
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// waitFor polls cond for up to 20 s; got, if given, describes the last state.
func waitFor(t *testing.T, what string, cond func() bool, got ...func() string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			for _, g := range got {
				what += ", got " + g()
			}
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

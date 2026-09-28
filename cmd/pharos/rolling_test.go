//go:build integration

package main

// Rolling restart of three instances under traffic (ARCHITECTURE §14, layer 3
// multi-instance). The multi-instance tests in internal/peer don't run serve,
// where drain lives, so this runs three real serve loops in one process on
// real sockets and a real clock, against fake engines. No Docker, no engine:
// it runs in seconds anywhere.
//
//	go test -tags integration -v -run TestRollingRestart ./cmd/pharos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
)

// Every stream that starts completes, and cluster usage equals the requests
// sent: a graceful restart loses nothing.
func TestRollingRestartUnderTraffic(t *testing.T) {
	var backends []string
	for range 2 {
		f := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}, Slots: 4,
			CacheTokens: 100_000, DecodeSecTok: 0.01, Speed: 1}) // 30 tokens: a stream lasts ~0.3 s
		t.Cleanup(f.Close)
		backends = append(backends, fmt.Sprintf("{url: %q}", f.URL()))
	}
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	os.WriteFile(secret, []byte("s3cret"), 0o600)
	api, peers := freePorts(t, 3), freePorts(t, 3)

	type instance struct {
		cancel context.CancelFunc
		done   chan error
	}
	insts := make([]*instance, 3)
	start := func(i int) {
		cfg := filepath.Join(dir, fmt.Sprintf("pharos%d.yaml", i))
		os.WriteFile(cfg, fmt.Appendf(nil, "listen: %s\nbackends: [%s]\npeers: {listen: %s, secret_file: %s, members: [%s]}\ndrain: {grace: 300ms, timeout: 30s}\n",
			api[i], strings.Join(backends, ", "), peers[i], secret, strings.Join(peers, ", ")), 0o600)
		ctx, cancel := context.WithCancel(context.Background())
		in := &instance{cancel, make(chan error, 1)}
		go func() { in.done <- serve(ctx, []string{"-config", cfg}, io.Discard) }()
		insts[i] = in
		waitFor(t, api[i]+" healthy", func() bool { return healthy(api[i]) })
	}
	for i := range 3 {
		start(i)
	}

	// Clients act as the load balancer: each request goes to the next
	// instance whose /healthz passes.
	var sent, next atomic.Int64
	var failed []string
	var mu sync.Mutex
	stop := make(chan struct{})
	var clients sync.WaitGroup
	for c := range 6 {
		clients.Go(func() {
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				addr := api[next.Add(1)%3]
				if !healthy(addr) {
					continue
				}
				// A conversation per client, so the prefix index has work to do.
				body := fmt.Sprintf(`{"model":"m","stream":true,"max_tokens":30,"messages":[{"role":"system","content":%q},{"role":"user","content":"turn %d"}]}`,
					strings.Repeat(fmt.Sprintf("Client %d's long system prompt. ", c), 50), n)
				if err := chat(addr, body); err != nil {
					mu.Lock()
					failed = append(failed, fmt.Sprintf("%s: %v", addr, err))
					mu.Unlock()
					continue
				}
				sent.Add(1)
			}
		})
	}

	waitFor(t, "traffic", func() bool { return sent.Load() >= 20 })
	for i := range 3 {
		insts[i].cancel() // SIGTERM
		if err := <-insts[i].done; err != nil {
			t.Fatalf("instance %d: serve: %v", i, err)
		}
		start(i)
		before := sent.Load()
		waitFor(t, "traffic after the restart", func() bool { return sent.Load() >= before+20 })
	}
	close(stop)
	clients.Wait()
	for _, f := range failed {
		t.Error("cut:", f)
	}

	// Every instance sees the whole cluster's requests once a tick has passed.
	for i := range 3 {
		waitFor(t, fmt.Sprintf("usage on %s = %d sent", api[i], sent.Load()), func() bool { return requestsToday(api[i]) == sent.Load() },
			func() string { return fmt.Sprint(requestsToday(api[0]), requestsToday(api[1]), requestsToday(api[2])) })
	}
	t.Logf("%d requests through three restarts, none cut, usage matches", sent.Load())
	for _, in := range insts {
		in.cancel()
	}
	for _, in := range insts {
		<-in.done
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

// chat sends a streamed chat and reads it to the end; a stream that stops
// before [DONE] was cut.
func chat(addr, body string) error {
	resp, err := http.Post("http://"+addr+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	switch {
	case err != nil:
		return err
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("status %d: %s", resp.StatusCode, b)
	case !bytes.HasSuffix(bytes.TrimSpace(b), []byte("data: [DONE]")):
		return fmt.Errorf("stream ended without [DONE]: %q", b[max(0, len(b)-200):])
	}
	return nil
}

func requestsToday(addr string) int64 {
	resp, err := http.Get("http://" + addr + "/usage")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var u struct {
		Rows []struct {
			Day      string `json:"day"`
			Requests int64  `json:"requests"`
		} `json:"rows"`
	}
	json.NewDecoder(resp.Body).Decode(&u)
	var n int64
	today := time.Now().Format(time.DateOnly)
	for _, r := range u.Rows {
		if r.Day == today {
			n += r.Requests
		}
	}
	return n
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

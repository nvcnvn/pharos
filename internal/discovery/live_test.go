//go:build integration

package discovery

// Layer 4: Docker label discovery and the log follow loop against a real
// Docker and a real Ollama container.
//
//	go test -tags integration -v -run TestLiveDocker ./internal/discovery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/state"
)

// The pinned Ollama of internal/engine's live test, whose startup "server
// config" line the log probe reads.
const (
	ollamaImage = "ollama/ollama:0.34.4"
	ollamaModel = "qwen2.5:0.5b"
)

func docker(t *testing.T, args ...string) string {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command("docker", args...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(args, " "), err, errb.String())
	}
	return strings.TrimSpace(out.String())
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(200 * time.Millisecond) // layer 4: a real engine on a real clock
	}
}

// A labeled container is discovered, its log feed fills Capacity (the
// container runs OLLAMA_NUM_PARALLEL=3, which no endpoint reports), the feed
// comes back after a restart, and the backend goes when the container does.
func TestLiveDockerLabelsAndLogFeed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !DockerAvailable(ctx) {
		t.Fatal("no Docker socket")
	}
	port := freePort(t)
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	docker(t, "volume", "create", "pharos-models")
	name := fmt.Sprintf("pharos-live-%d", port)
	id := docker(t, "run", "-d", "--name", name, "-p", fmt.Sprintf("127.0.0.1:%d:11434", port),
		"-e", "OLLAMA_NUM_PARALLEL=3", "-e", "OLLAMA_MODELS=/cache/ollama", "-v", "pharos-models:/cache",
		"-l", "pharos.enable=true", "-l", "pharos.url="+url, "-l", "pharos.memory_gb=8", ollamaImage)
	defer exec.Command("docker", "rm", "-f", name).Run()
	waitFor(t, "ollama up", time.Minute, func() bool {
		resp, err := http.Get(url + "/api/version")
		if err == nil {
			resp.Body.Close()
		}
		return err == nil && resp.StatusCode == http.StatusOK
	})
	if !strings.Contains(docker(t, "exec", name, "ollama", "list"), ollamaModel) {
		docker(t, "exec", name, "ollama", "pull", ollamaModel)
	}

	st := state.New(nil, state.Options{
		Client: &http.Client{Timeout: 5 * time.Second},
		OpenLogs: func(ctx context.Context, logs string) (io.ReadCloser, error) {
			return DockerLogs(ctx, logs, true)
		},
	})
	go st.Run(ctx)
	go DockerLabels(ctx, st.SetBackends)

	target := func() *state.Target {
		if ts := st.Targets(ollamaModel); len(ts) == 1 {
			return ts[0]
		}
		return nil
	}
	capacity := func() engine.Opt[int] {
		if tg := target(); tg != nil {
			return tg.View(st.Now()).Capacity
		}
		return engine.Opt[int]{}
	}
	waitFor(t, "the container discovered with capacity 3 from its log", time.Minute, func() bool { return capacity().V == 3 })
	tg := target()
	if b := tg.Backend.Spec; b.URL != url || b.Logs != id || b.Kind != engine.Auto || b.MemoryBytes != 8<<30 || tg.View(st.Now()).Kind != engine.Ollama {
		t.Errorf("backend %+v, kind %s", b, tg.View(st.Now()).Kind)
	}

	// The old feed ends when the container stops: its value must go unknown
	// (not stay, not 0), then come back from the new container's log.
	dropped := make(chan bool)
	go func() {
		deadline := time.Now().Add(time.Minute)
		for capacity().OK && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		dropped <- !capacity().OK
	}()
	docker(t, "restart", "-t", "1", name)
	if !<-dropped {
		t.Fatal("capacity stayed known across the restart")
	}
	waitFor(t, "capacity 3 again after the restart", 2*time.Minute, func() bool { return capacity().V == 3 })
	if target() != tg {
		t.Error("the restarted container (same labels) got a new target")
	}

	docker(t, "rm", "-f", name)
	waitFor(t, "the backend gone with its container", time.Minute, func() bool { return target() == nil })
}

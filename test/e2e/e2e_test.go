//go:build integration

// Package e2e runs the Pharos Docker image in front of real engines, all found
// by their Docker labels (compose.yaml): two Ollama containers serving the same
// model and one llama.cpp. It asserts only what needs the image and real
// engines together; routing and accounting are proven in the layers below.
//
//	go test -tags integration -timeout 20m -v ./test/e2e
package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	ollamaModel   = "qwen2.5:0.5b"
	llamacppModel = "qwen2.5-0.5b"
)

var base = "http://127.0.0.1:" + port()

func port() string {
	if p := os.Getenv("PORT"); p != "" {
		return p
	}
	return "18090"
}

func compose(t *testing.T, args ...string) string {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command("docker", append([]string{"compose", "-f", "compose.yaml"}, args...)...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, errb.String())
	}
	return strings.TrimSpace(out.String())
}

// waitFor polls cond on a real clock: real containers, real engines.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > timeout {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(time.Second)
	}
	t.Logf("%s after %s", what, time.Since(start).Round(time.Second))
}

func get(path string) (int, string) {
	resp, err := http.Get(base + path)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// stream starts a streamed chat without include_usage, as most clients send
// it, and returns the reply once its first content line arrived.
func stream(t *testing.T, model string, maxTokens int) (*http.Response, *bufio.Reader) {
	t.Helper()
	body := fmt.Sprintf(`{"model":%q,"stream":true,"max_tokens":%d,"temperature":0,"messages":[{"role":"user","content":"Count from 1 to 5000, one number per line."}]}`, model, maxTokens)
	resp, err := http.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s: status %d: %s", model, resp.StatusCode, b)
	}
	r := bufio.NewReader(resp.Body)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("%s: stream ended before any content: %v", model, err)
		}
		if strings.Contains(line, `"content":"`) {
			return resp, r
		}
	}
}

func TestE2E(t *testing.T) {
	if out, err := exec.Command("docker", "volume", "create", "pharos-models").CombinedOutput(); err != nil {
		t.Fatalf("docker volume create: %v: %s", err, out)
	}
	compose(t, "up", "-d", "--build")
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(compose(t, "logs", "--no-color", "--tail", "60", "pharos"))
		}
		compose(t, "down", "--remove-orphans")
	})
	compose(t, "exec", "ollama-a", "ollama", "pull", ollamaModel) // the shared volume: ollama-b sees it too

	t.Run("the_image_discovers_every_labeled_engine_and_detects_its_kind", func(t *testing.T) {
		waitFor(t, "/healthz", 5*time.Minute, func() bool { code, _ := get("/healthz"); return code == http.StatusOK })
		up := regexp.MustCompile(`(?m)^pharos_backend_up\{backend="[^"]+",kind="(\w+)",version="[^"]*"\} 1$`)
		waitFor(t, "two Ollama and one llama.cpp backend up", 10*time.Minute, func() bool {
			_, m := get("/metrics")
			kinds := map[string]int{}
			for _, k := range up.FindAllStringSubmatch(m, -1) {
				kinds[k[1]]++
			}
			if kinds["openai"] > 0 { // a guessed kind holds until the 10-minute re-resolve
				t.Fatalf("an engine was detected as the generic kind:\n%s", m)
			}
			return kinds["ollama"] == 2 && kinds["llamacpp"] == 1
		})
		waitFor(t, "both models listed", 5*time.Minute, func() bool {
			_, m := get("/v1/models")
			return strings.Contains(m, `"`+ollamaModel+`"`) && strings.Contains(m, `"`+llamacppModel+`"`)
		})
	})

	t.Run("a_stream_arrives_whole_and_is_metered", func(t *testing.T) {
		for _, model := range []string{ollamaModel, llamacppModel} {
			resp, r := stream(t, model, 20)
			rest, err := io.ReadAll(r)
			resp.Body.Close()
			if err != nil || !bytes.HasSuffix(bytes.TrimSpace(rest), []byte("data: [DONE]")) || bytes.Contains(rest, []byte(`"usage":{`)) {
				t.Errorf("%s: want the stream to [DONE] without the usage chunk Pharos asked for: %v\n%s", model, err, rest)
			}
		}
		_, body := get("/usage?by=model")
		var u struct {
			Rows []struct {
				Name         string `json:"name"`
				Requests     int    `json:"requests"`
				PromptTokens int    `json:"prompt_tokens"`
				Unmetered    int    `json:"unmetered_requests"`
			} `json:"rows"`
		}
		json.Unmarshal([]byte(body), &u)
		metered := map[string]bool{}
		for _, row := range u.Rows {
			metered[row.Name] = row.Requests > 0 && row.PromptTokens > 0 && row.Unmetered == 0
		}
		if !metered[ollamaModel] || !metered[llamacppModel] {
			t.Errorf("usage: %s", body)
		}
	})

	t.Run("an_engine_dying_mid_stream_cuts_only_its_stream", func(t *testing.T) {
		resp, r := stream(t, ollamaModel, 2000)
		defer resp.Body.Close()
		// Which Ollama serves it: the target with a request in flight.
		busy := regexp.MustCompile(`pharos_target_inflight\{target="http://([0-9.]+):\d+[^"]*",model="` + regexp.QuoteMeta(ollamaModel) + `"\} 1`)
		_, m := get("/metrics")
		ip := busy.FindStringSubmatch(m)
		if ip == nil {
			t.Fatalf("no Ollama target with a request in flight:\n%s", m)
		}
		victim := ""
		for _, svc := range []string{"ollama-a", "ollama-b"} {
			id := compose(t, "ps", "-q", svc)
			addr, _ := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", id).Output()
			if strings.TrimSpace(string(addr)) == ip[1] {
				victim = svc
			}
		}
		if victim == "" {
			t.Fatalf("no container has IP %s", ip[1])
		}
		compose(t, "kill", victim)
		rest, _ := io.ReadAll(r)
		if bytes.Contains(rest, []byte("[DONE]")) {
			t.Errorf("the stream on %s completed although it was killed", victim)
		}
		resp2, r2 := stream(t, ollamaModel, 20)
		rest2, _ := io.ReadAll(r2)
		resp2.Body.Close()
		if !bytes.Contains(rest2, []byte("[DONE]")) {
			t.Errorf("next request, with %s down: %s", victim, rest2)
		}
	})

	t.Run("sigterm_lets_a_stream_in_flight_finish", func(t *testing.T) {
		resp, r := stream(t, llamacppModel, 300)
		defer resp.Body.Close()
		compose(t, "kill", "-s", "SIGTERM", "pharos")
		rest, err := io.ReadAll(r)
		if err != nil || !bytes.HasSuffix(bytes.TrimSpace(rest), []byte("data: [DONE]")) {
			t.Errorf("stream cut by the shutdown: %v\n%s", err, rest[max(0, len(rest)-300):])
		}
		waitFor(t, "pharos to exit", 2*time.Minute, func() bool {
			return compose(t, "ps", "-a", "--format", "{{.State}} {{.ExitCode}}", "pharos") == "exited 0"
		})
	})
}

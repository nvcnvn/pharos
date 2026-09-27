//go:build integration

package engine

// Layer 4 (ARCHITECTURE §14). Each engine starts from its profile in
// test/engines/<engine>/. capture.sh drives it through idle → loaded → busy →
// cold and records every state with `pharos doctor -record`, the same recorder
// users run. This test then resolves the bodies the engine just served and
// asserts behavior through Resolve and Scrape.
//
//	go test -tags integration -timeout 90m -v -run TestLive ./internal/engine
//
//	PHAROS_LIVE_ENGINES=ollama,vllm  engines to run (default ollama,llamacpp,llama-swap,vllm)
//	PHAROS_LIVE_VERSION=latest       each engine's latest release instead of the pinned one
//	PHAROS_RECORD=1                  keep the capture in testdata/<engine>/<version>/ (review the diff)

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// livePinned is the version each engine is tested at on PRs. Bump it together
// with a new testdata/<engine>/<version>/ capture.
var livePinned = map[string]string{
	"ollama": "v0.34.4", "llamacpp": "v0.5.0", "llama-swap": "v260", "vllm": "v0.30.0",
	"sglang": "v0.5.20", "mlx-lm": "v0.31.3",
}

// liveWant is what each engine must show, whatever its version. A signal the
// engine doesn't report must be unknown; if a release starts reporting it the
// test fails, and docs/SUPPORT.md gets a new cell.
type liveWant struct {
	kind      Kind
	busy      bool // busy → Running=2, Waiting=2 (2 slots, 4 requests); false = both unknown
	residency bool // loaded/busy → Loaded, cold → Cold; false = Residency unknown (single model, always loaded)
	capacity  bool // Capacity=2 from a scraped or a log probe; false = unknown
}

var liveWants = map[string]liveWant{
	"ollama":     {kind: Ollama, residency: true, capacity: true}, // capacity from the log
	"llamacpp":   {kind: LlamaCpp, busy: true, capacity: true},
	"llama-swap": {kind: LlamaSwap, residency: true},
	"vllm":       {kind: VLLM, busy: true},
	"sglang":     {kind: OpenAI}, // no recipe yet
	"mlx-lm":     {kind: OpenAI},
}

func TestLive(t *testing.T) {
	engines := strings.Split(cmp(os.Getenv("PHAROS_LIVE_ENGINES"), "ollama,llamacpp,llama-swap,vllm"), ",")
	for _, name := range engines {
		t.Run(name, func(t *testing.T) {
			want, ok := liveWants[name]
			if !ok {
				t.Fatalf("no liveWants row for %s", name)
			}
			dir := runCapture(t, name)
			checkLive(t, dir, want)
		})
	}
}

func cmp(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// runCapture runs capture.sh for engine and returns the capture dir it wrote.
func runCapture(t *testing.T, engine string) string {
	args := []string{engine}
	if os.Getenv("PHAROS_LIVE_VERSION") != "latest" {
		args = append(args, livePinned[engine])
	}
	cmd := exec.CommandContext(context.Background(), "../../test/engines/capture.sh", args...)
	cmd.Env = os.Environ()
	if os.Getenv("PHAROS_RECORD") != "1" {
		cmd.Env = append(cmd.Env, "CAPTURE_OUT="+filepath.Join(t.TempDir(), engine))
	}
	var out bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &out)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("capture.sh %s: %v", strings.Join(args, " "), err)
	}
	for line := range strings.Lines(out.String()) {
		if dir, ok := strings.CutPrefix(line, "done: "); ok {
			dir, _, _ = strings.Cut(dir, " (")
			if !filepath.IsAbs(dir) {
				dir = filepath.Join("../../test/engines", dir)
			}
			return dir
		}
	}
	t.Fatal("capture.sh printed no done: line")
	return ""
}

func checkLive(t *testing.T, dir string, want liveWant) {
	ctx := context.Background()
	states := map[string]Snapshot{}
	var plan Plan
	for _, state := range []string{"idle", "loaded", "busy", "cold"} {
		if _, err := os.Stat(filepath.Join(dir, state, "paths.tsv")); err != nil {
			continue // cold only where the engine unloads
		}
		srv := serveCapture(t, filepath.Join(dir, state))
		p, s, err := Resolve(ctx, srv.Client(), srv.URL, Auto, nil, true)
		if err != nil {
			t.Fatalf("%s: %v", state, err)
		}
		if p.Kind != want.kind {
			t.Errorf("%s: detected %s, want %s", state, p.Kind, want.kind)
		}
		s2, err := p.Scrape(ctx, srv.Client(), srv.URL)
		if err != nil {
			t.Errorf("%s: scrape: %v", state, err)
		}
		for sig := range s.From {
			if s2.Show(sig) != s.Show(sig) {
				t.Errorf("%s: scrape %s = %s, resolve read %s", state, sig, s2.Show(sig), s.Show(sig))
			}
		}
		states[state], plan = s, p
		t.Logf("%s: %s", state, showAll(s))
	}
	for _, state := range []string{"idle", "loaded", "busy"} {
		if _, ok := states[state]; !ok {
			t.Fatalf("no %s capture", state)
		}
	}

	// Running and Waiting: 2 slots, 4 slow requests.
	for _, sig := range []Signal{Running, Waiting} {
		for state, n := range map[string]int{"idle": 0, "busy": 2} {
			got, ok := sumLoad(states[state], sig)
			switch {
			case want.busy && (!ok || got != n):
				t.Errorf("%s: %s = %s, want %d", state, sig, states[state].Show(sig), n)
			case !want.busy && ok:
				t.Errorf("%s: %s = %s, want unknown (not reported by this engine)", state, sig, states[state].Show(sig))
			}
		}
	}

	// Residency: in memory after a request, cold after keep-alive.
	for state, s := range states {
		wantState := map[string]ResidencyState{"loaded": Loaded, "busy": Loaded, "cold": Cold}[state]
		switch {
		case !want.residency && s.From[Residency] != "":
			t.Errorf("%s: residency = %s, want unknown", state, s.Show(Residency))
		case want.residency && wantState != Unknown:
			if len(s.Models) == 0 {
				t.Errorf("%s: no models", state)
			}
			for m, info := range s.Models {
				if info.State != wantState {
					t.Errorf("%s: %s is %s, want %s", state, m, info.State, wantState)
				}
			}
		}
	}
	if want.residency {
		if _, ok := states["cold"]; !ok {
			t.Error("no cold capture; the profile needs UNLOAD_WAIT")
		}
	}

	// Capacity: scraped, or from the engine log over the whole run.
	capacity := states["loaded"]
	if capacity.From[Capacity] == "" {
		f, err := os.Open(filepath.Join(dir, "engine.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := plan.Follow(ctx, f, func(s Snapshot) {
			if s.From[Capacity] != "" {
				capacity = s
			}
		}); err != nil {
			t.Errorf("follow engine.log: %v", err)
		}
	}
	got, ok := sumLoad(capacity, Capacity)
	switch {
	case want.capacity && (!ok || got != 2):
		t.Errorf("capacity = %s, want 2", capacity.Show(Capacity))
	case !want.capacity && ok:
		t.Errorf("capacity = %s from %s, want unknown", capacity.Show(Capacity), capacity.From[Capacity])
	}

	// The same ~3200-token prefix twice: the second reply reports it cached.
	first, second := cachedTokens(t, filepath.Join(dir, "streams", "openai-chat.1.sse")), cachedTokens(t, filepath.Join(dir, "streams", "openai-chat.2.sse"))
	if second <= 0 || second <= first {
		t.Errorf("cached_tokens: first %d, second %d; want the second > 0 and > the first", first, second)
	}
}

func sumLoad(s Snapshot, sig Signal) (int, bool) {
	total, known := 0.0, false
	for key := range s.Load {
		if v, ok := loadSignal(s, sig, key); ok {
			total, known = total+v, true
		}
	}
	return int(total), known
}

func showAll(s Snapshot) string {
	var parts []string
	for sig := Version; sig <= KVUsage; sig++ {
		if s.From[sig] != "" {
			parts = append(parts, sig.String()+"="+s.Show(sig))
		}
	}
	return strings.Join(parts, " ")
}

// cachedTokens reads usage.prompt_tokens_details.cached_tokens from the last
// SSE chunk that has usage (stream_options.include_usage). -1 = not reported.
func cachedTokens(t *testing.T, file string) int {
	f, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := -1
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var chunk struct {
			Usage *struct {
				Details *struct {
					Cached *int `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			} `json:"usage"`
		}
		if json.Unmarshal([]byte(data), &chunk) == nil && chunk.Usage != nil && chunk.Usage.Details != nil && chunk.Usage.Details.Cached != nil {
			n = *chunk.Usage.Details.Cached
		}
	}
	return n
}

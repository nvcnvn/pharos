package engine

import (
	"errors"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// serveCapture serves one capture state dir (testdata/<engine>/<version>/<state>)
// the way the engine served it: every path in paths.tsv with its recorded
// status and content type. 404s have no body file; a missing body file for
// another status is an empty body (SGLang's /health). A request for a path the
// capture didn't record fails the test, because captures must cover every path
// a probe reads.
func serveCapture(t *testing.T, dir string) *httptest.Server {
	t.Helper()
	tsv, err := os.ReadFile(filepath.Join(dir, "paths.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	type record struct {
		status int
		ctype  string
		body   []byte
	}
	records := map[string]record{}
	for line := range strings.Lines(string(tsv)) {
		if line = strings.TrimRight(line, "\r\n"); line == "" {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 3 {
			t.Fatalf("%s/paths.tsv: want path, status, content type in %q", dir, line)
		}
		r := record{ctype: f[2]}
		if r.status, err = strconv.Atoi(f[1]); err != nil {
			t.Fatalf("%s/paths.tsv: %q: %v", dir, line, err)
		}
		if r.status != http.StatusNotFound {
			r.body, err = os.ReadFile(filepath.Join(dir, captureFile(f[0])))
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatal(err)
			}
		}
		records[f[0]] = r
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r, ok := records[req.URL.Path]
		if !ok {
			t.Errorf("%s: capture has no record of %s", dir, req.URL.Path)
			http.NotFound(w, req)
			return
		}
		if r.ctype != "" {
			w.Header().Set("Content-Type", r.ctype)
		}
		w.WriteHeader(r.status)
		w.Write(r.body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// captures returns every capture state dir, keyed "engine/version/state".
func captures(t *testing.T) map[string]string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("testdata", "*", "*", "*", "paths.tsv"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("no captures: %v", err)
	}
	dirs := map[string]string{}
	for _, m := range matches {
		dir := filepath.Dir(m)
		rel, _ := filepath.Rel("testdata", dir)
		dirs[filepath.ToSlash(rel)] = dir
	}
	return dirs
}

func get(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

// TestReplayLibrary runs every library probe against every capture that serves
// its path. Rows are keyed "engine/version" (every state) or
// "engine/version/state" (overrides). A capture without a row must leave the
// signal unknown; "error" means Parse must fail. Values are Show()n.
func TestReplayLibrary(t *testing.T) {
	const (
		ollama    = "ollama/v0.34.4"
		llamacpp  = "llamacpp/v0.5.0"
		llamaswap = "llama-swap/v260"
		vllm      = "vllm/v0.30.0"
		sglang    = "sglang/v0.5.20"
		mlx       = "mlx-lm/v0.31.3"
		failed    = "error"
	)
	rows := map[string]map[string]string{
		// Prometheus. llama-swap and SGLang /metrics carry none of these names.
		"llamacpp-running": {llamacpp: "[=0]", llamacpp + "/busy": "[=2]"},
		"llamacpp-waiting": {llamacpp: "[=0]", llamacpp + "/busy": "[=2]"},
		"vllm-running":     {vllm: "[qwen2.5-0.5b=0]", vllm + "/busy": "[qwen2.5-0.5b=2]"},
		"vllm-waiting":     {vllm: "[qwen2.5-0.5b=0]", vllm + "/busy": "[qwen2.5-0.5b=2]"},
		"vllm-kv-cache-usage-perc": {
			vllm: "[qwen2.5-0.5b=0]", vllm + "/busy": "[qwen2.5-0.5b=0.0014662756598240456]",
		},

		// llama-swap answers /api/version with "v260", which is not an Ollama version.
		"ollama-version": {ollama: "0.34.4", llamaswap: failed},
		"vllm-version":   {vllm: "0.30.0"},
		// Reads Ollama's body too; only the recipe keeps it off Ollama.
		"llamaswap-version": {llamaswap: "v260", ollama: "0.34.4"},

		// Residency: loaded → cold after keep-alive (Ollama) or ttl (llama-swap).
		"ollama-ps-residency": {
			ollama + "/idle": "[]", ollama + "/loaded": "[qwen2.5:0.5b=loaded]",
			ollama + "/busy": "[qwen2.5:0.5b=loaded]", ollama + "/cold": "[]",
		},
		"llamaswap-running": {
			llamaswap + "/idle": "[]", llamaswap + "/loaded": "[qwen2.5-0.5b=loaded]",
			llamaswap + "/busy": "[qwen2.5-0.5b=loaded]", llamaswap + "/cold": "[]",
		},
		// CPU run: size_vram 0 is a real 0.
		"ollama-ps-size-vram": {
			ollama + "/idle": "[]", ollama + "/loaded": "[qwen2.5:0.5b=0]",
			ollama + "/busy": "[qwen2.5:0.5b=0]", ollama + "/cold": "[]",
		},
		// SGLang serves Ollama's /api/tags with a placeholder size 0.
		"ollama-tags-size": {ollama: "[qwen2.5:0.5b=397821319]", sglang: "[qwen2.5-0.5b=?]"},

		"llamacpp-props-total-slots":   {llamacpp: "[=2]"},
		"llamacpp-props-build-info":    {llamacpp: "b11146-7fe450e19"},
		"llamacpp-slots-is-processing": {llamacpp: "[=0]", llamacpp + "/busy": "[=2]"},

		"openai-models": {
			ollama: "[qwen2.5:0.5b]", llamacpp: "[qwen2.5-0.5b]", llamaswap: "[qwen2.5-0.5b]",
			vllm: "[qwen2.5-0.5b]", sglang: "[qwen2.5-0.5b]", mlx: "[mlx-community/Qwen2.5-0.5B-Instruct-4bit]",
		},
	}

	used := map[string]bool{}
	dirs := captures(t)
	for _, capture := range slices.Sorted(maps.Keys(dirs)) {
		srv := serveCapture(t, dirs[capture])
		for _, p := range Library {
			if p.Feed.Log {
				continue // TestReplayLogProbes
			}
			want, ok := rows[p.Name][capture]
			key := capture
			if !ok {
				key = path.Dir(capture)
				want, ok = rows[p.Name][key]
			}
			if ok {
				used[p.Name+" "+key] = true
			} else {
				want = "unknown"
			}
			t.Run(p.Name+"/"+capture, func(t *testing.T) {
				status, body := get(t, srv.URL+p.Feed.Path)
				if status == http.StatusNotFound {
					if ok {
						t.Errorf("%s answers 404, row expects %s", p.Feed.Path, want)
					}
					return
				}
				if status != http.StatusOK {
					t.Fatalf("%s: status %d", p.Feed.Path, status)
				}
				got := failed
				if s, err := p.Parse(body); err == nil {
					got = s.Show(p.Signal)
				} else if want != failed {
					t.Logf("err = %v", err)
				}
				if got != want {
					t.Errorf("%s = %s, want %s", p.Signal, got, want)
				}
			})
		}
	}

	names := map[string]bool{}
	for _, p := range Library {
		names[p.Name] = true
		if rows[p.Name] == nil && !p.Feed.Log {
			t.Errorf("library probe %s has no replay rows", p.Name)
		}
	}
	for name, byCapture := range rows {
		if !names[name] {
			t.Errorf("rows for %s, which is not in Library", name)
		}
		for key := range byCapture {
			if !used[name+" "+key] {
				t.Errorf("row %s %s matches no capture", name, key)
			}
		}
	}
}

// Edge cases per JSON probe, as short bodies derived from a real capture.
func TestJSONProbeEdgeCases(t *testing.T) {
	for _, tt := range []struct {
		name  string
		probe Probe
		body  string
		want  string // Show()n, or "error"
	}{
		{"version_absent_is_unknown", ollamaVersion, `{}`, "unknown"},
		{"version_not_a_string_is_an_error", vllmVersion, `{"version":0.3}`, "error"},
		{"llamaswap_version_is_not_an_ollama_version", ollamaVersion,
			`{"build_date":"2026-09-26T15:25:50Z","commit":"fcefa7b","version":"v260"}`, "error"},
		{"truncated_body_is_an_error", ollamaVersion, `{"version":"0.34.4"`, "error"},

		{"ps_models_absent_is_unknown", ollamaPSResidency, `{}`, "unknown"},
		{"ps_entry_without_name_is_an_error", ollamaPSResidency, `{"models":[{"model":"qwen2.5:0.5b"}]}`, "error"},
		{"ps_models_not_a_list_is_an_error", ollamaPSResidency, `{"models":{}}`, "error"},
		{"size_vram_absent_is_unknown", ollamaPSVRAM, `{"models":[{"name":"qwen2.5:0.5b","size":721231542}]}`, "[qwen2.5:0.5b=?]"},
		{"size_vram_not_a_number_is_an_error", ollamaPSVRAM, `{"models":[{"name":"qwen2.5:0.5b","size_vram":"0"}]}`, "error"},
		{"tags_size_absent_is_unknown", ollamaTagsSize, `{"models":[{"name":"qwen2.5:0.5b"}]}`, "[qwen2.5:0.5b=?]"},
		// llama.cpp's Ollama-shaped /models has placeholder strings where Ollama has numbers.
		{"llamacpp_models_body_is_not_ollama_tags", ollamaTagsSize,
			`{"models":[{"name":"qwen2.5-0.5b","model":"qwen2.5-0.5b","modified_at":"","size":"","digest":""}]}`, "error"},

		{"total_slots_absent_is_unknown", llamacppPropsCapacity, `{"model_alias":"qwen2.5-0.5b"}`, "unknown"},
		{"total_slots_zero_is_an_error", llamacppPropsCapacity, `{"total_slots":0}`, "error"},
		{"total_slots_not_a_number_is_an_error", llamacppPropsCapacity, `{"total_slots":"2"}`, "error"},
		{"build_info_absent_is_unknown", llamacppPropsVersion, `{"total_slots":2}`, "unknown"},
		{"slot_without_is_processing_makes_running_unknown", llamacppSlotsRunning,
			`[{"id":0,"is_processing":true},{"id":1,"n_ctx":4096}]`, "unknown"},
		{"slots_not_a_list_is_an_error", llamacppSlotsRunning, `{"id":0,"is_processing":true}`, "error"},

		{"running_absent_is_unknown", llamaswapRunning, `{}`, "unknown"},
		{"unseen_process_state_is_unknown_residency", llamaswapRunning,
			`{"running":[{"model":"qwen2.5-0.5b","state":"starting"}]}`, "[qwen2.5-0.5b=unknown]"},

		{"data_absent_is_unknown", openaiModels, `{"object":"list"}`, "unknown"},
		{"empty_data_is_no_models", openaiModels, `{"object":"list","data":[]}`, "[]"},
		{"model_without_id_is_an_error", openaiModels, `{"object":"list","data":[{"object":"model"}]}`, "error"},
		{"garbage_is_an_error", openaiModels, `<html>404 page not found</html>`, "error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := "error"
			if s, err := tt.probe.Parse([]byte(tt.body)); err == nil {
				got = s.Show(tt.probe.Signal)
			}
			if got != tt.want {
				t.Errorf("%s = %s, want %s", tt.probe.Signal, got, tt.want)
			}
		})
	}
}

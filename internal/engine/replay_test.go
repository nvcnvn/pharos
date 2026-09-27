package engine

import (
	"errors"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// serveCapture serves one capture state dir (testdata/<engine>/<version>/<state>)
// the way the engine served it: every path in paths.tsv with its recorded
// status and content type. 404s have no body file; a missing body file for
// another status is an empty body (SGLang's /health). Status 000 is a request
// that got no response (llama.cpp before b8772 under load): the connection is
// dropped, so the client sees an error, as it did. A request for a path the
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
		if r.status == 0 {
			panic(http.ErrAbortHandler) // closes the connection without a response
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

// rowKeys lists the expectation rows that can describe a capture, most specific
// first: "engine/version/state", "engine/version" (every state of that version),
// "engine/*/state" (that state of every version), "engine/*". A behavior every
// version shares is written once; a version only needs rows where it differs.
// A capture without a state ("engine/version", an engine.log) skips the state keys.
func rowKeys(capture string) []string {
	engine, rest, _ := strings.Cut(capture, "/")
	version, state, ok := strings.Cut(rest, "/")
	if !ok {
		return []string{capture, engine + "/*"}
	}
	return []string{capture, engine + "/" + version, engine + "/*/" + state, engine + "/*"}
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

// fetch GETs url; err is set when the capture recorded no response (000).
func fetch(url string) (int, []byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
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
// its path. Rows are keyed as in rowKeys, most specific wins. A capture without
// a row must leave the signal unknown; "error" means Parse must fail. Values are
// Show()n.
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
		// Prometheus. Every capture runs 2 slots: busy = 4 requests, saturated = 8.
		// llama-swap and SGLang /metrics carry none of these names.
		"llamacpp-running": {
			"llamacpp/*": "[=0]", "llamacpp/*/busy": "[=2]", "llamacpp/*/saturated": "[=2]",
			"llamacpp/b7139": "unknown", // /metrics is a JSON-quoted string
		},
		// Before b8772 requests_deferred reads 0 with 2 requests queued: a wrong
		// number, recorded as the engine serves it (spike 2026-09-27-older-versions).
		"llamacpp-waiting": {
			"llamacpp/*": "[=0]", "llamacpp/*/busy": "[=2]", "llamacpp/*/saturated": "[=6]",
			"llamacpp/b6602/busy": "[=0]", "llamacpp/b7493/busy": "[=0]",
			"llamacpp/b7139": "unknown",
		},
		"vllm-running": {"vllm/*": "[qwen2.5-0.5b=0]", "vllm/*/busy": "[qwen2.5-0.5b=2]", "vllm/*/saturated": "[qwen2.5-0.5b=2]"},
		"vllm-waiting": {"vllm/*": "[qwen2.5-0.5b=0]", "vllm/*/busy": "[qwen2.5-0.5b=2]", "vllm/*/saturated": "[qwen2.5-0.5b=6]"},
		"vllm-kv-cache-usage-perc": {
			"vllm/*":      "[qwen2.5-0.5b=0]",
			"vllm/*/busy": "[qwen2.5-0.5b=0.0014662756598240456]", "vllm/*/saturated": "[qwen2.5-0.5b=0.0014662756598240456]",
		},

		// llama-swap answers /api/version too: "v260" is not an Ollama version,
		// but "185" and "219" pass as one. Only the recipe keeps it off llama-swap.
		"ollama-version": {
			ollama: "0.34.4", "ollama/v0.12.4": "0.12.4", "ollama/v0.30.0": "0.30.0", "ollama/v0.33.2": "0.33.2", "ollama/v0.33.3": "0.33.3",
			llamaswap: failed, "llama-swap/v185": "185", "llama-swap/v219": "219",
		},
		"vllm-version": {vllm: "0.30.0", "vllm/v0.10.2": "0.10.2", "vllm/v0.11.1": "0.11.1"},
		// Reads Ollama's body too; only the recipe keeps it off Ollama.
		"llamaswap-version": {
			llamaswap: "v260", "llama-swap/v185": "185", "llama-swap/v219": "219",
			ollama: "0.34.4", "ollama/v0.12.4": "0.12.4", "ollama/v0.30.0": "0.30.0", "ollama/v0.33.2": "0.33.2", "ollama/v0.33.3": "0.33.3",
		},

		// Residency: in memory while serving, gone after keep-alive (Ollama) or ttl (llama-swap).
		"ollama-ps-residency": {
			"ollama/*/idle": "[]", "ollama/*/cold": "[]", "ollama/*/loaded": "[qwen2.5:0.5b=loaded]", "ollama/*/busy": "[qwen2.5:0.5b=loaded]",
			"ollama/*/saturated": "[qwen2.5:0.5b=loaded]", "ollama/*/cancelled": "[qwen2.5:0.5b=loaded]",
		},
		"llamaswap-running": {
			"llama-swap/*/idle": "[]", "llama-swap/*/cold": "[]", "llama-swap/*/loaded": "[qwen2.5-0.5b=loaded]", "llama-swap/*/busy": "[qwen2.5-0.5b=loaded]",
			"llama-swap/*/saturated": "[qwen2.5-0.5b=loaded]", "llama-swap/*/cancelled": "[qwen2.5-0.5b=loaded]",
		},
		// CPU run: size_vram 0 is a real 0.
		"ollama-ps-size-vram": {
			"ollama/*/idle": "[]", "ollama/*/cold": "[]", "ollama/*/loaded": "[qwen2.5:0.5b=0]", "ollama/*/busy": "[qwen2.5:0.5b=0]",
			"ollama/*/saturated": "[qwen2.5:0.5b=0]", "ollama/*/cancelled": "[qwen2.5:0.5b=0]",
		},
		// SGLang (from v0.5.8) serves Ollama's /api/tags with a placeholder size 0;
		// llama.cpp before v0.5.0 serves it with size "" (a string: an error, not 0).
		"ollama-tags-size": {
			"ollama/*":      "[qwen2.5:0.5b=397821319]",
			"sglang/v0.5.8": "[qwen2.5-0.5b=?]", "sglang/v0.5.11": "[qwen2.5-0.5b=?]", sglang: "[qwen2.5-0.5b=?]",
			"llamacpp/b6602": failed, "llamacpp/b7139": failed, "llamacpp/b7493": failed, "llamacpp/b8772": failed,
		},

		"llamacpp-props-total-slots": {"llamacpp/*": "[=2]"},
		"llamacpp-props-build-info": {
			llamacpp: "b11146-7fe450e19", "llamacpp/b6602": "b6602-72b24d96", "llamacpp/b7139": "b7139-923ae3c61",
			"llamacpp/b7493": "b7493-9496bbb80", "llamacpp/b8772": "b8772-bafae2765",
		},
		"llamacpp-slots-is-processing": {"llamacpp/*": "[=0]", "llamacpp/*/busy": "[=2]", "llamacpp/*/saturated": "[=2]"},

		"openai-models": {
			"ollama/*": "[qwen2.5:0.5b]", "llamacpp/*": "[qwen2.5-0.5b]", "llama-swap/*": "[qwen2.5-0.5b]",
			"vllm/*": "[qwen2.5-0.5b]", "sglang/*": "[qwen2.5-0.5b]", mlx: "[mlx-community/Qwen2.5-0.5B-Instruct-4bit]",
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
			var want, key string
			var ok bool
			for _, key = range rowKeys(capture) {
				if want, ok = rows[p.Name][key]; ok {
					break
				}
			}
			if ok {
				used[p.Name+" "+key] = true
			} else {
				want = "unknown"
			}
			t.Run(p.Name+"/"+capture, func(t *testing.T) {
				status, body, err := fetch(srv.URL + p.Feed.Path)
				if err != nil {
					// The engine didn't answer this path (000): there is no body to
					// parse, so rows for other states don't apply. Scrape turns the
					// failed GET into unknown (TestScrapeFailingProbeIsAnErrorAndItsSignalUnknown).
					return
				}
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

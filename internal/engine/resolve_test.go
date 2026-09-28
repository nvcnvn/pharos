package engine

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
)

// fakeEngine serves inline bodies by path; other paths are 404. A body
// "status N" answers with status N. It counts GETs per path.
type fakeEngine struct {
	*httptest.Server
	mu     sync.Mutex
	bodies map[string]string
	hits   map[string]int
}

func newFakeEngine(t *testing.T, bodies map[string]string) *fakeEngine {
	f := &fakeEngine{bodies: bodies, hits: map[string]int{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.hits[r.URL.Path]++
		body, ok := f.bodies[r.URL.Path]
		var status int
		if !ok {
			http.NotFound(w, r)
		} else if _, err := fmt.Sscanf(body, "status %d", &status); err == nil {
			w.WriteHeader(status)
		} else {
			w.Write([]byte(body))
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeEngine) set(path, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bodies[path] = body
}

func (f *fakeEngine) resetHits() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.hits)
}

func names(ps []Probe) string {
	var out []string
	for _, p := range ps {
		out = append(out, p.Name)
	}
	return strings.Join(out, " ")
}

// merged shows every signal the snapshot knows as "probe value" (see Show).
func merged(s Snapshot) map[Signal]string {
	m := map[Signal]string{}
	for sig, from := range s.From {
		m[sig] = from + " " + s.Show(sig)
	}
	return m
}

type resolveWant struct {
	kind    Kind
	active  string            // probe names in plan order
	dropped map[string]string // probe name -> substring of the reason
	signals map[Signal]string // merged(); signals not listed must be unknown
	err     string            // substring of the Resolve error, when the engine didn't answer
}

func checkPlan(t *testing.T, plan Plan, s Snapshot, want resolveWant) {
	t.Helper()
	if want.kind != "" && plan.Kind != want.kind {
		t.Errorf("kind = %s, want %s", plan.Kind, want.kind)
	}
	if got := names(plan.Active); got != want.active {
		t.Errorf("active = %q, want %q", got, want.active)
	}
	for name, reason := range want.dropped {
		if got, ok := plan.Dropped[name]; !ok || !strings.Contains(got, reason) {
			t.Errorf("dropped[%s] = %q (present %v), want it to mention %q", name, got, ok, reason)
		}
	}
	for name := range plan.Dropped {
		if _, ok := want.dropped[name]; !ok {
			t.Errorf("dropped %s (%s), want it active or absent", name, plan.Dropped[name])
		}
	}
	if got := merged(s); !maps.Equal(got, want.signals) {
		t.Errorf("snapshot:\n got %s\nwant %s", showSignals(got), showSignals(want.signals))
	}
}

func showSignals(m map[Signal]string) string {
	var out []string
	for _, sig := range slices.Sorted(maps.Keys(m)) {
		out = append(out, sig.String()+": "+m[sig])
	}
	return "{" + strings.Join(out, "; ") + "}"
}

var (
	testRunA = Prom("run-a", "/metrics", Running, "a")
	testRunB = Prom("run-b", "/metrics", Running, "b")
)

func TestResolve(t *testing.T) {
	newer := func(v string) bool { return !strings.HasPrefix(v, "0.") } // true for "" too
	guarded := testRunA
	guarded.Name, guarded.When = "run-a-guarded", newer
	version := versionProbe("version", "/version")

	for _, tt := range []struct {
		name   string
		kind   Kind
		own    []Probe
		bodies map[string]string
		want   resolveWant
	}{
		{"first_known_value_wins_and_later_probes_are_redundant", OpenAI, []Probe{testRunA, testRunB},
			map[string]string{"/metrics": "a 1\nb 2\n"},
			resolveWant{active: "run-a", dropped: map[string]string{"run-b": "redundant", "openai-models": "404"},
				signals: map[Signal]string{Running: "run-a [=1]"}}},
		{"unknown_value_falls_through_to_the_next_probe", OpenAI, []Probe{testRunA, testRunB},
			map[string]string{"/metrics": "b 2\n"},
			resolveWant{active: "run-b", dropped: map[string]string{"run-a": "no value", "openai-models": "404"},
				signals: map[Signal]string{Running: "run-b [=2]"}}},
		{"parse_error_drops_the_probe_and_the_next_one_answers", OpenAI, []Probe{testRunA, testRunB},
			map[string]string{"/metrics": "a 1.5\nb 2\n"},
			resolveWant{active: "run-b", dropped: map[string]string{"run-a": "parse error", "openai-models": "404"},
				signals: map[Signal]string{Running: "run-b [=2]"}}},
		{"error_status_drops_the_probe", OpenAI, []Probe{testRunA},
			map[string]string{"/metrics": "status 500", "/v1/models": `{"data":[]}`},
			resolveWant{active: "openai-models", dropped: map[string]string{"run-a": "status 500"},
				signals: map[Signal]string{Models: "openai-models []"}}},
		{"own_probe_goes_ahead_of_the_recipe", VLLM, []Probe{testRunA},
			map[string]string{"/metrics": "a 3\nvllm:num_requests_running{model_name=\"q\"} 1\n"},
			resolveWant{active: "run-a", dropped: map[string]string{
				"vllm-running": "redundant: run-a", "vllm-version": "404", "openai-models": "404",
				"vllm-waiting": "no value", "vllm-kv-cache-usage-perc": "no value"},
				signals: map[Signal]string{Running: "run-a [=3]"}}},
		{"guarded_probe_runs_on_a_matching_version", OpenAI, []Probe{version, guarded},
			map[string]string{"/version": `{"version":"1.2.0"}`, "/metrics": "a 1\n"},
			resolveWant{active: "version run-a-guarded", dropped: map[string]string{"openai-models": "404"},
				signals: map[Signal]string{Version: "version 1.2.0", Running: "run-a-guarded [=1]"}}},
		{"guarded_probe_is_dropped_on_another_version", OpenAI, []Probe{version, guarded},
			map[string]string{"/version": `{"version":"0.9.0"}`, "/metrics": "a 1\n"},
			resolveWant{active: "version", dropped: map[string]string{"run-a-guarded": "version guard", "openai-models": "404"},
				signals: map[Signal]string{Version: "version 0.9.0"}}},
		{"guarded_probe_is_dropped_when_the_version_is_unknown", OpenAI, []Probe{version, guarded},
			map[string]string{"/metrics": "a 1\n"},
			resolveWant{active: "", dropped: map[string]string{"version": "404", "run-a-guarded": "version guard", "openai-models": "404"},
				signals: map[Signal]string{}}},

		// Residency lists the models in memory; Models lists every model.
		{"listed_model_is_loaded_and_unlisted_is_cold", LlamaSwap, nil,
			map[string]string{"/v1/models": `{"data":[{"id":"a"},{"id":"b"}]}`,
				"/running": `{"running":[{"model":"a","state":"ready"}]}`},
			resolveWant{active: "openai-models llamaswap-running", dropped: map[string]string{"llamaswap-version": "404"},
				signals: map[Signal]string{Models: "openai-models [a b]", Residency: "llamaswap-running [a=loaded b=cold]"}}},
		{"empty_residency_list_means_every_model_is_cold", LlamaSwap, nil,
			map[string]string{"/v1/models": `{"data":[{"id":"a"},{"id":"b"}]}`, "/running": `{"running":[]}`},
			resolveWant{active: "openai-models llamaswap-running", dropped: map[string]string{"llamaswap-version": "404"},
				signals: map[Signal]string{Models: "openai-models [a b]", Residency: "llamaswap-running [a=cold b=cold]"}}},
		{"listed_model_in_an_unseen_state_stays_unknown_not_cold", LlamaSwap, nil,
			map[string]string{"/v1/models": `{"data":[{"id":"a"},{"id":"b"}]}`,
				"/running": `{"running":[{"model":"a","state":"starting"}]}`},
			resolveWant{active: "openai-models llamaswap-running", dropped: map[string]string{"llamaswap-version": "404"},
				signals: map[Signal]string{Models: "openai-models [a b]", Residency: "llamaswap-running [a=unknown b=cold]"}}},
		{"no_residency_probe_leaves_every_state_unknown", LlamaSwap, nil,
			map[string]string{"/v1/models": `{"data":[{"id":"a"}]}`},
			resolveWant{active: "openai-models", dropped: map[string]string{"llamaswap-version": "404", "llamaswap-running": "404"},
				signals: map[Signal]string{Models: "openai-models [a]"}}},
		// Auto-detect: llama-swap also answers /api/version, so /running is checked first.
		// The version here passes for an Ollama one, which only the order can catch.
		{"llama_swap_is_detected_before_ollama", Auto, nil,
			map[string]string{"/running": `{"running":[]}`, "/api/version": `{"version":"0.1.0"}`},
			resolveWant{kind: LlamaSwap, active: "llamaswap-version llamaswap-running", dropped: map[string]string{"openai-models": "404"},
				signals: map[Signal]string{Version: "llamaswap-version 0.1.0", Residency: "llamaswap-running []"}}},
		{"unrecognized_engine_is_generic_openai", Auto, nil,
			map[string]string{"/v1/models": `{"data":[{"id":"a"}]}`, "/version": `{"version":"1.0"}`},
			resolveWant{kind: OpenAI, active: "openai-models", signals: map[Signal]string{Models: "openai-models [a]"}}},
		{"sizes_all_unknown_is_no_value", Ollama, nil,
			map[string]string{"/api/tags": `{"models":[{"name":"a","size":0}]}`},
			resolveWant{active: "", dropped: map[string]string{"ollama-version": "404", "openai-models": "404",
				"ollama-ps-residency": "404", "ollama-ps-size-vram": "404", "ollama-tags-size": "no value",
				"ollama-log-num-parallel": "no log feed"},
				signals: map[Signal]string{}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeEngine(t, tt.bodies)
			plan, s, err := Resolve(context.Background(), f.Client(), f.URL, tt.kind, tt.own, false)
			if err != nil {
				t.Fatal(err)
			}
			checkPlan(t, plan, s, tt.want)
		})
	}
}

func TestResolveLogProbes(t *testing.T) {
	logRun := mustLogLine("log-run", Running, `running=(?P<value>\d+)`, "")
	for _, tt := range []struct {
		name string
		logs bool
		own  []Probe
		want resolveWant
	}{
		{"log_probe_without_a_feed_is_dropped", false, []Probe{logRun, testRunA},
			resolveWant{active: "run-a", dropped: map[string]string{"log-run": "no log feed", "openai-models": "404"},
				signals: map[Signal]string{Running: "run-a [=1]"}}},
		{"log_probe_with_a_feed_is_active_and_unknown_until_a_line_matches", true, []Probe{logRun},
			resolveWant{active: "log-run", dropped: map[string]string{"openai-models": "404"},
				signals: map[Signal]string{}}},
		{"log_probe_ahead_makes_a_later_probe_for_its_signal_redundant", true, []Probe{logRun, testRunA},
			resolveWant{active: "log-run", dropped: map[string]string{"run-a": "redundant: log-run", "openai-models": "404"},
				signals: map[Signal]string{}}},
		{"log_probe_after_an_answering_probe_is_redundant", true, []Probe{testRunA, logRun},
			resolveWant{active: "run-a", dropped: map[string]string{"log-run": "redundant: run-a", "openai-models": "404"},
				signals: map[Signal]string{Running: "run-a [=1]"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeEngine(t, map[string]string{"/metrics": "a 1\n"})
			plan, s, err := Resolve(context.Background(), f.Client(), f.URL, OpenAI, tt.own, tt.logs)
			if err != nil {
				t.Fatal(err)
			}
			checkPlan(t, plan, s, tt.want)
			s, err = plan.Scrape(context.Background(), f.Client(), f.URL)
			if err != nil {
				t.Errorf("scrape: %v, want a log probe to leave the round alone", err)
			}
			if got := merged(s); !maps.Equal(got, tt.want.signals) {
				t.Errorf("scrape:\n got %s\nwant %s", showSignals(got), showSignals(tt.want.signals))
			}
		})
	}
}

func TestResolveFails(t *testing.T) {
	f := newFakeEngine(t, nil)
	if _, _, err := Resolve(context.Background(), f.Client(), f.URL, "tgi", nil, false); err == nil {
		t.Error("unknown kind: err = nil")
	}
	f.Close()
	for _, k := range []Kind{Auto, OpenAI} {
		if _, _, err := Resolve(context.Background(), f.Client(), f.URL, k, nil, false); err == nil {
			t.Errorf("backend down, kind %s: err = nil", k)
		}
	}
}

func TestScrapeGetsEachPathOnce(t *testing.T) {
	f := newFakeEngine(t, map[string]string{
		"/version":   `{"version":"0.30.0"}`,
		"/v1/models": `{"data":[{"id":"q"}]}`,
		"/metrics": "vllm:num_requests_running{model_name=\"q\"} 1\nvllm:num_requests_waiting{model_name=\"q\"} 0\n" +
			"vllm:kv_cache_usage_perc{model_name=\"q\"} 0.5\n",
	})
	plan, _, err := Resolve(context.Background(), f.Client(), f.URL, VLLM, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, round := range []string{"resolve", "scrape"} {
		if round == "scrape" {
			f.resetHits()
			if _, err := plan.Scrape(context.Background(), f.Client(), f.URL); err != nil {
				t.Fatal(err)
			}
		}
		if f.hits["/metrics"] != 1 || f.hits["/version"] != 1 || f.hits["/v1/models"] != 1 {
			t.Errorf("%s: GETs per path = %v, want 1 each", round, f.hits)
		}
	}
}

func TestScrapeFailingProbeIsAnErrorAndItsSignalUnknown(t *testing.T) {
	f := newFakeEngine(t, map[string]string{
		"/props":   `{"build_info":"b8772","total_slots":2}`, // a build whose Waiting is trusted
		"/metrics": "llamacpp:requests_processing 1\nllamacpp:requests_deferred 0\n",
		"/slots":   `[{"is_processing":true},{"is_processing":false}]`,
	})
	plan, _, err := Resolve(context.Background(), f.Client(), f.URL, LlamaCpp, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	f.set("/metrics", "llamacpp:requests_deferred 0\n") // running metric gone
	s, err := plan.Scrape(context.Background(), f.Client(), f.URL)
	if err == nil || !strings.Contains(err.Error(), "llamacpp-running") {
		t.Errorf("err = %v, want it to name llamacpp-running", err)
	}
	// /slots was dropped as redundant, so it doesn't stand in until the next Resolve.
	want := map[Signal]string{
		Version:  "llamacpp-props-build-info b8772",
		Capacity: "llamacpp-props-total-slots [=2]",
		Waiting:  "llamacpp-waiting [=0]",
	}
	if got := merged(s); !maps.Equal(got, want) {
		t.Errorf("snapshot:\n got %s\nwant %s", showSignals(got), showSignals(want))
	}
	if l := s.Load[""]; l.Running.OK {
		t.Errorf("Running = %v, want unknown", l.Running.V)
	}
}

// llama.cpp's requests_deferred reads 0 with requests queued before b8772
// (captures b6602, b7493; b8772 reads the queue). The build comes from /props
// build_info, "b<build>-<commit>"; anything else is an unknown build.
func TestLlamacppWaitingIsTrustedFromBuild8772(t *testing.T) {
	for _, tt := range []struct {
		version string
		trusted bool
	}{
		{"b8772-bafae2765", true},
		{"b11146-7fe450e19", true}, // release v0.5.0
		{"b8771-0000000", false},
		{"b7493-9496bbb80", false},
		{"b6602-72b24d96", false},
		{"b8772", true}, // no commit suffix
		{"", false},
		{"v0.5.0", false}, // not a build number: unknown, so not trusted
		{"bogus-1", false},
	} {
		if got := llamacppWaiting.When(tt.version); got != tt.trusted {
			t.Errorf("When(%q) = %v, want %v", tt.version, got, tt.trusted)
		}
	}
}

// TestResolveCaptures resolves every capture with Kind Auto. Rows are keyed as
// in rowKeys and layered from the least specific ("engine/*") to the most
// specific ("engine/version/state"): a row's kind, active, dropped and err
// replace the less specific ones when set, and its signals override single
// signals; an empty signal value makes that signal unknown.
func TestResolveCaptures(t *testing.T) {
	const (
		ollama    = "ollama/v0.34.4"
		llamacpp  = "llamacpp/v0.5.0"
		llamaswap = "llama-swap/v260"
		vllm      = "vllm/v0.30.0"
		sglang    = "sglang/v0.5.20"
		mlx       = "mlx-lm/v0.31.3"
	)
	ollamaLoaded := resolveWant{signals: map[Signal]string{
		Residency: "ollama-ps-residency [qwen2.5:0.5b=loaded]", VRAMBytes: "ollama-ps-size-vram [qwen2.5:0.5b=0]",
	}}
	swapLoaded := resolveWant{signals: map[Signal]string{Residency: "llamaswap-running [qwen2.5-0.5b=loaded]"}}
	version := func(v string) resolveWant { return resolveWant{signals: map[Signal]string{Version: v}} }
	waitingGuarded := func(v string) resolveWant {
		return resolveWant{
			active: "llamacpp-props-build-info openai-models llamacpp-props-total-slots llamacpp-running",
			dropped: map[string]string{
				"llamacpp-waiting":             "version guard",
				"llamacpp-slots-is-processing": "redundant: llamacpp-running",
			},
			signals: map[Signal]string{Version: v, Waiting: ""},
		}
	}
	rows := map[string]resolveWant{
		"ollama/*": {kind: Ollama, active: "ollama-version openai-models ollama-ps-residency ollama-ps-size-vram ollama-tags-size",
			dropped: map[string]string{"ollama-log-num-parallel": "no log feed"},
			signals: map[Signal]string{
				Models:    "openai-models [qwen2.5:0.5b]",
				Residency: "ollama-ps-residency [qwen2.5:0.5b=cold]",
				VRAMBytes: "ollama-ps-size-vram [qwen2.5:0.5b=?]", // nothing in memory
				SizeBytes: "ollama-tags-size [qwen2.5:0.5b=397821319]",
			}},
		"ollama/*/loaded": ollamaLoaded, "ollama/*/busy": ollamaLoaded, "ollama/*/saturated": ollamaLoaded, "ollama/*/cancelled": ollamaLoaded,
		ollama:           version("ollama-version 0.34.4"),
		"ollama/v0.33.3": version("ollama-version 0.33.3"),
		"ollama/v0.33.2": version("ollama-version 0.33.2"),
		"ollama/v0.30.0": version("ollama-version 0.30.0"),
		"ollama/v0.12.4": version("ollama-version 0.12.4"),

		"llamacpp/*": {kind: LlamaCpp,
			active:  "llamacpp-props-build-info openai-models llamacpp-props-total-slots llamacpp-running llamacpp-waiting",
			dropped: map[string]string{"llamacpp-slots-is-processing": "redundant: llamacpp-running"},
			signals: map[Signal]string{
				Models:   "openai-models [qwen2.5-0.5b]",
				Capacity: "llamacpp-props-total-slots [=2]",
				Running:  "llamacpp-running [=0]",
				Waiting:  "llamacpp-waiting [=0]",
			}},
		"llamacpp/*/busy":      {signals: map[Signal]string{Running: "llamacpp-running [=2]", Waiting: "llamacpp-waiting [=2]"}},
		"llamacpp/*/saturated": {signals: map[Signal]string{Running: "llamacpp-running [=2]", Waiting: "llamacpp-waiting [=6]"}},
		llamacpp:               version("llamacpp-props-build-info b11146-7fe450e19"),
		"llamacpp/b8772":       version("llamacpp-props-build-info b8772-bafae2765"),
		// Before b8772 requests_deferred reads 0 with 2 requests queued, so the
		// version guard drops llamacpp-waiting and Waiting is unknown. With 8
		// requests these builds answer no path, so auto-detect can't run
		// (spike 2026-09-27-older-versions).
		"llamacpp/b6602":           waitingGuarded("llamacpp-props-build-info b6602-72b24d96"),
		"llamacpp/b7493":           waitingGuarded("llamacpp-props-build-info b7493-9496bbb80"),
		"llamacpp/b6602/saturated": {err: "EOF"},
		"llamacpp/b7139/saturated": {err: "EOF"},
		"llamacpp/b7493/saturated": {err: "EOF"},
		// While it loads its model, llama.cpp answers 503 on every path, its own
		// and others'. Not ready is not "not this engine": detection fails and
		// the scraper tries again next round, instead of settling on the generic
		// kind until the plan's 10-minute re-resolve.
		"llamacpp/*/loading": {err: "status 503"},
		// b7139 serves /metrics as a JSON-quoted string: no Prometheus value, so
		// /slots reads Running and Waiting stays unknown.
		"llamacpp/b7139": {
			active:  "llamacpp-props-build-info openai-models llamacpp-props-total-slots llamacpp-slots-is-processing",
			dropped: map[string]string{"llamacpp-running": "no value", "llamacpp-waiting": "version guard"},
			signals: map[Signal]string{
				Version: "llamacpp-props-build-info b7139-923ae3c61",
				Running: "llamacpp-slots-is-processing [=0]", Waiting: "",
			}},
		"llamacpp/b7139/busy": {signals: map[Signal]string{Running: "llamacpp-slots-is-processing [=2]", Waiting: ""}},

		"llama-swap/*": {kind: LlamaSwap, active: "llamaswap-version openai-models llamaswap-running",
			signals: map[Signal]string{
				Models:    "openai-models [qwen2.5-0.5b]",
				Residency: "llamaswap-running [qwen2.5-0.5b=cold]",
			}},
		"llama-swap/*/loaded": swapLoaded, "llama-swap/*/busy": swapLoaded, "llama-swap/*/saturated": swapLoaded, "llama-swap/*/cancelled": swapLoaded,
		llamaswap:         version("llamaswap-version v260"),
		"llama-swap/v219": version("llamaswap-version 219"),
		"llama-swap/v185": version("llamaswap-version 185"),

		"vllm/*": {kind: VLLM, active: "vllm-version openai-models vllm-running vllm-waiting vllm-kv-cache-usage-perc",
			signals: map[Signal]string{
				Models:  "openai-models [qwen2.5-0.5b]",
				Running: "vllm-running [qwen2.5-0.5b=0]",
				Waiting: "vllm-waiting [qwen2.5-0.5b=0]",
				KVUsage: "vllm-kv-cache-usage-perc [qwen2.5-0.5b=0]",
			}},
		"vllm/*/busy": {signals: map[Signal]string{
			Running: "vllm-running [qwen2.5-0.5b=2]",
			Waiting: "vllm-waiting [qwen2.5-0.5b=2]",
			KVUsage: "vllm-kv-cache-usage-perc [qwen2.5-0.5b=0.0014662756598240456]",
		}},
		"vllm/*/saturated": {signals: map[Signal]string{
			Running: "vllm-running [qwen2.5-0.5b=2]",
			Waiting: "vllm-waiting [qwen2.5-0.5b=6]",
			KVUsage: "vllm-kv-cache-usage-perc [qwen2.5-0.5b=0.0014662756598240456]",
		}},
		vllm:           version("vllm-version 0.30.0"),
		"vllm/v0.11.1": version("vllm-version 0.11.1"),
		"vllm/v0.10.2": version("vllm-version 0.10.2"),

		"sglang/*": {kind: SGLang, active: "sglang-version openai-models sglang-max-running-requests sglang-running sglang-waiting",
			signals: map[Signal]string{
				Models:   "openai-models [qwen2.5-0.5b]",
				Capacity: "sglang-max-running-requests [=2 qwen2.5-0.5b=?]",
				Running:  "sglang-running [=? qwen2.5-0.5b=0]",
				Waiting:  "sglang-waiting [=? qwen2.5-0.5b=0]",
			}},
		"sglang/*/busy":            {signals: map[Signal]string{Running: "sglang-running [=? qwen2.5-0.5b=1]", Waiting: "sglang-waiting [=? qwen2.5-0.5b=2]"}},
		"sglang/*/saturated":       {signals: map[Signal]string{Running: "sglang-running [=? qwen2.5-0.5b=1]", Waiting: "sglang-waiting [=? qwen2.5-0.5b=6]"}},
		"sglang/v0.5.11/cancelled": {signals: map[Signal]string{Waiting: "sglang-waiting [=? qwen2.5-0.5b=4]"}},
		sglang + "/busy":           {signals: map[Signal]string{Running: "sglang-running [=? qwen2.5-0.5b=2]"}},
		sglang:                     version("sglang-version 0.5.20"),
		"sglang/v0.5.11":           version("sglang-version 0.5.11"),
		"sglang/v0.5.8":            version("sglang-version 0.5.8"),
		"sglang/v0.5.5.post3":      version("sglang-version 0.5.5.post3"),

		// No recipe of its own: the generic OpenAI kind.
		mlx: {kind: OpenAI, active: "openai-models", signals: map[Signal]string{Models: "openai-models [mlx-community/Qwen2.5-0.5B-Instruct-4bit]"}},
	}

	dirs := captures(t)
	for _, capture := range slices.Sorted(maps.Keys(dirs)) {
		var want resolveWant
		keys := rowKeys(capture)
		for _, key := range slices.Backward(keys) {
			r, ok := rows[key]
			if !ok {
				continue
			}
			if r.kind != "" {
				want.kind = r.kind
			}
			if r.active != "" {
				want.active = r.active
			}
			if r.dropped != nil {
				want.dropped = r.dropped
			}
			if r.err != "" {
				want.err = r.err
			}
			want.signals = maps.Clone(want.signals)
			if want.signals == nil {
				want.signals = map[Signal]string{}
			}
			maps.Copy(want.signals, r.signals)
		}
		if want.kind == "" {
			t.Errorf("no row for %s", capture)
			continue
		}
		maps.DeleteFunc(want.signals, func(_ Signal, v string) bool { return v == "" })
		t.Run(capture, func(t *testing.T) {
			srv := serveCapture(t, dirs[capture])
			plan, s, err := Resolve(context.Background(), srv.Client(), srv.URL, Auto, nil, false)
			if want.err != "" {
				if err == nil || !strings.Contains(err.Error(), want.err) {
					t.Errorf("err = %v, want it to mention %q", err, want.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			checkPlan(t, plan, s, want)
			// A scrape round of the plan reads the same snapshot.
			s2, err := plan.Scrape(context.Background(), srv.Client(), srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			if !maps.Equal(merged(s2), merged(s)) {
				t.Errorf("scrape:\n got %s\nwant %s", showSignals(merged(s2)), showSignals(merged(s)))
			}
		})
	}
}

// TestResolveWrongKind resolves a kind's recipe against another engine that
// emulates some of its paths. The recipe's probes must drop, not read the
// emulation as values.
func TestResolveWrongKind(t *testing.T) {
	models := map[Signal]string{Models: "openai-models [qwen2.5-0.5b]"}
	for _, tt := range []struct {
		capture string
		kind    Kind
		want    resolveWant
	}{
		// SGLang serves Ollama's /api/tags with size 0.
		{"sglang/v0.5.20/idle", Ollama, resolveWant{active: "openai-models", signals: models, dropped: map[string]string{
			"ollama-version": "404", "ollama-ps-residency": "404", "ollama-ps-size-vram": "404", "ollama-tags-size": "no value",
			"ollama-log-num-parallel": "no log feed"}}},
		// llama-swap answers /api/version with "v260".
		{"llama-swap/v260/loaded", Ollama, resolveWant{active: "openai-models", signals: models, dropped: map[string]string{
			"ollama-version": "parse error", "ollama-ps-residency": "404", "ollama-ps-size-vram": "404", "ollama-tags-size": "404",
			"ollama-log-num-parallel": "no log feed"}}},
		// llama-swap's /metrics is host CPU and memory, not llama.cpp's.
		{"llama-swap/v260/busy", LlamaCpp, resolveWant{active: "openai-models", signals: models, dropped: map[string]string{
			"llamacpp-props-build-info": "404", "llamacpp-props-total-slots": "404", "llamacpp-running": "no value",
			"llamacpp-waiting": "version guard", "llamacpp-slots-is-processing": "404"}}},
	} {
		t.Run(string(tt.kind)+"/"+tt.capture, func(t *testing.T) {
			srv := serveCapture(t, "testdata/"+tt.capture)
			plan, s, err := Resolve(context.Background(), srv.Client(), srv.URL, tt.kind, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			checkPlan(t, plan, s, tt.want)
		})
	}
}

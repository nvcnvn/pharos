package obs

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
	"github.com/nvcnvn/pharos/internal/peer"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/proxy"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
	"github.com/nvcnvn/pharos/internal/usage"
)

// Layer 3: a proxy wired to Obs, in front of a fake vLLM.
func setup(t *testing.T) (*Obs, *httptest.Server) {
	t.Helper()
	f := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}, Speed: 1})
	t.Cleanup(f.Close)
	now := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	var o *Obs
	st := state.New([]state.BackendSpec{{URL: f.URL()}}, state.Options{Now: clock, OnDecision: func(b, s, out string) { o.Background(b, s, out) }})
	px := prefix.New(100)
	sc := sched.New(st, px, sched.Config{Policy: policy.Defaults})
	u := usage.New(usage.Config{Origin: 1, Now: clock})
	node := peer.New(peer.Config{Origin: 1}, st, sc, px, u)
	o = New(st, sc, node, u)
	srv := httptest.NewServer(proxy.New(st, sc, proxy.Options{Usage: u, OnDone: o.Done}))
	t.Cleanup(srv.Close)
	st.ScrapeAll(t.Context())
	return o, srv
}

func chat(t *testing.T, srv *httptest.Server, model string) {
	t.Helper()
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"`+model+`","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func get(t *testing.T, h http.HandlerFunc, url string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, url, nil))
	return rec.Code, rec.Body.String()
}

// A series line in the Prometheus text format: name, optional labels, value.
var seriesLine = regexp.MustCompile(`^[a-z_]+(\{([a-z_]+="([^"\\]|\\.)*",?)*\})? [-+0-9.eEInf]+$`)

func TestMetrics(t *testing.T) {
	o, srv := setup(t)
	chat(t, srv, "m")
	chat(t, srv, "nope")
	_, body := get(t, o.Metrics, "/metrics")
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if !strings.HasPrefix(line, "# ") && !seriesLine.MatchString(line) {
			t.Errorf("not a series line: %q", line)
		}
	}
	for _, want := range []string{
		`pharos_requests_total{key="",model="m",status="200"} 1`,
		`pharos_requests_total{key="",model="nope",status="404"} 1`,
		`pharos_ttft_seconds_count{model="m"} 1`,
		`pharos_ttft_seconds_bucket{model="m",le="+Inf"} 1`,
		`pharos_decisions_total{stage="admission",outcome="ok"} 2`,
		`pharos_decisions_total{stage="route",outcome="unknown_model"} 1`,
		`pharos_decisions_total{stage="upstream",outcome="ok"} 1`,
		`pharos_decisions_total{stage="resolve",outcome="new_plan"} 1`, // the background hook
		`pharos_overhead_seconds_bucket{le="+Inf"} 1`,                  // only m reached an engine
		`pharos_overhead_seconds_count 1`,
		`kind="vllm"`,
		`pharos_target_inflight{`,
		`pharos_target_inflight_cluster{`,
		`pharos_queue_waiting 0`,
		"# TYPE pharos_tokens_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("no %s in\n%s", want, body)
		}
	}
	if !regexp.MustCompile(`pharos_tokens_total\{key="",model="m",type="completion"\} [1-9]`).MatchString(body) {
		t.Errorf("no completion tokens in\n%s", body)
	}
	if strings.Contains(body, "pharos_unmetered_requests_total") {
		t.Error("the stream reported usage (Pharos asked for it), yet it counts as unmetered")
	}
}

func TestUnknownUsageIsUnmeteredNotZero(t *testing.T) {
	o, _ := setup(t)
	o.Done(proxy.Done{Key: "k", Model: "m", Status: 200, Target: "t"})
	_, body := get(t, o.Metrics, "/metrics")
	if !strings.Contains(body, `pharos_unmetered_requests_total{key="k",model="m"} 1`) || strings.Contains(body, "pharos_tokens_total") {
		t.Errorf("got\n%s", body)
	}
}

// Prediction accuracy is split by where the prediction came from; a request
// without a prediction doesn't count.
func TestPrefixPredictionsBySource(t *testing.T) {
	o, _ := setup(t)
	o.Done(proxy.Done{Model: "m", Status: 200, Target: "t", PrefixSource: "peer", Decisions: []proxy.Decision{{Stage: "prefix", Outcome: "wrong_prediction"}}})
	o.Done(proxy.Done{Model: "m", Status: 200, Target: "t", PrefixSource: "local", Decisions: []proxy.Decision{{Stage: "prefix", Outcome: "predicted_hit"}}})
	o.Done(proxy.Done{Model: "m", Status: 200, Target: "t", Decisions: []proxy.Decision{{Stage: "prefix", Outcome: "miss"}}})
	_, body := get(t, o.Metrics, "/metrics")
	for _, want := range []string{
		`pharos_prefix_predictions_total{source="peer",outcome="wrong_prediction"} 1`,
		`pharos_prefix_predictions_total{source="local",outcome="predicted_hit"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("no %s in\n%s", want, body)
		}
	}
	if n := strings.Count(body, "\npharos_prefix_predictions_total{"); n != 2 {
		t.Errorf("%d series; a request without a prediction must not count", n)
	}
}

func TestStatusPage(t *testing.T) {
	o, srv := setup(t)
	chat(t, srv, "m")
	o.Done(proxy.Done{Key: "<script>x</script>", Model: "m", Status: 401})
	code, body := get(t, o.Status, "/status")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{"vllm", "<td>m</td>", "vllm-running", "unknown", "A single instance", "&lt;script&gt;"} {
		if !strings.Contains(body, want) {
			t.Errorf("no %q on the page", want)
		}
	}
	if strings.Contains(body, "<script>x") {
		t.Error("a key name is not escaped")
	}
}

func TestUsageEndpoint(t *testing.T) {
	o, srv := setup(t)
	chat(t, srv, "m")
	code, body := get(t, o.Usage, "/usage")
	var got struct{ Rows []usage.Row }
	if err := json.Unmarshal([]byte(body), &got); code != 200 || err != nil || len(got.Rows) != 1 || got.Rows[0].Day != "2026-09-28" || got.Rows[0].Requests != 1 {
		t.Errorf("json: %d %s", code, body)
	}
	_, body = get(t, o.Usage, "/usage?by=model&format=csv&from=2026-09-28&to=2026-09-28")
	rows, err := csv.NewReader(strings.NewReader(body)).ReadAll()
	if err != nil || len(rows) != 2 || rows[1][0] != "2026-09-28" || rows[1][1] != "m" {
		t.Errorf("csv: %q, %v", rows, err)
	}
	_, body = get(t, o.Usage, "/usage?from=2026-01-01&to=2026-01-02")
	if !strings.Contains(body, `"rows":[]`) {
		t.Errorf("no usage in range: %s", body)
	}
	for _, bad := range []string{"/usage?from=yesterday", "/usage?by=user"} {
		if code, _ := get(t, o.Usage, bad); code != 400 {
			t.Errorf("%s: %d", bad, code)
		}
	}
}

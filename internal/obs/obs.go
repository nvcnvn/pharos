// Package obs serves /metrics, the /status page and /usage, and logs each
// routing decision (ARCHITECTURE §13). Prompt content is never logged.
package obs

import (
	"cmp"
	"embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/peer"
	"github.com/nvcnvn/pharos/internal/proxy"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
	"github.com/nvcnvn/pharos/internal/usage"
)

// recent is how many routing decisions /status shows.
const recent = 50

// buckets are each histogram's upper bounds, in seconds. The overhead
// budget is 2 ms at p99 (§15).
var buckets = map[string][]float64{
	"pharos_ttft_seconds":     {0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	"pharos_overhead_seconds": {0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.05},
}

type Obs struct {
	st    *state.State
	sc    *sched.Sched
	node  *peer.Node
	u     *usage.Counter
	now   func() time.Time
	start time.Time

	mu        sync.Mutex
	counters  map[string]map[string]float64    // metric -> rendered labels -> value
	hists     map[string]map[string]*histogram // metric -> rendered labels -> histogram
	decisions []proxy.Done                     // the last ones, oldest first
}

type histogram struct {
	counts []uint64 // per bucket, not cumulative; the last is +Inf
	sum    float64
	n      uint64
}

func New(st *state.State, sc *sched.Sched, node *peer.Node, u *usage.Counter) *Obs {
	return &Obs{st: st, sc: sc, node: node, u: u, now: st.Now, start: st.Now(),
		counters: map[string]map[string]float64{}, hists: map[string]map[string]*histogram{}}
}

// Done records one finished request. The proxy calls it (Options.OnDone).
func (o *Obs) Done(d proxy.Done) {
	var decisions []string
	for _, x := range d.Decisions {
		decisions = append(decisions, x.Stage+"="+x.Outcome)
	}
	slog.Debug("request", "key", d.Key, "model", d.Model, "path", d.Path, "target", d.Target, "reason", d.Reason,
		"decisions", strings.Join(decisions, " "), "prefix_source", d.PrefixSource, "status", d.Status, "ttft", d.TTFT, "overhead", d.Overhead, "duration", d.Duration)
	o.mu.Lock()
	defer o.mu.Unlock()
	kl := labels("key", d.Key, "model", d.Model)
	o.add("pharos_requests_total", labels("key", d.Key, "model", d.Model, "status", strconv.Itoa(d.Status)), 1)
	for _, x := range d.Decisions {
		o.add("pharos_decisions_total", labels("stage", x.Stage, "outcome", x.Outcome), 1)
		if x.Stage == "prefix" && d.PrefixSource != "" {
			o.add("pharos_prefix_predictions_total", labels("source", d.PrefixSource, "outcome", x.Outcome), 1)
		}
	}
	if d.Overhead > 0 {
		o.observe("pharos_overhead_seconds", "", d.Overhead)
	}
	if o.decisions = append(o.decisions, d); len(o.decisions) > recent {
		o.decisions = o.decisions[1:]
	}
	if d.Target == "" {
		return // never reached an engine
	}
	u := d.Usage
	for _, t := range []struct {
		name string
		v    engine.Opt[int]
	}{{"prompt", u.PromptTokens}, {"cached", u.CachedTokens}, {"completion", u.CompletionTokens}} {
		if t.v.OK {
			o.add("pharos_tokens_total", labels("key", d.Key, "model", d.Model, "type", t.name), float64(t.v.V))
		}
	}
	if !u.PromptTokens.OK || !u.CompletionTokens.OK {
		o.add("pharos_unmetered_requests_total", kl, 1)
	}
	if d.TTFT > 0 {
		o.observe("pharos_ttft_seconds", labels("model", d.Model), d.TTFT)
	}
}

// Background records one branch a background round took, for a backend
// (state.Options.OnDecision). It counts with the request path's decisions.
func (o *Obs) Background(backend, stage, outcome string) {
	slog.Debug("background", "backend", backend, "stage", stage, "outcome", outcome)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.add("pharos_decisions_total", labels("stage", stage, "outcome", outcome), 1)
}

func (o *Obs) observe(name, labels string, v time.Duration) {
	m := o.hists[name]
	if m == nil {
		m = map[string]*histogram{}
		o.hists[name] = m
	}
	h := m[labels]
	if h == nil {
		h = &histogram{counts: make([]uint64, len(buckets[name])+1)}
		m[labels] = h
	}
	s := v.Seconds()
	i, _ := slices.BinarySearch(buckets[name], s)
	h.counts[i]++
	h.sum += s
	h.n++
}

func (o *Obs) add(name, labels string, v float64) {
	m := o.counters[name]
	if m == nil {
		m = map[string]float64{}
		o.counters[name] = m
	}
	m[labels] += v
}

// labels renders name/value pairs as a Prometheus label set.
func labels(kv ...string) string {
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(kv[i] + `="`)
		b.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(kv[i+1]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

var help = map[string]string{
	"pharos_requests_total":                    "counter Requests this instance answered, by key, model and HTTP status (499: the client left first).",
	"pharos_tokens_total":                      "counter Tokens engines reported for this instance's requests, by type: prompt, cached (part of prompt) and completion.",
	"pharos_unmetered_requests_total":          "counter Requests whose reply didn't report prompt or completion tokens: their usage is unknown, not 0.",
	"pharos_ttft_seconds":                      "histogram Time to the first byte from the engine, by model.",
	"pharos_overhead_seconds":                  "histogram Pharos's own time before sending a request upstream (auth, admission, body read, prefix lookup, routing), queue wait excluded. Budget: 2 ms at p99.",
	"pharos_decisions_total":                   "counter Branches requests (admission, usage, route, queue, load, prefix, upstream) and background rounds (resolve, scrape, generation, log_feed) took, by stage and outcome. ARCHITECTURE §13 lists them.",
	"pharos_prefix_predictions_total":          "counter Prefix predictions judged against the engine's cached tokens, by where the prediction came from (local, peer, restored) and outcome.",
	"pharos_backend_up":                        "gauge 1 when the backend answers and isn't ejected, with its engine kind and version.",
	"pharos_backend_log_feed_up":               "gauge 1 while the backend's log feed is connected.",
	"pharos_backend_probes_active":             "gauge Probes in the backend's resolved plan. A change after an engine upgrade is how drift shows.",
	"pharos_target_inflight":                   "gauge Requests this instance has in flight on the target.",
	"pharos_target_inflight_cluster":           "gauge Requests in flight on the target as this instance sees the cluster: its own plus what peers reported within 2 s.",
	"pharos_target_signals_unknown":            "gauge Routing signals (residency, running, waiting, capacity, kv_usage) the target doesn't know now.",
	"pharos_queue_waiting":                     "gauge Requests waiting in this instance's fair queue.",
	"pharos_peer_up":                           "gauge 1 when the peer answered the last delta.",
	"pharos_peer_mismatch":                     "gauge 1 when the peer has a different backend or key set.",
	"pharos_peer_proto_mismatch":               "gauge 1 when the peer refused our deltas for an incompatible wire protocol: the two don't share state.",
	"pharos_peer_dropped_prefix_ops":           "gauge Prefix ops not sent to the peer because it was slow.",
	"pharos_peer_last_heard_timestamp_seconds": "gauge When the peer's last delta arrived. Until the next one, quotas and routing here miss what it served since.",
}

// Metrics serves /metrics in the Prometheus text format.
func (o *Obs) Metrics(w http.ResponseWriter, r *http.Request) {
	now := o.now()
	gauges := map[string]map[string]float64{}
	set := func(name, l string, v float64) {
		if gauges[name] == nil {
			gauges[name] = map[string]float64{}
		}
		gauges[name][l] = v
	}
	inflight, cluster := o.sc.Export().Inflight, o.sc.ClusterInflight()
	for _, b := range o.st.Backends() {
		in := b.Info(now)
		set("pharos_backend_up", labels("backend", b.Spec.URL, "kind", string(in.Kind), "version", version(in.Plan.Version)), bit(in.Up))
		set("pharos_backend_log_feed_up", labels("backend", b.Spec.URL), bit(in.LogFeed))
		set("pharos_backend_probes_active", labels("backend", b.Spec.URL), float64(len(in.Plan.Active)))
		for _, t := range b.Targets() {
			l := labels("target", t.Key, "model", t.Model)
			set("pharos_target_inflight", l, float64(inflight[t.Key]))
			set("pharos_target_inflight_cluster", l, float64(cluster[t.Key]))
			set("pharos_target_signals_unknown", l, float64(unknownSignals(t.View(now))))
		}
	}
	set("pharos_queue_waiting", "", float64(o.sc.Waiting()))
	for _, p := range o.node.Status() {
		if !p.Self {
			l := labels("peer", p.Addr)
			set("pharos_peer_up", l, bit(p.Up))
			set("pharos_peer_mismatch", l, bit(p.Mismatch))
			set("pharos_peer_proto_mismatch", l, bit(p.ProtoMismatch))
			set("pharos_peer_dropped_prefix_ops", l, float64(p.Dropped))
			if !p.Heard.IsZero() {
				set("pharos_peer_last_heard_timestamp_seconds", l, float64(p.Heard.UnixMilli())/1e3)
			}
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, name := range slices.Sorted(maps.Keys(help)) {
		typ, text, _ := strings.Cut(help[name], " ")
		series := o.counters[name]
		if typ == "gauge" {
			series = gauges[name]
		}
		if typ == "histogram" {
			if len(o.hists[name]) == 0 {
				continue
			}
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, text, name, typ)
			for _, l := range slices.Sorted(maps.Keys(o.hists[name])) {
				h := o.hists[name][l]
				var cum uint64
				for i, c := range h.counts {
					cum += c
					le := "+Inf"
					if i < len(buckets[name]) {
						le = strconv.FormatFloat(buckets[name][i], 'g', -1, 64)
					}
					fmt.Fprintf(w, "%s_bucket%s %d\n", name, withLabel(l, "le", le), cum)
				}
				fmt.Fprintf(w, "%s_sum%s %g\n%s_count%s %d\n", name, l, h.sum, name, l, h.n)
			}
			continue
		}
		if len(series) == 0 {
			continue
		}
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, text, name, typ)
		for _, l := range slices.Sorted(maps.Keys(series)) {
			fmt.Fprintf(w, "%s%s %g\n", name, l, series[l])
		}
	}
}

// withLabel adds one label to a rendered label set.
func withLabel(set, name, value string) string {
	l := labels(name, value)
	if set == "" {
		return l
	}
	return set[:len(set)-1] + "," + l[1:]
}

func bit(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func unknownSignals(v state.View) int {
	n := 0
	for _, known := range []bool{v.Residency != engine.Unknown, v.Running.OK, v.Waiting.OK, v.Capacity.OK, v.KVUsage.OK} {
		if !known {
			n++
		}
	}
	return n
}

// Usage serves /usage?from=YYYY-MM-DD&to=YYYY-MM-DD&by=key|model&format=json|csv.
// The default is the last 30 days by key, as JSON.
func (o *Obs) Usage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	now := o.now()
	from, to := now.AddDate(0, 0, -29), now
	for _, p := range []struct {
		name string
		t    *time.Time
	}{{"from", &from}, {"to", &to}} {
		if v := q.Get(p.name); v != "" {
			t, err := time.Parse(time.DateOnly, v)
			if err != nil {
				http.Error(w, p.name+": want YYYY-MM-DD", http.StatusBadRequest)
				return
			}
			*p.t = t.Add(12 * time.Hour) // midday: the same date in any timezone
		}
	}
	by := usage.ByKey
	switch q.Get("by") {
	case "", "key":
	case "model":
		by = usage.ByModel
	default:
		http.Error(w, "by: want key or model", http.StatusBadRequest)
		return
	}
	rows := o.u.History(from, to, by)
	if rows == nil {
		rows = []usage.Row{} // JSON [], not null
	}
	if q.Get("format") == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		writeCSV(w, rows)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"rows": rows})
}

func writeCSV(w io.Writer, rows []usage.Row) {
	c := csv.NewWriter(w)
	c.Write([]string{"day", "name", "requests", "prompt_tokens", "cached_tokens", "completion_tokens", "unmetered_requests"})
	for _, r := range rows {
		c.Write([]string{r.Day, r.Name, fmt.Sprint(r.Requests), fmt.Sprint(r.PromptTok), fmt.Sprint(r.CachedTok), fmt.Sprint(r.CompletionTok), fmt.Sprint(r.Unmetered)})
	}
	c.Flush()
}

//go:embed status.html
var files embed.FS

var page = template.Must(template.New("status.html").Funcs(template.FuncMap{
	"ago": func(now, t time.Time) string {
		if t.IsZero() {
			return "never"
		}
		return now.Sub(t).Round(time.Second).String() + " ago"
	},
	"secs": func(d time.Duration) string { return d.Round(time.Millisecond).String() },
}).ParseFS(files, "status.html"))

// Status serves the /status page.
func (o *Obs) Status(w http.ResponseWriter, r *http.Request) {
	now := o.now()
	type signal struct{ Name, Value, Probe string }
	type target struct {
		Model, Residency                   string
		Running, Waiting, Capacity, KV     string
		Inflight, Cluster                  int
		PrefillSecTok, LoadSec, ServiceSec string
	}
	type backend struct {
		URL, Kind, Version, Own string
		Up, LogFeed             bool
		SlowAt, FastAt          time.Time
		Signals                 []signal
		Dropped                 [][2]string
		Targets                 []target
	}
	inflight, cluster := o.sc.Export().Inflight, o.sc.ClusterInflight()
	var backends []backend
	for _, b := range o.st.Backends() {
		in := b.Info(now)
		bk := backend{URL: b.Spec.URL, Kind: string(in.Kind), Version: version(in.Plan.Version), Up: in.Up, LogFeed: in.LogFeed,
			SlowAt: in.Slow.At, FastAt: in.Fast.At}
		own := map[string]bool{}
		for _, p := range b.Spec.Own {
			own[p.Name] = true
		}
		for _, p := range in.Plan.Active {
			snap := in.Slow
			if p.Signal == engine.Running || p.Signal == engine.Waiting || p.Signal == engine.KVUsage {
				snap = in.Fast
			}
			name := p.Name
			if own[name] {
				name += " (own)"
			}
			bk.Signals = append(bk.Signals, signal{p.Signal.String(), snap.Show(p.Signal), name})
		}
		for _, name := range slices.Sorted(maps.Keys(in.Plan.Dropped)) {
			bk.Dropped = append(bk.Dropped, [2]string{name, in.Plan.Dropped[name]})
		}
		for _, t := range b.Targets() {
			v := t.View(now)
			pf, ld, sv := t.Estimates()
			bk.Targets = append(bk.Targets, target{Model: t.Model, Residency: v.Residency.String(),
				Running: showOpt(v.Running), Waiting: showOpt(v.Waiting), Capacity: showOpt(v.Capacity), KV: showOpt(v.KVUsage),
				Inflight: inflight[t.Key], Cluster: cluster[t.Key], PrefillSecTok: showOpt(pf), LoadSec: showOpt(ld), ServiceSec: showOpt(sv)})
		}
		slices.SortFunc(bk.Targets, func(a, b target) int { return cmp.Compare(a.Model, b.Model) })
		backends = append(backends, bk)
	}

	o.mu.Lock()
	decisions := slices.Clone(o.decisions)
	o.mu.Unlock()
	slices.Reverse(decisions) // newest first

	today := o.u.History(now, now, usage.ByKey)
	month := sumByName(o.u.History(now.AddDate(0, 0, -29), now, usage.ByKey))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := page.Execute(w, map[string]any{
		"Now": now, "Up": now.Sub(o.start).Round(time.Second), "Backends": backends, "Waiting": o.sc.Waiting(),
		"Decisions": decisions, "Peers": o.node.Status(), "Today": today, "Month": month,
	})
	if err != nil {
		slog.Warn("status page", "err", err)
	}
}

func sumByName(rows []usage.Row) []usage.Row {
	sums := map[string]*usage.Row{}
	for _, r := range rows {
		s := sums[r.Name]
		if s == nil {
			s = &usage.Row{Name: r.Name}
			sums[r.Name] = s
		}
		s.Requests += r.Requests
		s.PromptTok += r.PromptTok
		s.CachedTok += r.CachedTok
		s.CompletionTok += r.CompletionTok
		s.Unmetered += r.Unmetered
	}
	var out []usage.Row
	for _, name := range slices.Sorted(maps.Keys(sums)) {
		out = append(out, *sums[name])
	}
	return out
}

func showOpt[T int | float64](o engine.Opt[T]) string {
	if !o.OK {
		return "unknown"
	}
	return strconv.FormatFloat(float64(o.V), 'g', 4, 64)
}

func version(v engine.Opt[string]) string {
	if !v.OK {
		return "unknown"
	}
	return v.V
}

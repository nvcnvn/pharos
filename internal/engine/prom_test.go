package engine

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestPromSamples(t *testing.T) {
	nan := math.NaN()
	tests := []struct {
		name   string
		body   string
		metric string
		want   []promSample
	}{
		{
			name:   "unlabeled_gauge_between_help_and_type_comments",
			body:   "# HELP llamacpp:requests_processing Number of requests processing.\n# TYPE llamacpp:requests_processing gauge\nllamacpp:requests_processing 2\n",
			metric: "llamacpp:requests_processing",
			want:   []promSample{{value: 2}},
		},
		{
			name:   "absent_metric_yields_no_samples",
			body:   "# TYPE llamacpp:requests_deferred gauge\nllamacpp:requests_deferred 2\n",
			metric: "llamacpp:requests_processing",
			want:   nil,
		},
		{
			name:   "longer_name_sharing_the_prefix_is_another_metric",
			body:   "vllm:num_requests_waiting_by_reason{reason=\"capacity\"} 2.0\nvllm:num_requests_waiting{model_name=\"q\"} 1.0\n",
			metric: "vllm:num_requests_waiting",
			want:   []promSample{{labels: map[string]string{"model_name": "q"}, value: 1}},
		},
		{
			name:   "labels_and_every_series_of_the_metric",
			body:   "vllm:num_requests_running{engine=\"0\",model_name=\"a\"} 2.0\nvllm:num_requests_running{engine=\"0\",model_name=\"b\"} 0.0\n",
			metric: "vllm:num_requests_running",
			want: []promSample{
				{labels: map[string]string{"engine": "0", "model_name": "a"}, value: 2},
				{labels: map[string]string{"engine": "0", "model_name": "b"}, value: 0},
			},
		},
		{
			name:   "label_values_with_escapes_commas_spaces_and_braces",
			body:   `m{a="x \"y\", z",b="back\\slash",c="line\nbreak",d="}{"} 1` + "\n",
			metric: "m",
			want:   []promSample{{labels: map[string]string{"a": `x "y", z`, "b": `back\slash`, "c": "line\nbreak", "d": "}{"}, value: 1}},
		},
		{
			name:   "empty_braces_trailing_comma_and_spaces_between_labels",
			body:   "m{} 1\nm{ a = \"1\" , } 2\n",
			metric: "m",
			want:   []promSample{{labels: nil, value: 1}, {labels: map[string]string{"a": "1"}, value: 2}},
		},
		{
			name:   "float_formats",
			body:   "m{f=\"a\"} 0.0014662756598240456\nm{f=\"b\"} 1e-3\nm{f=\"c\"} +Inf\nm{f=\"d\"} -Inf\nm{f=\"e\"} NaN\nm{f=\"f\"} 16747855872\n",
			metric: "m",
			want: []promSample{
				{labels: map[string]string{"f": "a"}, value: 0.0014662756598240456},
				{labels: map[string]string{"f": "b"}, value: 0.001},
				{labels: map[string]string{"f": "c"}, value: math.Inf(1)},
				{labels: map[string]string{"f": "d"}, value: math.Inf(-1)},
				{labels: map[string]string{"f": "e"}, value: nan},
				{labels: map[string]string{"f": "f"}, value: 16747855872},
			},
		},
		{
			name:   "optional_timestamp_is_ignored",
			body:   "m 3 1727400000000\n",
			metric: "m",
			want:   []promSample{{value: 3}},
		},
		{
			name:   "crlf_line_endings_and_blank_lines",
			body:   "\r\n# TYPE m gauge\r\nm 4\r\n\r\n",
			metric: "m",
			want:   []promSample{{value: 4}},
		},
		{
			name:   "no_trailing_newline",
			body:   "m 5",
			metric: "m",
			want:   []promSample{{value: 5}},
		},
		{
			name:   "garbage_in_another_metric_does_not_hide_ours",
			body:   "other{a=\"unterminated 1\nm 6\n",
			metric: "m",
			want:   []promSample{{value: 6}},
		},
		{
			name:   "non_prometheus_body_has_no_samples",
			body:   "404 page not found\n",
			metric: "m",
			want:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := promSamples([]byte(tt.body), tt.metric)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if !samplesEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestPromSamplesUnparseableLineOfTheMetricIsAnError(t *testing.T) {
	for _, body := range []string{
		"m\n",                           // no value
		"m abc\n",                       // value not a float
		"m 1 2 3\n",                     // too many fields
		"m 1 soon\n",                    // timestamp not an integer
		"m{a=\"1\" 2\n",                 // unterminated label set
		"m{a=\"1} 2\n",                  // unterminated label value
		"m{a=1} 2\n",                    // unquoted label value
		"m{=\"1\"} 2\n",                 // empty label name
		"m{a=\"1\"b=\"2\"} 3\n",         // missing comma
		"m{a=\"bad\\q\"} 1\n",           // unknown escape
		"# TYPE m gauge\nm 1\nm oops\n", // good line then a bad one
	} {
		if got, err := promSamples([]byte(body), "m"); err == nil {
			t.Errorf("%q: got %+v, want error", body, got)
		} else if !strings.Contains(err.Error(), "line") {
			t.Errorf("%q: error %q does not name the line", body, err)
		}
	}
}

// samplesEqual compares samples, treating NaN as equal to NaN.
func samplesEqual(a, b []promSample) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i].labels) != len(b[i].labels) || (len(a[i].labels) > 0 && !reflect.DeepEqual(a[i].labels, b[i].labels)) {
			return false
		}
		if a[i].value != b[i].value && !(math.IsNaN(a[i].value) && math.IsNaN(b[i].value)) {
			return false
		}
	}
	return true
}

func TestProm(t *testing.T) {
	type cell struct {
		key string
		v   float64
	}
	tests := []struct {
		name  string
		probe Probe
		body  string
		want  []cell // known values; every other key is unknown
	}{
		{
			name:  "missing_metric_is_unknown_not_zero",
			probe: Prom("p", "/metrics", Running, "llamacpp:requests_processing"),
			body:  "llamacpp:requests_deferred 2\n",
		},
		{
			name:  "unlabeled_metric_is_the_whole_backend",
			probe: Prom("p", "/metrics", Running, "llamacpp:requests_processing"),
			body:  "llamacpp:requests_processing 2\nllamacpp:requests_deferred 3\n",
			want:  []cell{{"", 2}},
		},
		{
			name:  "zero_is_a_known_zero",
			probe: Prom("p", "/metrics", Waiting, "llamacpp:requests_deferred"),
			body:  "llamacpp:requests_deferred 0\n",
			want:  []cell{{"", 0}},
		},
		{
			name:  "per_model_label_splits_by_model",
			probe: Prom("p", "/metrics", Waiting, "vllm:num_requests_waiting", PerModel("model_name")),
			body:  "vllm:num_requests_waiting{engine=\"0\",model_name=\"a\"} 2.0\nvllm:num_requests_waiting{engine=\"0\",model_name=\"b\"} 0.0\n",
			want:  []cell{{"a", 2}, {"b", 0}},
		},
		{
			name:  "series_without_the_model_label_names_no_target",
			probe: Prom("p", "/metrics", Running, "vllm:num_requests_running", PerModel("model_name")),
			body:  "vllm:num_requests_running{engine=\"0\"} 2.0\n",
		},
		{
			name:  "capacity_is_a_count",
			probe: Prom("p", "/metrics", Capacity, "m"),
			body:  "m 4\n",
			want:  []cell{{"", 4}},
		},
		{
			name:  "kv_usage_fraction_passes_through",
			probe: Prom("p", "/metrics", KVUsage, "vllm:kv_cache_usage_perc", PerModel("model_name")),
			body:  "vllm:kv_cache_usage_perc{engine=\"0\",model_name=\"q\"} 0.0014662756598240456\n",
			want:  []cell{{"q", 0.0014662756598240456}},
		},
		{
			name:  "scale_turns_a_percentage_into_a_fraction",
			probe: Prom("p", "/metrics", KVUsage, "m", Scale(0.01)),
			body:  "m 45\n",
			want:  []cell{{"", 0.45}},
		},
		{
			name:  "nan_is_unknown",
			probe: Prom("p", "/metrics", KVUsage, "m"),
			body:  "m NaN\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := tt.probe.Parse([]byte(tt.body))
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if len(s.Load) != len(tt.want) {
				t.Errorf("Load = %+v, want %d known keys", s.Load, len(tt.want))
			}
			for _, c := range tt.want {
				got, ok := loadSignal(s, tt.probe.Signal, c.key)
				if !ok || math.Abs(got-c.v) > 1e-12 {
					t.Errorf("%s[%q] = %v (known %v), want %v", tt.probe.Signal, c.key, got, ok, c.v)
				}
			}
			for key, l := range s.Load {
				if others := otherSignalsKnown(l, tt.probe.Signal); others != "" {
					t.Errorf("Load[%q] sets %s, probe reads only %s", key, others, tt.probe.Signal)
				}
			}
		})
	}
}

func TestPromUnusableValueIsAnError(t *testing.T) {
	for _, tt := range []struct {
		name  string
		probe Probe
		body  string
	}{
		{"unparseable_line", Prom("p", "/metrics", Running, "m"), "m two\n"},
		{"fractional_count", Prom("p", "/metrics", Running, "m"), "m 1.5\n"},
		{"negative_count", Prom("p", "/metrics", Waiting, "m"), "m -1\n"},
		{"infinite_count", Prom("p", "/metrics", Capacity, "m"), "m +Inf\n"},
		{"percentage_without_scale", Prom("p", "/metrics", KVUsage, "m"), "m 45\n"},
		{"two_series_for_one_model", Prom("p", "/metrics", Running, "m", PerModel("model_name")),
			"m{engine=\"0\",model_name=\"q\"} 1\nm{engine=\"1\",model_name=\"q\"} 1\n"},
		{"two_series_without_per_model", Prom("p", "/metrics", Running, "m"),
			"m{engine=\"0\"} 1\nm{engine=\"1\"} 1\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if s, err := tt.probe.Parse([]byte(tt.body)); err == nil {
				t.Errorf("got %+v, want error", s.Load)
			}
		})
	}
}

func otherSignalsKnown(l Load, sig Signal) string {
	var known []string
	for _, other := range []Signal{Running, Waiting, Capacity, KVUsage} {
		if _, ok := loadSignal(Snapshot{Load: map[string]Load{"": l}}, other, ""); ok && other != sig {
			known = append(known, other.String())
		}
	}
	return strings.Join(known, ",")
}

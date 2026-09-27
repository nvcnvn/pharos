package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogLine(t *testing.T) {
	for _, tt := range []struct {
		name    string
		sig     Signal
		pattern string
		value   string // fixed value, "" = the value group
		line    string
		want    string // Show()n, or "error"
	}{
		{"value_group_reads_the_backend_count", Capacity, `NUM_PARALLEL:(?P<value>\d+)`, "",
			`msg="server config" env="map[OLLAMA_NUM_PARALLEL:2 OLLAMA_ORIGINS:[]]"`, "[=2]"},
		{"model_group_keys_the_value_per_model", Running, `(?P<model>\S+) running: (?P<value>\d+)`, "",
			"qwen2.5 running: 3", "[qwen2.5=3]"},
		{"non_matching_line_is_unknown", Capacity, `NUM_PARALLEL:(?P<value>\d+)`, "",
			`msg="inference compute" id=cpu library=cpu`, "unknown"},
		{"empty_value_group_is_unknown", Capacity, `NUM_PARALLEL:(?P<value>\d*) `, "",
			"OLLAMA_NUM_PARALLEL: OLLAMA_ORIGINS", "unknown"},
		{"fractional_count_is_an_error", Running, `running: (?P<value>\S+)`, "", "running: 1.5", "error"},
		{"negative_count_is_an_error", Waiting, `waiting: (?P<value>\S+)`, "", "waiting: -1", "error"},
		{"non_numeric_count_is_an_error", Running, `running: (?P<value>\S+)`, "", "running: two", "error"},
		{"kv_usage_outside_0_1_is_an_error", KVUsage, `kv: (?P<value>\S+)`, "", "kv: 42", "error"},
		{"kv_usage_reads_a_fraction", KVUsage, `kv: (?P<value>\S+)`, "", "kv: 0.25", "[=0.25]"},
		{"fixed_residency_for_the_matched_model", Residency, `<(?P<model>[^>]+)> Unloading model`, "cold",
			"[INFO] <qwen2.5-0.5b> Unloading model, TTL of 15s reached", "[qwen2.5-0.5b=cold]"},
		{"version_group_reads_a_string", Version, `version (?P<value>\S+)`, "", "vLLM version 0.30.0", "0.30.0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := mustLogLine(tt.name, tt.sig, tt.pattern, tt.value)
			if !p.Feed.Log {
				t.Fatal("LogLine probe does not read the log feed")
			}
			got := "error"
			if s, err := p.Parse([]byte(tt.line)); err == nil {
				got = s.Show(tt.sig)
			}
			if got != tt.want {
				t.Errorf("%s = %s, want %s", tt.sig, got, tt.want)
			}
		})
	}
}

func TestLogLineRejectsBadSpecs(t *testing.T) {
	for _, tt := range []struct {
		name, pattern, value string
		sig                  Signal
	}{
		{"pattern_does_not_compile", `(?P<value>\d+`, "", Running},
		{"no_value_group_and_no_fixed_value", `running`, "", Running},
		{"value_group_and_fixed_value", `running (?P<value>\d+)`, "3", Running},
		{"residency_without_model_group", `Unloading model`, "cold", Residency},
		{"residency_value_not_a_state", `<(?P<model>\S+)> up`, "warm", Residency},
		{"fixed_count_not_a_count", `busy`, "many", Running},
		{"models_signal_is_not_read_from_logs", `(?P<model>\S+) listed`, "x", Models},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := LogLine(tt.name, tt.sig, tt.pattern, tt.value); err == nil {
				t.Error("want error")
			}
		})
	}
}

func TestFollow(t *testing.T) {
	capacity := mustLogLine("cap", Capacity, `parallel=(?P<value>\d+)`, "")
	cold := mustLogLine("cold", Residency, `unload (?P<model>\S+)`, "cold")
	plan := Plan{Active: []Probe{openaiModels, capacity, cold}}
	logs := "start\nparallel=2\nnoise\nunload m1\nparallel=4 unload m2\n"

	var got []string
	err := plan.Follow(context.Background(), strings.NewReader(logs), func(s Snapshot) {
		var parts []string
		for _, sig := range []Signal{Models, Capacity, Residency} {
			if s.From[sig] != "" {
				parts = append(parts, s.From[sig]+" "+s.Show(sig))
			}
		}
		got = append(got, strings.Join(parts, ", "))
	})
	if err != nil {
		t.Fatal(err)
	}
	// One Snapshot per line that set a value, holding only that line's signals.
	// The HTTP probe never sees a line.
	want := []string{"cap [=2]", "cold [m1=cold]", "cap [=4], cold [m2=cold]"}
	if strings.Join(got, "; ") != strings.Join(want, "; ") {
		t.Errorf("emitted\n %q\nwant\n %q", got, want)
	}
}

func TestFollowUnparseableValueStopsWithTheError(t *testing.T) {
	plan := Plan{Active: []Probe{mustLogLine("run", Running, `running: (?P<value>\S+)`, "")}}
	n := 0
	err := plan.Follow(context.Background(), strings.NewReader("running: 1\nrunning: x\nrunning: 3\n"), func(Snapshot) { n++ })
	if err == nil || !strings.Contains(err.Error(), "run") {
		t.Errorf("err = %v, want it to name the probe", err)
	}
	if n != 1 {
		t.Errorf("emitted %d snapshots, want 1 (none after the bad line)", n)
	}
}

func TestFollowWithoutLogProbesDoesNotRead(t *testing.T) {
	plan := Plan{Active: []Probe{openaiModels}}
	if err := plan.Follow(context.Background(), errReader{}, func(Snapshot) { t.Error("emitted") }); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestFollowStopsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	plan := Plan{Active: []Probe{mustLogLine("run", Running, `running: (?P<value>\d+)`, "")}}
	err := plan.Follow(ctx, strings.NewReader("running: 1\n"), func(Snapshot) { t.Error("emitted") })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read") }

// TestReplayLogProbes runs every library log probe over every captured
// engine.log. Rows are keyed "engine/version"; a log without a row must
// leave the signal unknown. want is the last value the log set.
func TestReplayLogProbes(t *testing.T) {
	rows := map[string]map[string]string{
		"ollama-log-num-parallel": {"ollama/v0.34.4": "[=2]"},
	}
	logs, err := filepath.Glob(filepath.Join("testdata", "*", "*", "engine.log"))
	if err != nil || len(logs) == 0 {
		t.Fatalf("no engine.log captures: %v", err)
	}
	for _, p := range Library {
		if !p.Feed.Log {
			continue
		}
		if rows[p.Name] == nil {
			t.Errorf("library log probe %s has no replay rows", p.Name)
		}
		for _, file := range logs {
			capture := filepath.ToSlash(filepath.Dir(strings.TrimPrefix(file, "testdata"+string(filepath.Separator))))
			t.Run(p.Name+"/"+capture, func(t *testing.T) {
				f, err := os.Open(file)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				got := "unknown"
				err = Plan{Active: []Probe{p}}.Follow(context.Background(), f, func(s Snapshot) { got = s.Show(p.Signal) })
				if err != nil {
					t.Fatal(err)
				}
				want, ok := rows[p.Name][capture]
				if !ok {
					want = "unknown"
				}
				if got != want {
					t.Errorf("%s = %s, want %s", p.Signal, got, want)
				}
			})
		}
	}
}

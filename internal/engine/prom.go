package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

type promSample struct {
	labels map[string]string
	value  float64
}

// promSamples returns every sample of metric in a Prometheus text body (format
// 0.0.4 and 1.0.0 lines look the same for plain gauges and counters). Lines of
// other metrics are skipped unparsed, so syntax we don't know elsewhere in the
// body can't hide the metric we read. A line of metric that doesn't parse is
// an error.
func promSamples(in []byte, metric string) ([]promSample, error) {
	var out []promSample
	n := 0
	for line := range strings.Lines(string(in)) {
		n++
		line = strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(line, metric)
		if !ok {
			continue
		}
		if rest != "" && rest[0] != '{' && rest[0] != ' ' && rest[0] != '\t' {
			continue // a longer name that shares the prefix
		}
		s, err := parsePromSample(rest)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		out = append(out, s)
	}
	return out, nil
}

// parsePromSample parses what follows the metric name: an optional label set,
// the value and an optional timestamp.
func parsePromSample(rest string) (promSample, error) {
	var s promSample
	if strings.HasPrefix(rest, "{") {
		labels, after, err := parsePromLabels(rest[1:])
		if err != nil {
			return s, err
		}
		s.labels, rest = labels, after
	}
	f := strings.Fields(rest)
	if len(f) < 1 || len(f) > 2 {
		return s, fmt.Errorf("want value and optional timestamp, got %q", rest)
	}
	v, err := strconv.ParseFloat(f[0], 64) // also NaN, +Inf, -Inf
	if err != nil {
		return s, fmt.Errorf("value: %w", err)
	}
	if len(f) == 2 {
		if _, err := strconv.ParseInt(f[1], 10, 64); err != nil {
			return s, fmt.Errorf("timestamp: %w", err)
		}
	}
	s.value = v
	return s, nil
}

// parsePromLabels parses `a="x",b="y"}` and returns the labels and the text after '}'.
func parsePromLabels(s string) (map[string]string, string, error) {
	var labels map[string]string
	for {
		s = strings.TrimLeft(s, " \t")
		if strings.HasPrefix(s, "}") {
			return labels, s[1:], nil
		}
		name, after, ok := strings.Cut(s, "=")
		name = strings.TrimSpace(name)
		if !ok || name == "" || strings.ContainsAny(name, `"{},`) {
			return nil, "", fmt.Errorf("bad label near %q", s)
		}
		s = strings.TrimLeft(after, " \t")
		if !strings.HasPrefix(s, `"`) {
			return nil, "", fmt.Errorf("label %s: value not quoted", name)
		}
		val, after, err := parsePromQuoted(s[1:])
		if err != nil {
			return nil, "", fmt.Errorf("label %s: %w", name, err)
		}
		if labels == nil {
			labels = map[string]string{}
		}
		labels[name] = val
		s = strings.TrimLeft(after, " \t")
		switch {
		case strings.HasPrefix(s, ","):
			s = s[1:]
		case strings.HasPrefix(s, "}"):
		default:
			return nil, "", fmt.Errorf("label %s: want ',' or '}' after value", name)
		}
	}
}

// parsePromQuoted reads a label value up to its closing quote. Escapes: \\ \" \n.
func parsePromQuoted(s string) (string, string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			return b.String(), s[i+1:], nil
		case '\\':
			i++
			if i == len(s) {
				return "", "", fmt.Errorf("unterminated value")
			}
			switch s[i] {
			case '\\', '"':
				b.WriteByte(s[i])
			case 'n':
				b.WriteByte('\n')
			default:
				return "", "", fmt.Errorf("unknown escape \\%c", s[i])
			}
		default:
			b.WriteByte(c)
		}
	}
	return "", "", fmt.Errorf("unterminated value")
}

type promOpts struct {
	label string
	scale float64
}

// PromOpt configures a Prom probe.
type PromOpt func(*promOpts)

// PerModel splits the metric per model by the value of label.
func PerModel(label string) PromOpt { return func(o *promOpts) { o.label = label } }

// Scale multiplies the metric value, e.g. Scale(0.01) for a percentage gauge.
func Scale(f float64) PromOpt { return func(o *promOpts) { o.scale = f } }

// Prom reads one metric from a Prometheus-text endpoint into a Load signal
// (Running, Waiting, Capacity or KVUsage). A missing metric, a series without
// the PerModel label and NaN are unknown. A value that can't be that signal
// (a fractional or negative count, KV usage outside 0..1) is an error.
func Prom(name, path string, sig Signal, metric string, opts ...PromOpt) Probe {
	switch sig {
	case Running, Waiting, Capacity, KVUsage:
	default:
		panic(fmt.Sprintf("engine.Prom %s: signal %s is not a Load signal", name, sig))
	}
	o := promOpts{scale: 1}
	for _, opt := range opts {
		opt(&o)
	}
	parse := func(in []byte) (Snapshot, error) {
		samples, err := promSamples(in, metric)
		if err != nil {
			return Snapshot{}, fmt.Errorf("%s: %w", metric, err)
		}
		var s Snapshot
		seen := map[string]bool{}
		for _, smp := range samples {
			key := ""
			if o.label != "" {
				if key = smp.labels[o.label]; key == "" {
					continue
				}
			}
			// ponytail: one series per model; sum counts (and average KV) across
			// labels such as vLLM's engine="N" when a data-parallel capture shows them.
			if seen[key] {
				return Snapshot{}, fmt.Errorf("%s: more than one series for model %q", metric, key)
			}
			seen[key] = true
			v := smp.value * o.scale
			if math.IsNaN(v) {
				continue
			}
			l, err := promLoad(sig, v)
			if err != nil {
				return Snapshot{}, fmt.Errorf("%s{%s=%q}: %w", metric, o.label, key, err)
			}
			if s.Load == nil {
				s.Load = map[string]Load{}
			}
			s.Load[key] = l
		}
		return s, nil
	}
	return Probe{Name: name, Signal: sig, Feed: Feed{Path: path}, Parse: parse}
}

func promLoad(sig Signal, v float64) (Load, error) {
	var l Load
	if sig == KVUsage {
		if v < 0 || v > 1 {
			return l, fmt.Errorf("KV usage %v outside 0..1 (percentage without Scale?)", v)
		}
		l.KVUsage = Opt[float64]{v, true}
		return l, nil
	}
	if v < 0 || v != math.Trunc(v) || math.IsInf(v, 0) || v > math.MaxInt32 {
		return l, fmt.Errorf("%v is not a count", v)
	}
	n := Opt[int]{int(v), true}
	switch sig {
	case Running:
		l.Running = n
	case Waiting:
		l.Waiting = n
	case Capacity:
		l.Capacity = n
	}
	return l, nil
}

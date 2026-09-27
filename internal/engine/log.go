package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
)

// LogLine reads one signal from the backend's log, one line at a time.
// pattern is a regular expression. Its named group "value" holds the value,
// unless value is set: then a matching line reports that fixed value. A named
// group "model" keys the value per model; without it the value is for the
// whole backend. A line that doesn't match, or matches with an empty value, is
// unknown. A value that can't be the signal is an error. Models, VRAMBytes and
// SizeBytes are not read from logs.
func LogLine(name string, sig Signal, pattern, value string) (Probe, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return Probe{}, fmt.Errorf("log probe %s: %w", name, err)
	}
	vi, mi := re.SubexpIndex("value"), re.SubexpIndex("model")
	switch {
	case (vi < 0) == (value == ""):
		return Probe{}, fmt.Errorf("log probe %s: want a (?P<value>...) group or a fixed value, not both", name)
	case sig == Residency && mi < 0:
		return Probe{}, fmt.Errorf("log probe %s: residency needs a (?P<model>...) group", name)
	case sig == Models || sig == VRAMBytes || sig == SizeBytes:
		return Probe{}, fmt.Errorf("log probe %s: signal %s is not read from logs", name, sig)
	}
	if value != "" {
		if _, err := logValue(sig, "", value); err != nil {
			return Probe{}, fmt.Errorf("log probe %s: %w", name, err)
		}
	}
	parse := func(line []byte) (Snapshot, error) {
		m := re.FindSubmatch(line)
		if m == nil {
			return Snapshot{}, nil
		}
		v := value
		if vi >= 0 {
			v = string(m[vi])
		}
		model := ""
		if mi >= 0 {
			model = string(m[mi])
		}
		if v == "" || mi >= 0 && model == "" {
			return Snapshot{}, nil
		}
		s, err := logValue(sig, model, v)
		if err != nil {
			return Snapshot{}, fmt.Errorf("log probe %s: %w", name, err)
		}
		return s, nil
	}
	return Probe{Name: name, Signal: sig, Feed: Feed{Log: true}, Parse: parse}, nil
}

func logValue(sig Signal, model, v string) (Snapshot, error) {
	switch sig {
	case Version:
		return Snapshot{Version: Opt[string]{v, true}}, nil
	case Residency:
		for _, st := range []ResidencyState{Loaded, Loading, Cold} {
			if v == st.String() {
				return Snapshot{Models: map[string]ModelInfo{model: {State: st}}}, nil
			}
		}
		return Snapshot{}, fmt.Errorf("residency %q is not loaded, loading or cold", v)
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%s %q is not a number", sig, v)
	}
	l, err := promLoad(sig, f)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Load: map[string]Load{model: l}}, nil
}

// maxLogLine bounds one log line. Ollama's "server config" line is ~2 KB.
const maxLogLine = 1 << 20

// Follow runs the plan's log probes on each line of logs until it ends or ctx
// is done. For each line that sets a value it emits a Snapshot holding only
// the signals that line set, with From filled; a residency value covers only
// the model the line names. A log-derived value holds until
// a newer line replaces it; the caller makes it unknown when the stream ends.
// A matching line with an unusable value stops Follow with the error.
// The caller closes logs to unblock a pending read on cancel.
func (p Plan) Follow(ctx context.Context, logs io.Reader, emit func(Snapshot)) error {
	var probes []Probe
	for _, pr := range p.Active {
		if pr.Feed.Log {
			probes = append(probes, pr)
		}
	}
	if len(probes) == 0 {
		return nil
	}
	sc := bufio.NewScanner(logs)
	sc.Buffer(make([]byte, 0, 64<<10), maxLogLine) // ponytail: a longer line stops Follow with bufio.ErrTooLong
	outs := make([]outcome, len(probes))
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		matched := false
		for i, pr := range probes {
			s, err := pr.Parse(sc.Bytes())
			switch {
			case err != nil:
				return err
			case hasSignal(s, pr.Signal):
				outs[i], matched = outcome{s: s}, true
			default:
				outs[i] = outcome{fail: "no match"}
			}
		}
		if matched {
			emit(merge(probes, outs))
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return sc.Err()
}

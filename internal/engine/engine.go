// Package engine reads engine state through probes: one probe per signal,
// combined into per-kind recipes and per-backend plans (ARCHITECTURE §4).
package engine

import "time"

// Opt is a value plus a known flag. OK=false means unknown. Never read V without OK.
type Opt[T any] struct {
	V  T
	OK bool
}

// Signal is one piece of engine state Pharos routes on. Each maps to one Snapshot field.
type Signal uint8

const (
	Version   Signal = iota // per backend
	Models                  // which models the backend serves
	Residency               // per model: a probe lists the models in memory; a model it leaves out is Cold
	VRAMBytes               // per model
	SizeBytes               // per model
	Running                 // per model, or "" = whole backend
	Waiting                 // per model, or "" = whole backend
	Capacity                // per model, or "" = whole backend
	KVUsage                 // 0..1
)

var signalNames = [...]string{"version", "models", "residency", "vram_bytes", "size_bytes", "running", "waiting", "capacity", "kv_usage"}

func (s Signal) String() string {
	if int(s) < len(signalNames) {
		return signalNames[s]
	}
	return "signal(?)"
}

// ParseSignal returns the Signal whose String is name.
func ParseSignal(name string) (Signal, bool) {
	for i, n := range signalNames {
		if n == name {
			return Signal(i), true
		}
	}
	return 0, false
}

// ResidencyState says whether a model is in memory. The zero value is Unknown.
type ResidencyState uint8

const (
	Unknown ResidencyState = iota
	Loaded
	Loading
	Cold
)

var residencyNames = [...]string{"unknown", "loaded", "loading", "cold"}

func (r ResidencyState) String() string {
	if int(r) < len(residencyNames) {
		return residencyNames[r]
	}
	return "residency(?)"
}

// Probe is one way to read one signal from one feed.
type Probe struct {
	Name   string // e.g. "vllm-kv-cache-usage-perc"; shown by doctor and /status
	Signal Signal // the one signal it reads
	Feed   Feed
	// Parse gets a whole HTTP body, or one log line. Only Signal is read from the result.
	Parse func(in []byte) (Snapshot, error)
	// When is nil for any version. Set only for semantic drift (same name, new meaning).
	When func(version string) bool
}

// Feed is where a probe's input comes from.
type Feed struct {
	Path string // HTTP GET, run each scrape round; probes on the same path share one GET
	Log  bool   // or: one line at a time from the backend's log stream
}

// Snapshot is an immutable set of signals from one scrape round of a backend.
type Snapshot struct {
	At      time.Time // stamped by the caller that owns the clock, not by Scrape
	Version Opt[string]
	Models  map[string]ModelInfo // model -> residency
	Load    map[string]Load      // model -> occupancy; key "" = whole backend
	From    map[Signal]string    // signal -> probe that filled it
}

type ModelInfo struct {
	State     ResidencyState
	VRAMBytes Opt[int64]
	SizeBytes Opt[int64] // needed to decide whether a cold load fits
}

type Load struct {
	Running, Waiting, Capacity Opt[int]
	KVUsage                    Opt[float64] // 0..1
}

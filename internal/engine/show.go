package engine

import (
	"maps"
	"slices"
	"strconv"
	"strings"
)

// Show renders the one signal sig of s for doctor and tests: "unknown", a version string, or
// "[key=value ...]" sorted by key ("" is the whole backend, "?" an unknown value).
func (s Snapshot) Show(sig Signal) string {
	var parts []string
	switch sig {
	case Version:
		if !s.Version.OK {
			return "unknown"
		}
		return s.Version.V
	case Models, Residency, VRAMBytes, SizeBytes:
		if s.Models == nil {
			return "unknown"
		}
		for _, k := range slices.Sorted(maps.Keys(s.Models)) {
			m := s.Models[k]
			switch sig {
			case Models:
				parts = append(parts, k)
			case Residency:
				parts = append(parts, k+"="+m.State.String())
			case VRAMBytes:
				parts = append(parts, k+"="+showOpt(m.VRAMBytes))
			case SizeBytes:
				parts = append(parts, k+"="+showOpt(m.SizeBytes))
			}
		}
	default:
		if len(s.Load) == 0 {
			return "unknown"
		}
		for _, k := range slices.Sorted(maps.Keys(s.Load)) {
			v := "?"
			if f, ok := loadSignal(s, sig, k); ok {
				v = strconv.FormatFloat(f, 'g', -1, 64)
			}
			parts = append(parts, k+"="+v)
		}
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func showOpt(o Opt[int64]) string {
	if !o.OK {
		return "?"
	}
	return strconv.FormatInt(o.V, 10)
}

// loadSignal reads sig for key from s.Load as a float, and whether it is known.
func loadSignal(s Snapshot, sig Signal, key string) (float64, bool) {
	l := s.Load[key]
	switch sig {
	case Running:
		return float64(l.Running.V), l.Running.OK
	case Waiting:
		return float64(l.Waiting.V), l.Waiting.OK
	case Capacity:
		return float64(l.Capacity.V), l.Capacity.OK
	case KVUsage:
		return l.KVUsage.V, l.KVUsage.OK
	}
	return 0, false
}

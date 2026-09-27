package engine

import (
	"encoding/json"
	"fmt"
)

// jsonProbe decodes a JSON body into T and hands it to read. A body that isn't
// JSON, or has a field of the wrong type, is an error. read decides what is
// unknown; pointer fields in T tell an absent field from a zero one.
func jsonProbe[T any](name, path string, sig Signal, read func(T) (Snapshot, error)) Probe {
	parse := func(in []byte) (Snapshot, error) {
		var body T
		if err := json.Unmarshal(in, &body); err != nil {
			return Snapshot{}, fmt.Errorf("%s: %w", path, err)
		}
		return read(body)
	}
	return Probe{Name: name, Signal: sig, Feed: Feed{Path: path}, Parse: parse}
}

// modelMap turns a decoded model list into Snapshot.Models. A missing list is
// unknown; an empty one is known (no models). An entry without a name is an error.
func modelMap[E any](list *[]E, name func(E) string, info func(E) ModelInfo) (Snapshot, error) {
	if list == nil {
		return Snapshot{}, nil
	}
	m := make(map[string]ModelInfo, len(*list))
	for i, e := range *list {
		n := name(e)
		if n == "" {
			return Snapshot{}, fmt.Errorf("model %d has no name", i)
		}
		m[n] = info(e)
	}
	return Snapshot{Models: m}, nil
}

// versionProbe reads {"version": "..."}. Ollama and vLLM report a bare version
// ("0.34.4"). llama-swap also answers /api/version, with "v260", and that must
// not pass for an Ollama version, so a version that doesn't start with a digit
// is an error.
func versionProbe(name, path string) Probe {
	return jsonProbe(name, path, Version, func(b struct {
		Version *string `json:"version"`
	}) (Snapshot, error) {
		if b.Version == nil || *b.Version == "" {
			return Snapshot{}, nil
		}
		if v := *b.Version; v[0] < '0' || v[0] > '9' {
			return Snapshot{}, fmt.Errorf("%s: version %q doesn't start with a digit", path, v)
		}
		return Snapshot{Version: Opt[string]{*b.Version, true}}, nil
	})
}

func known[T any](p *T) Opt[T] {
	if p == nil {
		return Opt[T]{}
	}
	return Opt[T]{*p, true}
}

func backendLoad(l Load) Snapshot { return Snapshot{Load: map[string]Load{"": l}} }

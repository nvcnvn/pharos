package engine

import (
	"fmt"
	"strconv"
	"strings"
)

// Library probes. Each metric name and JSON field here was seen in a real
// capture under testdata/ (spike 2026-09-27); the replay test runs every probe
// against every capture that serves its path.
var (
	llamacppRunning = Prom("llamacpp-running", "/metrics", Running, "llamacpp:requests_processing")
	// Before b8772 requests_deferred reads 0 while requests queue (captures b6602,
	// b7493): same name, other meaning, so it is trusted only from that build.
	llamacppWaiting = guarded(Prom("llamacpp-waiting", "/metrics", Waiting, "llamacpp:requests_deferred"), llamacppBuildAtLeast(8772))
	vllmRunning     = Prom("vllm-running", "/metrics", Running, "vllm:num_requests_running", PerModel("model_name"))
	vllmWaiting     = Prom("vllm-waiting", "/metrics", Waiting, "vllm:num_requests_waiting", PerModel("model_name"))
	vllmKVUsage     = Prom("vllm-kv-cache-usage-perc", "/metrics", KVUsage, "vllm:kv_cache_usage_perc", PerModel("model_name"))

	ollamaVersion = versionProbe("ollama-version", "/api/version")
	vllmVersion   = versionProbe("vllm-version", "/version")
	// llama-swap reports its release as "v260", which versionProbe rejects. On
	// Ollama's /api/version this reads Ollama's version; the recipe keeps it out.
	llamaswapVersion = jsonProbe("llamaswap-version", "/api/version", Version, func(b struct {
		Version string `json:"version"`
	}) (Snapshot, error) {
		return Snapshot{Version: Opt[string]{b.Version, b.Version != ""}}, nil
	})

	// An /api/ps entry is a model in memory. Only the recipe keeps look-alikes
	// out: llama.cpp's /models has the same {"models":[{"name"}]} shape.
	ollamaPSResidency = jsonProbe("ollama-ps-residency", "/api/ps", Residency, func(b ollamaList) (Snapshot, error) {
		return modelMap(b.Models, ollamaName, func(ollamaModel) ModelInfo { return ModelInfo{State: Loaded} })
	})
	ollamaPSVRAM = jsonProbe("ollama-ps-size-vram", "/api/ps", VRAMBytes, func(b ollamaList) (Snapshot, error) {
		return modelMap(b.Models, ollamaName, func(m ollamaModel) ModelInfo { return ModelInfo{VRAMBytes: known(m.SizeVRAM)} })
	})
	// /api/tags size is the model file on disk. SGLang serves /api/tags with
	// size 0 for a real model; no model file has size 0, so 0 is unknown.
	ollamaTagsSize = jsonProbe("ollama-tags-size", "/api/tags", SizeBytes, func(b ollamaList) (Snapshot, error) {
		return modelMap(b.Models, ollamaName, func(m ollamaModel) ModelInfo {
			if m.Size != nil && *m.Size <= 0 {
				return ModelInfo{}
			}
			return ModelInfo{SizeBytes: known(m.Size)}
		})
	})

	llamacppPropsCapacity = jsonProbe("llamacpp-props-total-slots", "/props", Capacity, func(b struct {
		TotalSlots *int `json:"total_slots"`
	}) (Snapshot, error) {
		if b.TotalSlots == nil {
			return Snapshot{}, nil
		}
		if *b.TotalSlots <= 0 {
			return Snapshot{}, fmt.Errorf("/props: total_slots %d is not a slot count", *b.TotalSlots)
		}
		return backendLoad(Load{Capacity: known(b.TotalSlots)}), nil
	})
	llamacppPropsVersion = jsonProbe("llamacpp-props-build-info", "/props", Version, func(b struct {
		BuildInfo string `json:"build_info"`
	}) (Snapshot, error) {
		return Snapshot{Version: Opt[string]{b.BuildInfo, b.BuildInfo != ""}}, nil
	})
	// /slots counts busy slots when --metrics is off. One slot without
	// is_processing makes the count unknown.
	llamacppSlotsRunning = jsonProbe("llamacpp-slots-is-processing", "/slots", Running, func(slots *[]struct {
		IsProcessing *bool `json:"is_processing"`
	}) (Snapshot, error) {
		if slots == nil {
			return Snapshot{}, nil
		}
		n := 0
		for _, s := range *slots {
			if s.IsProcessing == nil {
				return Snapshot{}, nil
			}
			if *s.IsProcessing {
				n++
			}
		}
		return backendLoad(Load{Running: Opt[int]{n, true}}), nil
	})

	// /running lists the models llama-swap has started. Only state "ready" was
	// seen in a capture (v260); any other state stays unknown until one is.
	llamaswapRunning = jsonProbe("llamaswap-running", "/running", Residency, func(b struct {
		Running *[]llamaswapProcess `json:"running"`
	}) (Snapshot, error) {
		return modelMap(b.Running, func(p llamaswapProcess) string { return p.Model }, func(p llamaswapProcess) ModelInfo {
			if p.State == "ready" {
				return ModelInfo{State: Loaded}
			}
			return ModelInfo{}
		})
	})

	// Ollama prints its settings once at startup. It runs up to
	// OLLAMA_NUM_PARALLEL requests per loaded model [U: per model, from docs],
	// so this is the capacity of each of its targets. 0 (older releases: pick
	// automatically [U]) doesn't match and stays unknown.
	ollamaLogNumParallel = mustLogLine("ollama-log-num-parallel", Capacity, `msg="server config" .*\bOLLAMA_NUM_PARALLEL:(?P<value>[1-9][0-9]*)\b`, "")

	openaiModels = jsonProbe("openai-models", "/v1/models", Models, func(b struct {
		Data *[]openaiModel `json:"data"`
	}) (Snapshot, error) {
		return modelMap(b.Data, func(m openaiModel) string { return m.ID }, func(openaiModel) ModelInfo { return ModelInfo{} })
	})
)

// Library is every built-in probe. The replay test requires rows for each one.
var Library = []Probe{
	llamacppRunning, llamacppWaiting, vllmRunning, vllmWaiting, vllmKVUsage,
	ollamaVersion, vllmVersion, ollamaPSResidency, ollamaPSVRAM, ollamaTagsSize,
	llamacppPropsCapacity, llamacppPropsVersion, llamacppSlotsRunning,
	llamaswapVersion, llamaswapRunning, openaiModels, ollamaLogNumParallel,
}

func mustLogLine(name string, sig Signal, pattern, value string) Probe {
	p, err := LogLine(name, sig, pattern, value)
	if err != nil {
		panic(err)
	}
	return p
}

// Kind names an engine type. It picks the recipe: the probes we trust on it.
type Kind string

const (
	Auto      Kind = "auto"
	OpenAI    Kind = "openai" // any OpenAI-compatible server: mlx-lm, and SGLang until it has a recipe
	Ollama    Kind = "ollama"
	LlamaCpp  Kind = "llamacpp"
	LlamaSwap Kind = "llama-swap"
	VLLM      Kind = "vllm"
)

// Recipes are the probes we trust per kind. Order is priority when two probes
// read the same signal.
var Recipes = map[Kind][]Probe{
	Ollama:    {ollamaVersion, openaiModels, ollamaPSResidency, ollamaPSVRAM, ollamaTagsSize, ollamaLogNumParallel},
	LlamaCpp:  {llamacppPropsVersion, openaiModels, llamacppPropsCapacity, llamacppRunning, llamacppWaiting, llamacppSlotsRunning},
	LlamaSwap: {llamaswapVersion, openaiModels, llamaswapRunning},
	VLLM:      {vllmVersion, openaiModels, vllmRunning, vllmWaiting, vllmKVUsage},
	OpenAI:    {openaiModels},
}

type ollamaList struct {
	Models *[]ollamaModel `json:"models"`
}

type ollamaModel struct {
	Name     string `json:"name"`
	Size     *int64 `json:"size"`
	SizeVRAM *int64 `json:"size_vram"`
}

func ollamaName(m ollamaModel) string { return m.Name }

type llamaswapProcess struct {
	Model string `json:"model"`
	State string `json:"state"`
}

type openaiModel struct {
	ID string `json:"id"`
}

// guarded sets p.When: p runs only on the versions when accepts. Only for a
// name whose meaning changed between versions (ARCHITECTURE §4).
func guarded(p Probe, when func(version string) bool) Probe {
	p.When = when
	return p
}

// llamacppBuildAtLeast accepts a llama.cpp /props build_info "b<build>-<commit>"
// from build n on. Anything else is an unknown build, so it isn't accepted.
func llamacppBuildAtLeast(n int) func(version string) bool {
	return func(v string) bool {
		b, ok := strings.CutPrefix(v, "b")
		if !ok {
			return false
		}
		b, _, _ = strings.Cut(b, "-")
		build, err := strconv.Atoi(b)
		return err == nil && build >= n
	}
}

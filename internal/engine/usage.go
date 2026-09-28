package engine

import (
	"bytes"
	"encoding/json"
)

// Usage is what one reply reports about its prompt and output. The proxy's stream tap
// reads it from the last usage-bearing line of a response (ARCHITECTURE §9).
type Usage struct {
	PromptTokens     Opt[int]     // the whole prompt, cached or not
	CachedTokens     Opt[int]     // prompt tokens served from the engine's prefix cache
	CompletionTokens Opt[int]     // generated tokens
	PrefillSec       Opt[float64] // time the engine spent on the uncached part of the prompt
	LoadSec          Opt[float64] // time spent loading the model for this request
}

// usageLine holds every usage field seen in a recorded response stream
// (testdata/*/*/streams). Each value reads the first field that is present, in
// the order ParseUsage lists them; a newer name goes first once a recorded
// stream shows it.
type usageLine struct {
	Usage *struct {
		PromptTokens     *int `json:"prompt_tokens"`
		CompletionTokens *int `json:"completion_tokens"`
		Details          *struct {
			CachedTokens *int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
	Timings *struct { // llama.cpp, and llama-swap passing it through
		CacheN     *int     `json:"cache_n"`
		PredictedN *int     `json:"predicted_n"`
		PromptMS   *float64 `json:"prompt_ms"`
	} `json:"timings"`
	// Ollama's native API, on the done line. Durations are nanoseconds.
	PromptEvalCount       *int   `json:"prompt_eval_count"`
	EvalCount             *int   `json:"eval_count"`
	PromptEvalCachedCount *int   `json:"prompt_eval_cached_count"`
	PromptEvalDuration    *int64 `json:"prompt_eval_duration"`
	LoadDuration          *int64 `json:"load_duration"`
}

// ParseUsage reads one response line: an SSE "data: {...}" line, an NDJSON
// line or a whole JSON body. ok is false when the line carries no usage field;
// a line that isn't JSON is not an error, since most stream lines are content.
//
// Semantics differ by version under one name: Ollama v0.30.0 reports only the
// uncached tokens as prompt_eval_count (capture). The caller subtracts cached
// from prompt only when both are known, so that version reads no uncached count.
func ParseUsage(line []byte) (u Usage, ok bool) {
	line = bytes.TrimPrefix(bytes.TrimSpace(line), []byte("data:"))
	var l usageLine
	if json.Unmarshal(line, &l) != nil {
		return Usage{}, false
	}
	var cached, prompt, out []*int
	if l.Usage != nil {
		prompt = append(prompt, l.Usage.PromptTokens)
		out = append(out, l.Usage.CompletionTokens)
		if l.Usage.Details != nil {
			cached = append(cached, l.Usage.Details.CachedTokens)
		}
	}
	prompt = append(prompt, l.PromptEvalCount)
	cached = append(cached, l.PromptEvalCachedCount)
	out = append(out, l.EvalCount)
	var prefill []Opt[float64]
	if l.Timings != nil {
		cached = append(cached, l.Timings.CacheN)
		out = append(out, l.Timings.PredictedN)
		prefill = append(prefill, scaled(l.Timings.PromptMS, 1e-3))
	}
	prefill = append(prefill, nanos(l.PromptEvalDuration))

	u.PromptTokens = first(prompt)
	u.CachedTokens = first(cached)
	u.CompletionTokens = first(out)
	for _, p := range prefill {
		if p.OK {
			u.PrefillSec = p
			break
		}
	}
	u.LoadSec = nanos(l.LoadDuration)
	return u, u.PromptTokens.OK || u.CachedTokens.OK || u.CompletionTokens.OK || u.PrefillSec.OK || u.LoadSec.OK
}

// first returns the first non-negative count.
func first(fields []*int) Opt[int] {
	for _, f := range fields {
		if f != nil && *f >= 0 {
			return Opt[int]{*f, true}
		}
	}
	return Opt[int]{}
}

func scaled(v *float64, by float64) Opt[float64] {
	if v == nil || *v < 0 {
		return Opt[float64]{}
	}
	return Opt[float64]{*v * by, true}
}

func nanos(v *int64) Opt[float64] {
	if v == nil || *v < 0 {
		return Opt[float64]{}
	}
	return Opt[float64]{float64(*v) / 1e9, true}
}

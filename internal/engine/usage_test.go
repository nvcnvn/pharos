package engine

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// lastUsage reads a recorded response stream the way the stream tap does: the
// last line that carries usage wins.
func lastUsage(t *testing.T, file string) (Usage, bool) {
	t.Helper()
	f, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var last Usage
	found := false
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		if u, ok := ParseUsage(sc.Bytes()); ok {
			last, found = u, true
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return last, found
}

// showUsage prints counts exactly; timings only as known or not, since they
// differ on every run.
func showUsage(u Usage) string {
	count := func(o Opt[int]) string {
		if !o.OK {
			return "?"
		}
		return fmt.Sprint(o.V)
	}
	yes := func(o Opt[float64]) string {
		if !o.OK {
			return "?"
		}
		return "yes"
	}
	return fmt.Sprintf("prompt=%s cached=%s out=%s prefill=%s load=%s", count(u.PromptTokens), count(u.CachedTokens), count(u.CompletionTokens), yes(u.PrefillSec), yes(u.LoadSec))
}

// TestReplayUsage reads every recorded reply. Stream 1 sends a ~3200-token
// prompt, stream 2 sends it again. The rest (capture.sh's replies) are what
// clients send besides a streamed chat: a non-streamed chat or completion,
// embeddings, and Ollama's native generate (streamed) and embed, each a short
// prompt; "none" is a reply with no usage, e.g. an engine refusing embeddings.
// Rows are keyed as in rowKeys, with the reply's name in place of the state:
// "engine/version/openai-chat.2".
func TestReplayUsage(t *testing.T) {
	rows := map[string]string{
		// llama.cpp: usage.prompt_tokens_details from b8772, timings.cache_n in every build.
		"llamacpp/*/openai-chat.1":   "prompt=3209 cached=0 out=10 prefill=yes load=?",
		"llamacpp/*/openai-chat.2":   "prompt=3209 cached=3208 out=10 prefill=yes load=?",
		"llama-swap/*/openai-chat.1": "prompt=3209 cached=0 out=10 prefill=yes load=?",
		"llama-swap/*/openai-chat.2": "prompt=3209 cached=3208 out=10 prefill=yes load=?",
		"mlx-lm/*/openai-chat.1":     "prompt=3209 cached=3 out=10 prefill=? load=?",
		"mlx-lm/*/openai-chat.2":     "prompt=3209 cached=3208 out=10 prefill=? load=?",
		"sglang/*/openai-chat.1":     "prompt=3209 cached=3 out=10 prefill=? load=?",
		"sglang/*/openai-chat.2":     "prompt=3209 cached=3208 out=10 prefill=? load=?",
		// vLLM leaves prompt_tokens_details out of a reply with nothing cached
		// before v0.30.0: unknown, not 0.
		"vllm/*/openai-chat.1":       "prompt=3209 cached=? out=10 prefill=? load=?",
		"vllm/v0.30.0/openai-chat.1": "prompt=3209 cached=0 out=10 prefill=? load=?",
		"vllm/*/openai-chat.2":       "prompt=3209 cached=3200 out=10 prefill=? load=?",

		// Ollama reports cached tokens from v0.33.3. v0.30.0 counts only the
		// uncached tokens as the prompt.
		"ollama/*/openai-chat.1":       "prompt=3209 cached=? out=10 prefill=? load=?",
		"ollama/*/openai-chat.2":       "prompt=3209 cached=? out=10 prefill=? load=?",
		"ollama/v0.30.0/openai-chat.2": "prompt=1 cached=? out=10 prefill=? load=?",
		"ollama/v0.33.3/openai-chat.1": "prompt=3209 cached=0 out=10 prefill=? load=?",
		"ollama/v0.33.3/openai-chat.2": "prompt=3209 cached=3208 out=10 prefill=? load=?",
		"ollama/v0.34.4/openai-chat.1": "prompt=3209 cached=0 out=10 prefill=? load=?",
		"ollama/v0.34.4/openai-chat.2": "prompt=3209 cached=3208 out=10 prefill=? load=?",
		"ollama/*/ollama-chat.1":       "prompt=3211 cached=? out=10 prefill=yes load=yes",
		"ollama/*/ollama-chat.2":       "prompt=3211 cached=? out=10 prefill=yes load=yes",
		"ollama/v0.30.0/ollama-chat.1": "prompt=3208 cached=? out=10 prefill=yes load=yes",
		"ollama/v0.30.0/ollama-chat.2": "prompt=1 cached=? out=10 prefill=yes load=yes",
		"ollama/v0.33.3/ollama-chat.1": "prompt=3211 cached=3 out=10 prefill=yes load=yes",
		"ollama/v0.33.3/ollama-chat.2": "prompt=3211 cached=3210 out=10 prefill=yes load=yes",
		"ollama/v0.34.4/ollama-chat.1": "prompt=3209 cached=3208 out=10 prefill=yes load=yes",
		"ollama/v0.34.4/ollama-chat.2": "prompt=3209 cached=3208 out=10 prefill=yes load=yes",

		// One-line replies. Ollama counts the chat template, so its 3-token
		// completion prompt reads 32 like the chat.
		"llamacpp/*/openai-chat":       "prompt=32 cached=31 out=8 prefill=yes load=?",
		"llamacpp/*/openai-completion": "prompt=3 cached=0 out=8 prefill=yes load=?",
		"llamacpp/*/openai-embeddings": "none", // 501: a chat server doesn't embed
		"vllm/*/openai-chat":           "prompt=32 cached=0 out=8 prefill=? load=?",
		"vllm/*/openai-completion":     "prompt=3 cached=0 out=8 prefill=? load=?",
		"vllm/*/openai-embeddings":     "none", // 404: no embeddings route on a generative model
		"ollama/*/openai-chat":         "prompt=32 cached=24 out=8 prefill=? load=?",
		"ollama/*/openai-completion":   "prompt=32 cached=31 out=8 prefill=? load=?",
		"ollama/*/ollama-chat":         "prompt=32 cached=31 out=8 prefill=yes load=yes",
		"ollama/*/ollama-generate":     "prompt=32 cached=31 out=8 prefill=yes load=yes",
		// Embeddings (all-minilm) report the prompt only: no completion count.
		"ollama/*/openai-embeddings": "prompt=5 cached=? out=? prefill=? load=?",
		"ollama/*/ollama-embed":      "prompt=5 cached=? out=? prefill=? load=yes",
	}
	all, err := filepath.Glob(filepath.Join("testdata", "*", "*", "streams", "*"))
	if err != nil || len(all) == 0 {
		t.Fatalf("no recorded streams: %v", err)
	}
	var files []string
	for _, f := range all {
		if b := filepath.Base(f); b != "replies.tsv" && b != "unknown-model.txt" {
			files = append(files, f)
		}
	}
	for _, f := range files {
		rel, _ := filepath.Rel("testdata", f)
		parts := strings.Split(filepath.ToSlash(rel), "/") // engine, version, streams, file
		name := strings.TrimSuffix(parts[3], filepath.Ext(parts[3]))
		key := parts[0] + "/" + parts[1] + "/" + name
		t.Run(key, func(t *testing.T) {
			want, found := "", false
			for _, k := range rowKeys(key) {
				if want, found = rows[k]; found {
					break
				}
			}
			if !found {
				t.Fatalf("no row for %s", key)
			}
			u, ok := lastUsage(t, f)
			got := "none"
			if ok {
				got = showUsage(u)
			}
			if got != want {
				t.Errorf("got %s, want %s", got, want)
			}
		})
	}
}

// Edge cases, cut down from recorded lines.
func TestParseUsage(t *testing.T) {
	cases := []struct {
		name, line string
		want       string // "" = not a usage line
	}{
		{"content_chunk_is_not_usage", `data: {"choices":[{"index":0,"delta":{"content":" you"}}]}`, ""},
		{"done_marker_is_not_usage", `data: [DONE]`, ""},
		{"garbage_is_not_usage", `data: {"usage":`, ""},
		{"llamacpp_prompt_ms_is_milliseconds", `data: {"timings":{"cache_n":3208,"prompt_n":1,"prompt_ms":35.33}}`, "cached=3208 prefill=0.03533"},
		{"ollama_durations_are_nanoseconds", `{"done":true,"load_duration":598208,"prompt_eval_count":3209,"prompt_eval_cached_count":3208,"prompt_eval_duration":5546000}`, "prompt=3209 cached=3208 prefill=0.005546 load=0.000598208"},
		{"missing_cached_tokens_is_unknown_not_zero", `data: {"choices":[],"usage":{"prompt_tokens":3209,"total_tokens":3219,"completion_tokens":10}}`, "prompt=3209 out=10"},
		{"openai_field_goes_ahead_of_timings", `data: {"usage":{"prompt_tokens":3209,"prompt_tokens_details":{"cached_tokens":3208}},"timings":{"cache_n":5}}`, "prompt=3209 cached=3208"},
		{"completion_tokens_alone_is_usage", `data: {"choices":[],"usage":{"completion_tokens":10}}`, "out=10"},
		{"openai_completion_tokens_go_ahead_of_predicted_n", `data: {"usage":{"completion_tokens":10},"timings":{"predicted_n":11}}`, "out=10"},
		{"llamacpp_predicted_n", `data: {"timings":{"cache_n":0,"prompt_n":5,"predicted_n":7}}`, "cached=0 out=7"},
		{"ollama_eval_count", `{"done":true,"prompt_eval_count":3209,"eval_count":10}`, "prompt=3209 out=10"},
		{"whole_json_body_without_data_prefix", `{"id":"x","usage":{"prompt_tokens":12,"prompt_tokens_details":{"cached_tokens":0}}}`, "prompt=12 cached=0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u, ok := ParseUsage([]byte(c.line))
			var got []string
			if u.PromptTokens.OK {
				got = append(got, fmt.Sprintf("prompt=%d", u.PromptTokens.V))
			}
			if u.CachedTokens.OK {
				got = append(got, fmt.Sprintf("cached=%d", u.CachedTokens.V))
			}
			if u.CompletionTokens.OK {
				got = append(got, fmt.Sprintf("out=%d", u.CompletionTokens.V))
			}
			if u.PrefillSec.OK {
				got = append(got, fmt.Sprintf("prefill=%.10g", u.PrefillSec.V))
			}
			if u.LoadSec.OK {
				got = append(got, fmt.Sprintf("load=%.10g", u.LoadSec.V))
			}
			if s := strings.Join(got, " "); s != c.want || ok != (c.want != "") {
				t.Errorf("got %q (ok=%v), want %q", s, ok, c.want)
			}
		})
	}
}

package proxy

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nvcnvn/pharos/internal/engine"
)

// The tap must read the same usage from a stream cut into arbitrary chunks as
// a line-by-line read does, on every recorded stream (layer 2). The expected
// values per engine version are in engine's TestReplayUsage.
func TestTapReplaysRecordedStreams(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "engine", "testdata", "*", "*", "streams", "*-chat.*"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no recorded streams: %v", err)
	}
	for _, f := range files {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var want engine.Usage
		sc := bufio.NewScanner(bytes.NewReader(body))
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			if u, ok := engine.ParseUsage(sc.Bytes()); ok {
				want = u
			}
		}
		if !want.PromptTokens.OK {
			t.Fatalf("%s: no usage line", f)
		}
		for _, size := range []int{1, 7, 4096} {
			var tp tap
			for b := body; len(b) > 0; b = b[min(size, len(b)):] {
				tp.write(b[:min(size, len(b))])
			}
			tp.close()
			if tp.usage != want {
				t.Errorf("%s in %d-byte chunks: %+v, want %+v", f, size, tp.usage, want)
			}
		}
	}
}

func TestTapLines(t *testing.T) {
	usage := `data: {"choices":[],"usage":{"prompt_tokens":3209,"prompt_tokens_details":{"cached_tokens":3208}}}`
	t.Run("last_usage_line_wins", func(t *testing.T) {
		var tp tap
		tp.write([]byte(`data: {"usage":{"prompt_tokens":1}}` + "\n\n" + usage + "\n\ndata: [DONE]\n\n"))
		if tp.usage.PromptTokens.V != 3209 || tp.usage.CachedTokens.V != 3208 {
			t.Errorf("got %+v", tp.usage)
		}
	})
	t.Run("overlong_line_is_skipped_and_the_next_line_still_read", func(t *testing.T) {
		var tp tap
		tp.write([]byte(`data: {"usage":{"prompt_tokens":1},"x":"` + strings.Repeat("x", maxTapLine) + "\"}\n"))
		tp.write([]byte(usage + "\n"))
		if tp.usage.PromptTokens.V != 3209 {
			t.Errorf("got %+v", tp.usage)
		}
	})
	t.Run("body_without_trailing_newline", func(t *testing.T) {
		var tp tap
		tp.write([]byte(`{"id":"x","usage":{"prompt_tokens":30,"prompt_tokens_details":{"cached_tokens":24}}}`))
		tp.close()
		if tp.usage.CachedTokens.V != 24 {
			t.Errorf("got %+v", tp.usage)
		}
	})
	t.Run("no_usage_is_unknown", func(t *testing.T) {
		var tp tap
		tp.write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"usage\"}}]}\n"))
		tp.close()
		if tp.usage != (engine.Usage{}) {
			t.Errorf("got %+v", tp.usage)
		}
	})
}

// Stripping on every recorded OpenAI chat stream removes exactly the usage-only
// event and passes every other byte unchanged, however the body is chunked; the
// tap still reads the usage (layer 2). Without strip the bytes are untouched.
func TestTapStripReplaysRecordedStreams(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "engine", "testdata", "*", "*", "streams", "openai-chat.*.sse"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no recorded streams: %v", err)
	}
	for _, f := range files {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var usageLines []string
		for line := range strings.Lines(string(body)) {
			if strings.Contains(line, `"prompt_tokens"`) {
				usageLines = append(usageLines, line)
			}
		}
		if len(usageLines) != 1 {
			t.Fatalf("%s: %d lines with prompt_tokens, want the one usage chunk", f, len(usageLines))
		}
		want := strings.Replace(string(body), usageLines[0]+"\n", "", 1)
		for _, size := range []int{1, 7, 4096} {
			for _, strip := range []bool{false, true} {
				tp := tap{strip: strip}
				var out []byte
				for b := body; len(b) > 0; b = b[min(size, len(b)):] {
					out = append(out, tp.write(b[:min(size, len(b))])...)
				}
				out = append(out, tp.close()...)
				if w := map[bool]string{false: string(body), true: want}[strip]; string(out) != w {
					t.Errorf("%s in %d-byte chunks, strip %v:\n%s\nwant:\n%s", f, size, strip, out, w)
				}
				if !tp.usage.PromptTokens.OK {
					t.Errorf("%s in %d-byte chunks, strip %v: usage not read", f, size, strip)
				}
			}
		}
	}
}

func TestTapStrip(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"usage_on_a_chunk_with_choices_is_kept",
			"data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":5}}\n\n",
			"data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":5}}\n\n"},
		{"usage_without_a_choices_field_is_kept",
			"data: {\"usage\":{\"prompt_tokens\":5}}\n\n",
			"data: {\"usage\":{\"prompt_tokens\":5}}\n\n"},
		{"done_right_after_the_usage_chunk_is_kept",
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5}}\ndata: [DONE]\n\n",
			"data: [DONE]\n\n"},
		{"crlf_event",
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5}}\r\n\r\ndata: [DONE]\r\n\r\n",
			"data: [DONE]\r\n\r\n"},
		{"body_without_trailing_newline_is_sent",
			`{"choices":[{"message":{}}],"usage":{"prompt_tokens":5}}`,
			`{"choices":[{"message":{}}],"usage":{"prompt_tokens":5}}`},
		{"overlong_line_passes_through",
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5},\"x\":\"" + strings.Repeat("x", maxTapLine) + "\"}\n\n",
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5},\"x\":\"" + strings.Repeat("x", maxTapLine) + "\"}\n\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tp := tap{strip: true}
			out := string(tp.write([]byte(c.in))) + string(tp.close())
			if out != c.want {
				t.Errorf("got %q, want %q", out, c.want)
			}
		})
	}
}

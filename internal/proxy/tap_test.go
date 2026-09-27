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
			tp.end()
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
		tp.end()
		if tp.usage.CachedTokens.V != 24 {
			t.Errorf("got %+v", tp.usage)
		}
	})
	t.Run("no_usage_is_unknown", func(t *testing.T) {
		var tp tap
		tp.write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"usage\"}}]}\n"))
		tp.end()
		if tp.usage != (engine.Usage{}) {
			t.Errorf("got %+v", tp.usage)
		}
	})
}

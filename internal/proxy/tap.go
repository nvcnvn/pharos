package proxy

import (
	"bytes"
	"encoding/json"

	"github.com/nvcnvn/pharos/internal/engine"
)

// maxTapLine bounds the line the tap keeps. A longer line passes through
// unparsed. ponytail: a non-streamed reply over 64 KiB on one line gives no usage.
const maxTapLine = 64 << 10

// tap watches reply bytes as they pass through and remembers the usage of the
// last line that carries any (engine.ParseUsage). It changes the bytes only
// with strip: then it drops the usage-only chunk that Pharos asked for on the
// client's behalf (withUsage), with the blank line that ends its SSE event.
type tap struct {
	line    []byte
	skip    bool // the current line is over maxTapLine
	usage   engine.Usage
	strip   bool
	dropped bool   // the last line was dropped
	out     []byte // strip: the bytes to send on
}

// write reads p and returns the bytes to send to the client: p itself, or
// with strip, the complete lines that were kept. A held partial line goes out
// once its newline arrives; SSE engines write whole events, so that adds no
// latency.
func (t *tap) write(p []byte) []byte {
	pass := p
	t.out = t.out[:0]
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		chunk := p
		if i >= 0 {
			chunk = p[:i]
		}
		switch {
		case t.skip:
			t.emit(chunk)
		case len(t.line)+len(chunk) > maxTapLine:
			t.emit(t.line)
			t.emit(chunk)
			t.skip, t.line = true, t.line[:0]
		default:
			t.line = append(t.line, chunk...)
		}
		if i < 0 {
			break
		}
		t.end(true)
		p = p[i+1:]
	}
	if t.strip {
		return t.out
	}
	return pass
}

// close finishes the body, which may end without a newline (a non-streamed
// reply), and returns the bytes still to send.
func (t *tap) close() []byte {
	t.out = t.out[:0]
	t.end(false)
	return t.out
}

func (t *tap) emit(b []byte) {
	if t.strip {
		t.out = append(t.out, b...)
	}
}

// end finishes the current line.
func (t *tap) end(newline bool) {
	drop := false
	if !t.skip && (bytes.Contains(t.line, []byte(`usage`)) || bytes.Contains(t.line, []byte(`prompt_eval`)) || bytes.Contains(t.line, []byte(`timings`))) {
		if u, ok := engine.ParseUsage(t.line); ok {
			t.usage = u
			drop = t.strip && usageOnly(t.line)
		}
	}
	blankAfterDrop := t.dropped && !t.skip && len(bytes.TrimSpace(t.line)) == 0
	t.dropped = drop
	if t.strip && !drop && !blankAfterDrop {
		if !t.skip {
			t.out = append(t.out, t.line...)
		}
		if newline {
			t.out = append(t.out, '\n')
		}
	}
	t.line, t.skip = t.line[:0], false
}

// usageOnly reports whether an SSE line is a chunk with an empty choices list:
// the chunk every recorded engine sends for stream_options.include_usage
// (captures: Ollama, llama.cpp, llama-swap, vLLM, SGLang, mlx-lm).
func usageOnly(line []byte) bool {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("data:")) {
		return false
	}
	var c struct {
		Choices []json.RawMessage `json:"choices"`
	}
	return json.Unmarshal(line[len("data:"):], &c) == nil && c.Choices != nil && len(c.Choices) == 0
}

package proxy

import (
	"bytes"

	"github.com/nvcnvn/pharos/internal/engine"
)

// maxTapLine bounds the line the tap keeps. A longer line passes through
// unparsed. ponytail: a non-streamed reply over 64 KiB on one line gives no usage.
const maxTapLine = 64 << 10

// tap watches reply bytes as they pass through and remembers the usage of the
// last line that carries any (engine.ParseUsage). It never changes the bytes.
type tap struct {
	line  []byte
	skip  bool // the current line is over maxTapLine
	usage engine.Usage
}

func (t *tap) write(p []byte) {
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		chunk := p
		if i >= 0 {
			chunk = p[:i]
		}
		if !t.skip {
			if len(t.line)+len(chunk) > maxTapLine {
				t.skip, t.line = true, t.line[:0]
			} else {
				t.line = append(t.line, chunk...)
			}
		}
		if i < 0 {
			return
		}
		t.end()
		p = p[i+1:]
	}
}

// end finishes the current line: at a newline, and at the end of the body (a
// non-streamed reply has no trailing newline).
func (t *tap) end() {
	if !t.skip && (bytes.Contains(t.line, []byte(`usage`)) || bytes.Contains(t.line, []byte(`prompt_eval`)) || bytes.Contains(t.line, []byte(`timings`))) {
		if u, ok := engine.ParseUsage(t.line); ok {
			t.usage = u
		}
	}
	t.line, t.skip = t.line[:0], false
}

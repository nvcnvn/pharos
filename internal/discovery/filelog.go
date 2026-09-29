package discovery

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// OpenLog opens a backend's log feed: docker://<container> or file:///<path>.
// With follow it stays open for new lines. The caller closes it.
func OpenLog(ctx context.Context, feed string, follow bool) (io.ReadCloser, error) {
	if name, ok := strings.CutPrefix(feed, "docker://"); ok && name != "" {
		return DockerLogs(ctx, name, follow)
	}
	if path, ok := strings.CutPrefix(feed, "file://"); ok && strings.HasPrefix(path, "/") {
		return FileLog(ctx, path, follow) // as written: no percent-decoding
	}
	return nil, fmt.Errorf("log feed %q: want docker://<container> or file:///<absolute path>", feed)
}

// pollEvery is how often a followed file is checked for new lines; a var so
// tests needn't wait.
var pollEvery = time.Second

// FileLog opens a log file from its start, such as the one a natively
// installed Ollama writes, so lines an engine prints once at startup count.
// With follow it waits at the end for new lines, and starts over when the file
// is replaced or truncated (a restart that rotates it).
func FileLog(ctx context.Context, path string, follow bool) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil || !follow {
		return f, err
	}
	return &tail{ctx: ctx, path: path, f: f, done: make(chan struct{})}, nil
}

// ponytail: polls the file; use kqueue/inotify if a second of lag ever matters.
type tail struct {
	ctx  context.Context
	path string
	done chan struct{}
	once sync.Once

	mu  sync.Mutex // guards f and off against Close
	f   *os.File
	off int64
}

func (t *tail) Read(p []byte) (int, error) {
	for {
		t.mu.Lock()
		n, err := t.f.Read(p)
		t.off += int64(n)
		t.mu.Unlock()
		if n > 0 || err != io.EOF {
			return n, err
		}
		select {
		case <-t.ctx.Done():
			return 0, t.ctx.Err()
		case <-t.done:
			return 0, os.ErrClosed
		case <-time.After(pollEvery):
		}
		t.reopen()
	}
}

// reopen starts over on a new file at the path, or on the same one truncated.
func (t *tail) reopen() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now, err := os.Stat(t.path)
	cur, err2 := t.f.Stat()
	if err != nil || err2 != nil || os.SameFile(now, cur) && now.Size() >= t.off {
		return
	}
	if f, err := os.Open(t.path); err == nil {
		t.f.Close()
		t.f, t.off = f, 0
	}
}

func (t *tail) Close() error {
	t.once.Do(func() { close(t.done) })
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.f.Close()
}

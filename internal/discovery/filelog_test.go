package discovery

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A natively installed engine logs to a file (Ollama: its server config line,
// which holds OLLAMA_NUM_PARALLEL, once at startup). The feed reads it from the
// start, follows new lines, and starts over when a restart replaces the file.
func TestFileLogFollowsAndStartsOverOnANewFile(t *testing.T) {
	pollEvery = time.Millisecond
	path := filepath.Join(t.TempDir(), "server.log")
	write := func(flag int, s string) {
		f, err := os.OpenFile(path, flag|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString(s)
		f.Close()
	}
	write(os.O_TRUNC, "startup OLLAMA_NUM_PARALLEL:2\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc, err := OpenLog(ctx, "file://"+path, true)
	if err != nil {
		t.Fatal(err)
	}
	lines := make(chan string)
	go func() {
		s := bufio.NewScanner(rc)
		for s.Scan() {
			lines <- s.Text()
		}
		close(lines)
	}()
	next := func(want string) {
		t.Helper()
		select {
		case got := <-lines:
			if got != want {
				t.Fatalf("line %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("no line; want %q", want)
		}
	}
	next("startup OLLAMA_NUM_PARALLEL:2")
	write(os.O_APPEND, "a request\n")
	next("a request")

	// A restart that rotates the log: a new file in its place.
	os.Rename(path, path+".1")
	write(os.O_TRUNC, "startup OLLAMA_NUM_PARALLEL:4\n")
	next("startup OLLAMA_NUM_PARALLEL:4")
	// One that truncates it in place.
	write(os.O_TRUNC, "again\n")
	next("again")

	cancel()
	select {
	case _, open := <-lines:
		if open {
			t.Error("a line after cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the feed didn't end when its context did")
	}
	rc.Close()
}

func TestOpenLogRejectsOtherFeeds(t *testing.T) {
	for _, feed := range []string{"/var/log/x.log", "file://relative.log", "docker://", "journald://ollama"} {
		if _, err := OpenLog(context.Background(), feed, false); err == nil || !strings.Contains(err.Error(), "want docker://") {
			t.Errorf("%s: %v", feed, err)
		}
	}
}

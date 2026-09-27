package engine

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestRecordRoundTrip records every replayed capture and wants the same files
// back, byte for byte: Record writes what serveCapture reads.
func TestRecordRoundTrip(t *testing.T) {
	dirs := captures(t)
	for _, capture := range slices.Sorted(maps.Keys(dirs)) {
		t.Run(capture, func(t *testing.T) {
			srv := serveCapture(t, dirs[capture])
			out := t.TempDir()
			if err := Record(context.Background(), srv.Client(), srv.URL, out); err != nil {
				t.Fatal(err)
			}
			want, got := readDir(t, dirs[capture]), readDir(t, out)
			for name, body := range want {
				if got[name] != body {
					t.Errorf("%s differs:\n got %.200q\nwant %.200q", name, got[name], body)
				}
			}
			for name := range got {
				if _, ok := want[name]; !ok {
					t.Errorf("recorded %s, which the capture doesn't have", name)
				}
			}
		})
	}
}

func readDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[e.Name()] = string(b)
	}
	return files
}

func TestRecordPathsCoverEveryLibraryPath(t *testing.T) {
	for _, p := range Library {
		if !p.Feed.Log && !slices.Contains(RecordPaths, p.Feed.Path) {
			t.Errorf("%s reads %s, which Record doesn't save", p.Name, p.Feed.Path)
		}
	}
}

func TestRecordKeepsNo404Body(t *testing.T) {
	f := newFakeEngine(t, map[string]string{"/v1/models": `{"data":[]}`}) // other paths: 404 with a body
	out := t.TempDir()
	if err := Record(context.Background(), f.Client(), f.URL, out); err != nil {
		t.Fatal(err)
	}
	if got := slices.Sorted(maps.Keys(readDir(t, out))); !slices.Equal(got, []string{"paths.tsv", "v1_models"}) {
		t.Errorf("files = %v, want paths.tsv and v1_models", got)
	}
}

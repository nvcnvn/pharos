package peer

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
)

// Layer 2: each release must decode the deltas and state file of the releases
// before it (rolling updates, warm restarts). testdata/<proto>/ holds messages
// encoded by the release that introduced that protocol version. A new release
// adds a directory only when Proto changes; PHAROS_RECORD=1 writes the current one.
var wireSamples = map[string]any{
	"delta.gob": Delta{Proto: 1, Origin: 0xfeed, Fingerprint: 42, Leaving: true,
		Gauges: sched.Gauges{Inflight: map[string]int{"http://gpu-box:8000 qwen": 2}, Waiting: map[string]int{"qwen": 1}},
		Prefix: []prefix.Op{{Target: "http://gpu-box:8000 qwen", Hashes: []uint64{1, 2}, Used: 1_790_000_000_000_000_000}, {Target: "http://gpu-box:8000 qwen", Hashes: []uint64{2}, Remove: true}},
	},
	"snapshot.gob": Snapshot{Proto: 1,
		Prefix: []prefix.Entry{{H: 2, Slots: []prefix.EntrySlot{{Target: "http://gpu-box:8000 qwen", Used: 1_790_000_000_000_000_000}}}},
		Stats:  []state.TargetStats{{Key: "http://gpu-box:8000 qwen", ServiceSec: engine.Opt[float64]{V: 4.5, OK: true}}},
	},
}

func TestWireFormatDecodes(t *testing.T) {
	dir := filepath.Join("testdata", "proto1")
	for name, want := range wireSamples {
		path := filepath.Join(dir, name)
		if os.Getenv("PHAROS_RECORD") == "1" {
			os.MkdirAll(dir, 0o755)
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := gob.NewEncoder(f).Encode(want); err != nil {
				t.Fatal(err)
			}
			f.Close()
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		got := reflect.New(reflect.TypeOf(want))
		if err := gob.NewDecoder(f).Decode(got.Interface()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		f.Close()
		if !reflect.DeepEqual(got.Elem().Interface(), want) {
			t.Errorf("%s:\n got %+v\nwant %+v", name, got.Elem().Interface(), want)
		}
	}
	if _, err := ReadFile(filepath.Join(dir, "snapshot.gob")); err != nil {
		t.Errorf("the state file reader: %v", err)
	}
}

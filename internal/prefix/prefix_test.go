package prefix

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"
)

func msgs(ss ...string) [][]byte {
	var parts [][]byte
	for _, s := range ss {
		parts = append(parts, []byte(s))
	}
	return parts
}

var t0 = time.Unix(1_790_000_000, 0)

func gen0(uint16) uint32 { return 0 }

func TestChain(t *testing.T) {
	a := Chain("m", nil, msgs("user\x00hi", "assistant\x00hello", "user\x00more"))
	b := Chain("m", nil, msgs("user\x00hi", "assistant\x00hello", "user\x00other"))
	if len(a) != 3 || a[0] != b[0] || a[1] != b[1] || a[2] == b[2] {
		t.Errorf("shared_prefix_shares_links_until_they_differ: %v vs %v", a, b)
	}
	if a[2].Bytes != len("user\x00hi")+len("assistant\x00hello")+len("user\x00more") {
		t.Errorf("bytes_are_cumulative: %d", a[2].Bytes)
	}
	if c := Chain("other", nil, msgs("user\x00hi")); c[0] == a[0] {
		t.Error("model_is_part_of_the_hash")
	}
	if c := Chain("m", []byte(`[{"type":"function"}]`), msgs("user\x00hi")); c[0].H == a[0].H {
		t.Error("tools_are_part_of_the_hash")
	}
	if c, d := Chain("m", nil, msgs("ab", "c")), Chain("m", nil, msgs("a", "bc")); c[1].H == d[1].H {
		t.Error("message_boundaries_are_part_of_the_hash")
	}
}

func TestBlocks(t *testing.T) {
	got := Blocks([]byte("abcdefg"), 3)
	if len(got) != 3 || string(got[0]) != "abc" || string(got[2]) != "g" {
		t.Errorf("got %q", got)
	}
	if Blocks(nil, 3) != nil {
		t.Error("empty prompt has no blocks")
	}
}

func TestIndex(t *testing.T) {
	turn1 := Chain("m", nil, msgs("system\x00long preamble", "user\x00q1"))
	turn2 := Chain("m", nil, msgs("system\x00long preamble", "user\x00q1", "assistant\x00a1", "user\x00q2"))

	t.Run("longest_match_per_target", func(t *testing.T) {
		x := New(100)
		x.Record(turn1[:1], 1, 0, t0) // target 1 saw only the system prompt
		x.Record(turn1, 2, 0, t0)     // target 2 saw turn 1
		got := x.Lookup(turn2, gen0)
		if got[1].Bytes != turn1[0].Bytes || got[2].Bytes != turn1[1].Bytes || len(got) != 2 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("no_match_is_empty", func(t *testing.T) {
		if got := New(100).Lookup(turn2, gen0); len(got) != 0 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("stale_generation_does_not_match", func(t *testing.T) {
		x := New(100)
		x.Record(turn1, 1, 0, t0)
		if got := x.Lookup(turn2, func(uint16) uint32 { return 1 }); len(got) != 0 {
			t.Errorf("model unloaded since: got %v", got)
		}
	})
	t.Run("remove_forgets_only_that_target", func(t *testing.T) {
		x := New(100)
		x.Record(turn1, 1, 0, t0)
		x.Record(turn1, 2, 0, t0)
		x.Remove(turn2, 1)
		if got := x.Lookup(turn2, gen0); len(got) != 1 || got[2].Bytes == 0 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("capacity_evicts_least_recently_recorded", func(t *testing.T) {
		x := New(2)
		a, b, c := Chain("m", nil, msgs("a")), Chain("m", nil, msgs("b")), Chain("m", nil, msgs("c"))
		x.Record(a, 1, 0, t0)
		x.Record(b, 1, 0, t0)
		x.Record(a, 1, 0, t0.Add(time.Second)) // a is fresh again
		x.Record(c, 1, 0, t0.Add(2*time.Second))
		if x.Len() != 2 || len(x.Lookup(b, gen0)) != 0 || len(x.Lookup(a, gen0)) != 1 {
			t.Errorf("len %d; b should be gone, a kept", x.Len())
		}
	})
	t.Run("full_entry_replaces_oldest_target", func(t *testing.T) {
		x := New(10)
		for i := range slotsPerEntry {
			x.Record(turn1, uint16(i), 0, t0.Add(time.Duration(i)*time.Second))
		}
		x.Record(turn1, 0, 0, t0.Add(time.Minute))    // refresh target 0: stays in its slot
		x.Record(turn1, 99, 0, t0.Add(2*time.Minute)) // evicts target 1, the oldest
		got := x.Lookup(turn1, gen0)
		if len(got) != slotsPerEntry || got[0].Bytes == 0 || got[99].Bytes == 0 || got[1].Bytes != 0 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("record_updates_generation", func(t *testing.T) {
		x := New(10)
		x.Record(turn1, 1, 0, t0)
		x.Record(turn1, 1, 1, t0.Add(time.Second)) // reloaded and served again
		if got := x.Lookup(turn1, func(uint16) uint32 { return 1 }); got[1].Bytes == 0 {
			t.Errorf("got %v", got)
		}
	})
}

// Two instances name the same target by key; each has its own local ID.
func resolverOf(ids map[string]uint16, gens map[uint16]uint32) Resolver {
	return func(key string) (uint16, uint32, bool) {
		id, ok := ids[key]
		return id, gens[id], ok
	}
}

func keysOf(ids map[string]uint16, gens map[uint16]uint32) func(uint16) (string, uint32, bool) {
	return func(id uint16) (string, uint32, bool) {
		for k, v := range ids {
			if v == id {
				return k, gens[id], true
			}
		}
		return "", 0, false
	}
}

func TestReplication(t *testing.T) {
	turn1 := Chain("m", nil, msgs("system\x00long preamble", "user\x00q1"))
	turn2 := Chain("m", nil, msgs("system\x00long preamble", "user\x00q1", "assistant\x00a1", "user\x00q2"))
	here := map[string]uint16{"http://x m": 7, "http://y m": 8}

	t.Run("ops_record_and_remove_by_key", func(t *testing.T) {
		x := New(100)
		x.Merge([]Op{
			{Target: "http://x m", Hashes: Hashes(turn1), Used: t0.UnixNano()},
			{Target: "http://y m", Hashes: Hashes(turn1), Used: t0.UnixNano()},
			{Target: "http://unknown m", Hashes: Hashes(turn2), Used: t0.UnixNano()}, // dropped
		}, resolverOf(here, nil))
		if got := x.Lookup(turn2, gen0); got[7].Bytes != turn1[1].Bytes || got[8].Bytes != turn1[1].Bytes || len(got) != 2 {
			t.Errorf("after records: %v", got)
		}
		x.Merge([]Op{{Target: "http://y m", Hashes: Hashes(turn2), Remove: true}}, resolverOf(here, nil))
		if got := x.Lookup(turn2, gen0); len(got) != 1 || got[7].Bytes == 0 {
			t.Errorf("after a correction: %v", got)
		}
	})
	t.Run("merged_slot_takes_the_current_generation_here", func(t *testing.T) {
		x := New(100)
		gens := map[uint16]uint32{7: 3}
		x.Merge([]Op{{Target: "http://x m", Hashes: Hashes(turn1), Used: t0.UnixNano()}}, resolverOf(here, gens))
		if got := x.Lookup(turn1, func(id uint16) uint32 { return gens[id] }); got[7].Bytes == 0 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("export_then_merge_entries_carries_matches_and_order", func(t *testing.T) {
		there := map[string]uint16{"http://x m": 1, "http://y m": 2, "http://gone m": 3}
		gens := map[uint16]uint32{2: 1} // y unloaded since it recorded
		src := New(100)
		a, b := Chain("m", nil, msgs("a")), Chain("m", nil, msgs("b"))
		src.Record(turn1, 1, 0, t0)
		src.Record(turn1, 2, 0, t0)
		src.Record(a, 3, 0, t0) // a target the receiver doesn't know
		src.Record(a, 1, 0, t0.Add(time.Second))
		src.Record(b, 1, 0, t0.Add(2*time.Second)) // the most recent
		entries := src.Export(keysOf(there, gens))
		for _, e := range entries {
			for _, s := range e.Slots {
				if s.Target == "http://y m" {
					t.Error("exported a slot of an older generation")
				}
			}
		}

		dst := New(2) // room for the two most recent prefixes only
		dst.MergeEntries(entries, resolverOf(here, nil))
		if got := dst.Lookup(b, gen0); got[7].Bytes == 0 {
			t.Errorf("most recent prefix lost: %v", got)
		}
		if got := dst.Lookup(a, gen0); got[7].Bytes == 0 || len(got) != 1 {
			t.Errorf("second most recent: %v", got)
		}
		if got := dst.Lookup(turn1, gen0); len(got) != 0 {
			t.Errorf("the oldest should have been evicted first: %v", got)
		}
	})
	t.Run("a_match_names_the_source_of_its_newest_record", func(t *testing.T) {
		x := New(100)
		x.Record(turn1, 7, 0, t0.Add(time.Second))
		if got := x.Lookup(turn1, gen0)[7].Source; got != Local {
			t.Errorf("recorded here: %v", got)
		}
		x.Merge([]Op{{Target: "http://x m", Hashes: Hashes(turn1), Used: t0.UnixNano()}}, resolverOf(here, nil))
		if got := x.Lookup(turn1, gen0)[7].Source; got != Local {
			t.Errorf("an older peer record changed the source: %v", got)
		}
		x.Merge([]Op{{Target: "http://x m", Hashes: Hashes(turn1), Used: t0.Add(time.Minute).UnixNano()}}, resolverOf(here, nil))
		if got := x.Lookup(turn1, gen0)[7].Source; got != Peer {
			t.Errorf("a newer peer record: %v", got)
		}
		x.MergeEntries([]Entry{{H: turn1[0].H, Slots: []EntrySlot{{Target: "http://x m", Used: t0.Add(time.Minute).UnixNano()}}}}, resolverOf(here, nil))
		if got := x.Lookup(turn1, gen0)[7].Source; got != Peer {
			t.Errorf("the same record again from a snapshot: %v", got)
		}
		x.MergeEntries([]Entry{{H: turn1[0].H, Slots: []EntrySlot{{Target: "http://y m", Used: t0.UnixNano()}}}}, resolverOf(here, nil))
		if got := x.Lookup(turn1, gen0)[8]; got.Source != Restored || got.Bytes != turn1[0].Bytes {
			t.Errorf("from a snapshot: %+v", got)
		}
	})
	t.Run("an_older_merged_slot_does_not_evict_newer_ones", func(t *testing.T) {
		x := New(10)
		for i := range slotsPerEntry {
			x.Record(turn1, uint16(i), 0, t0.Add(time.Minute))
		}
		x.Merge([]Op{{Target: "http://x m", Hashes: Hashes(turn1), Used: t0.UnixNano()}}, resolverOf(here, nil))
		if got := x.Lookup(turn1, gen0); got[7].Bytes != 0 || len(got) != slotsPerEntry {
			t.Errorf("got %v", got)
		}
	})
}

// Merge law: record ops in any order, any number of times, give the same matches.
func TestMergeRecordsInAnyOrder(t *testing.T) {
	chains := [][]Link{
		Chain("m", nil, msgs("a")), Chain("m", nil, msgs("a", "b")), Chain("m", nil, msgs("c")),
	}
	keys := []string{"http://x m", "http://y m", "http://z m"}
	ids := map[string]uint16{keys[0]: 1, keys[1]: 2, keys[2]: 3}
	var ops []Op
	for i, c := range chains {
		for j, k := range keys {
			if (i+j)%2 == 0 {
				ops = append(ops, Op{Target: k, Hashes: Hashes(c), Used: t0.Add(time.Duration(i+j) * time.Second).UnixNano()})
			}
		}
	}
	lookups := func(x *Index) []map[uint16]Match {
		var out []map[uint16]Match
		for _, c := range chains {
			out = append(out, x.Lookup(c, gen0))
		}
		return out
	}
	want := New(100)
	want.Merge(ops, resolverOf(ids, nil))
	f := func(order []uint8) bool {
		x := New(100)
		for _, o := range order { // a random sequence: reordered, duplicated, some missing
			x.Merge([]Op{ops[int(o)%len(ops)]}, resolverOf(ids, nil))
		}
		x.Merge(ops, resolverOf(ids, nil)) // then everything at least once
		return reflect.DeepEqual(lookups(x), lookups(want))
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

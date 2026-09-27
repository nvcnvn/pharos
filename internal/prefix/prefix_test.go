package prefix

import (
	"testing"
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
		if got[1] != turn1[0].Bytes || got[2] != turn1[1].Bytes || len(got) != 2 {
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
		if got := x.Lookup(turn2, gen0); len(got) != 1 || got[2] == 0 {
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
		if len(got) != slotsPerEntry || got[0] == 0 || got[99] == 0 || got[1] != 0 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("record_updates_generation", func(t *testing.T) {
		x := New(10)
		x.Record(turn1, 1, 0, t0)
		x.Record(turn1, 1, 1, t0.Add(time.Second)) // reloaded and served again
		if got := x.Lookup(turn1, func(uint16) uint32 { return 1 }); got[1] == 0 {
			t.Errorf("got %v", got)
		}
	})
}

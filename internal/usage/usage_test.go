package usage

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

// start is 10:00:00 UTC; minute and day boundaries are easy to reach from it.
func counter(origin uint64, c *clock) *Counter {
	return New(Config{Origin: origin, Now: c.Now})
}

func newClock() *clock { return &clock{time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)} }

func tokens(prompt, completion int) engine.Usage {
	return engine.Usage{PromptTokens: engine.Opt[int]{V: prompt, OK: true}, CompletionTokens: engine.Opt[int]{V: completion, OK: true}}
}

func TestRequestsPerMinute(t *testing.T) {
	t.Run("the_request_over_the_limit_is_rejected", func(t *testing.T) {
		c := newClock()
		u := counter(1, c)
		for i := range 3 {
			if _, err := u.Admit("alice", Limits{RPM: 3}); err != nil {
				t.Fatalf("request %d: %v", i+1, err)
			}
		}
		wait, err := u.Admit("alice", Limits{RPM: 3})
		if !errors.Is(err, ErrRateLimited) || wait <= 0 || wait > time.Minute {
			t.Errorf("4th request: wait %v, err %v", wait, err)
		}
		if _, err := u.Admit("bob", Limits{RPM: 3}); err != nil {
			t.Errorf("another key is limited on its own: %v", err)
		}
	})
	t.Run("rejected_requests_dont_count", func(t *testing.T) {
		c := newClock()
		u := counter(1, c)
		for range 10 {
			u.Admit("alice", Limits{RPM: 2})
		}
		if got := u.RPM("alice"); got != 2 {
			t.Errorf("rpm %v, want 2", got)
		}
	})
	t.Run("the_window_slides_over_the_previous_minute", func(t *testing.T) {
		c := newClock()
		u := counter(1, c)
		for range 4 {
			u.Admit("alice", Limits{})
		}
		// 15 s into the next minute: 4 × (1 − 0.25) + 0 = 3.
		c.now = c.now.Add(75 * time.Second)
		if got := u.RPM("alice"); got != 3 {
			t.Fatalf("rpm %v, want 3", got)
		}
		if _, err := u.Admit("alice", Limits{RPM: 3}); !errors.Is(err, ErrRateLimited) {
			t.Errorf("at 3 of 3: %v", err)
		}
		// 45 s in: 4 × 0.25 = 1, so one more fits under 3... and another.
		c.now = c.now.Add(30 * time.Second)
		for i := range 2 {
			if _, err := u.Admit("alice", Limits{RPM: 3}); err != nil {
				t.Errorf("request %d at 45 s: %v", i+1, err)
			}
		}
	})
	t.Run("counts_every_origin", func(t *testing.T) {
		c := newClock()
		u := counter(1, c)
		peer := counter(2, c)
		peer.Admit("alice", Limits{})
		peer.Admit("alice", Limits{})
		u.Merge(peer.Export())
		if _, err := u.Admit("alice", Limits{RPM: 3}); err != nil {
			t.Fatal(err)
		}
		if _, err := u.Admit("alice", Limits{RPM: 3}); !errors.Is(err, ErrRateLimited) {
			t.Errorf("2 on the peer + 1 here = 3 of 3, want rejected: %v", err)
		}
	})
	t.Run("older_minutes_are_forgotten", func(t *testing.T) {
		c := newClock()
		u := counter(1, c)
		u.Admit("alice", Limits{})
		c.now = c.now.Add(2 * time.Minute)
		if got := u.RPM("alice"); got != 0 {
			t.Errorf("rpm %v, want 0", got)
		}
		for _, cell := range append(u.Export(), u.Snapshot()...) {
			if cell.Kind == Minute {
				t.Errorf("stale minute cell kept: %+v", cell)
			}
		}
	})
}

func TestTokensPerDay(t *testing.T) {
	t.Run("a_key_over_its_tokens_waits_for_the_next_day", func(t *testing.T) {
		c := newClock()
		u := counter(1, c)
		u.Record("alice", "m", tokens(900, 100))
		wait, err := u.Admit("alice", Limits{TokensPerDay: 1000})
		if !errors.Is(err, ErrQuotaExceeded) || wait != 14*time.Hour {
			t.Fatalf("at 1000 of 1000: wait %v, err %v", wait, err)
		}
		c.now = c.now.Add(14 * time.Hour)
		if _, err := u.Admit("alice", Limits{TokensPerDay: 1000}); err != nil {
			t.Errorf("next day: %v", err)
		}
	})
	t.Run("summed_over_models_and_origins_including_departed_ones", func(t *testing.T) {
		c := newClock()
		u := counter(1, c)
		gone := counter(2, c) // a replaced instance: its cells stay
		gone.Record("alice", "a", tokens(300, 0))
		u.Merge(gone.Snapshot())
		u.Record("alice", "b", tokens(300, 300))
		if got := u.TokensToday("alice"); got != 900 {
			t.Errorf("tokens %d, want 900", got)
		}
	})
	t.Run("the_day_ends_at_midnight_in_the_configured_timezone", func(t *testing.T) {
		c := newClock()
		loc := time.FixedZone("UTC+7", 7*3600)
		u := New(Config{Origin: 1, Now: c.Now, Location: loc})
		u.Record("alice", "m", tokens(10, 0))
		c.now = time.Date(2026, 9, 28, 16, 59, 0, 0, time.UTC) // 23:59 local
		if got := u.TokensToday("alice"); got != 10 {
			t.Errorf("before local midnight: %d, want 10", got)
		}
		c.now = c.now.Add(time.Minute) // 00:00 local
		if got := u.TokensToday("alice"); got != 0 {
			t.Errorf("after local midnight: %d, want 0", got)
		}
	})
	t.Run("unknown_usage_is_counted_as_unmetered_not_as_zero_tokens", func(t *testing.T) {
		c := newClock()
		u := counter(1, c)
		u.Record("alice", "m", engine.Usage{})
		u.Record("alice", "m", engine.Usage{PromptTokens: engine.Opt[int]{V: 5, OK: true}})
		rows := u.History(c.now, c.now, ByKey)
		if len(rows) != 1 || rows[0].Requests != 2 || rows[0].Unmetered != 2 || rows[0].PromptTok != 5 {
			t.Errorf("got %+v", rows)
		}
	})
}

func TestMerge(t *testing.T) {
	// Any order and any duplicates of the same cells give the same state:
	// field-wise max per cell.
	c := newClock()
	var all []Cell
	for origin := uint64(1); origin <= 3; origin++ {
		u := counter(origin, c)
		for i := range 5 {
			u.Admit("alice", Limits{})
			u.Record([]string{"alice", "bob"}[i%2], []string{"a", "b"}[i%2], tokens(10*i, i))
			all = append(all, u.Export()...) // earlier, smaller versions of the same cells too
		}
	}
	want := counter(9, c)
	want.Merge(all)
	r := rand.New(rand.NewPCG(1, 2))
	for range 50 {
		cells := slices.Clone(all)
		r.Shuffle(len(cells), func(i, j int) { cells[i], cells[j] = cells[j], cells[i] })
		cells = append(cells, cells[:r.IntN(len(cells))]...)
		got := counter(9, c)
		for len(cells) > 0 { // in random batches, as deltas arrive
			n := 1 + r.IntN(len(cells))
			got.Merge(cells[:n])
			cells = cells[n:]
		}
		if !slices.Equal(sorted(got.Snapshot()), sorted(want.Snapshot())) || got.RPM("alice") != want.RPM("alice") {
			t.Fatalf("merge depends on order or duplicates")
		}
	}
	if got := want.TokensToday("alice"); got != 3*(0+2+20+4+40) {
		t.Errorf("tokens %d", got)
	}
}

func sorted(cells []Cell) []Cell {
	slices.SortFunc(cells, func(a, b Cell) int {
		if a.Origin != b.Origin {
			return int(a.Origin) - int(b.Origin)
		}
		if a.Key != b.Key {
			return map[bool]int{true: -1, false: 1}[a.Key < b.Key]
		}
		return map[bool]int{true: -1, false: 1}[a.Model < b.Model]
	})
	return cells
}

func TestExportAndSnapshot(t *testing.T) {
	c := newClock()
	u := counter(1, c)
	u.Record("alice", "m", tokens(1, 1)) // 3 days ago
	c.now = c.now.Add(48 * time.Hour)
	u.Record("alice", "m", tokens(1, 1)) // yesterday
	c.now = c.now.Add(24 * time.Hour)
	u.Admit("alice", Limits{})
	u.Record("alice", "m", tokens(1, 1)) // today
	peer := counter(2, c)
	peer.Record("bob", "m", tokens(1, 1))
	u.Merge(peer.Export())

	var days []int64
	minutes := 0
	for _, cell := range u.Export() {
		if cell.Origin != 1 {
			t.Errorf("a delta carries only this origin's cells: %+v", cell)
		}
		if cell.Kind == Minute {
			minutes++
		} else {
			days = append(days, cell.Bucket)
		}
	}
	if len(days) != 2 || minutes != 1 {
		t.Errorf("delta: days %v and %d minute cells, want today and yesterday and 1", days, minutes)
	}
	origins := map[uint64]int{}
	for _, cell := range u.Snapshot() {
		if cell.Kind == Minute {
			t.Errorf("snapshot has a minute cell: %+v", cell)
		}
		origins[cell.Origin]++
	}
	if origins[1] != 3 || origins[2] != 1 {
		t.Errorf("snapshot: day cells per origin %v, want 3 and 1", origins)
	}
}

func TestRetention(t *testing.T) {
	c := newClock()
	u := New(Config{Origin: 1, Now: c.Now, RetentionDays: 2})
	u.Record("alice", "m", tokens(1, 1))
	c.now = c.now.Add(2 * 24 * time.Hour)
	u.Record("alice", "m", tokens(1, 1))
	if n := len(u.Snapshot()); n != 1 {
		t.Errorf("%d day cells, want only today's", n)
	}
	old := counter(2, &clock{c.now.Add(-5 * 24 * time.Hour)})
	old.Record("bob", "m", tokens(1, 1))
	u.Merge(old.Snapshot())
	if n := len(u.Snapshot()); n != 1 {
		t.Errorf("merged a day outside retention: %d cells", n)
	}
}

func TestHistory(t *testing.T) {
	c := newClock()
	u := counter(1, c)
	u.Record("alice", "a", tokens(10, 1))
	u.Record("alice", "b", tokens(20, 2))
	u.Record("bob", "a", engine.Usage{PromptTokens: engine.Opt[int]{V: 30, OK: true}, CachedTokens: engine.Opt[int]{V: 25, OK: true}, CompletionTokens: engine.Opt[int]{V: 3, OK: true}})
	c.now = c.now.Add(24 * time.Hour)
	u.Record("alice", "a", tokens(40, 4))

	byKey := u.History(c.now.Add(-24*time.Hour), c.now, ByKey)
	want := []Row{
		{Day: "2026-09-28", Name: "alice", Requests: 2, PromptTok: 30, CompletionTok: 3},
		{Day: "2026-09-28", Name: "bob", Requests: 1, PromptTok: 30, CachedTok: 25, CompletionTok: 3},
		{Day: "2026-09-29", Name: "alice", Requests: 1, PromptTok: 40, CompletionTok: 4},
	}
	if !slices.Equal(byKey, want) {
		t.Errorf("by key:\n%+v\nwant\n%+v", byKey, want)
	}
	byModel := u.History(c.now, c.now, ByModel)
	if len(byModel) != 1 || byModel[0].Name != "a" || byModel[0].PromptTok != 40 {
		t.Errorf("by model, today only: %+v", byModel)
	}
}

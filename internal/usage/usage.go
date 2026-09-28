// Package usage counts requests and tokens per API key in per-origin counter
// cells (ARCHITECTURE §11). A process only increments its own origin's cells;
// other origins' cells are read-only copies merged by field-wise max, so
// deltas may arrive late, twice or out of order. Totals sum over origins, and
// cells of departed origins stay, so their usage isn't lost.
package usage

import (
	"cmp"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
)

type Kind uint8

const (
	Day    Kind = iota // usage history and tokens per day; Bucket is the day number
	Minute             // requests per minute; Bucket is the unix minute, Model is ""
)

type Cell struct {
	Origin uint64
	Kind   Kind
	Bucket int64
	Key    string // key name
	Model  string
	// Unmetered counts served requests whose reply didn't report prompt or
	// completion tokens: their tokens are unknown, not 0.
	Requests, PromptTok, CachedTok, CompletionTok, Unmetered uint64
}

type id struct {
	origin uint64
	kind   Kind
	bucket int64
	key    string
	model  string
}

func (c *Cell) id() id { return id{c.Origin, c.Kind, c.Bucket, c.Key, c.Model} }

type Config struct {
	Origin        uint64
	Location      *time.Location // where a day ends; nil = UTC
	RetentionDays int            // days of history kept, today included; default 400
	Now           func() time.Time
}

// Limits are one key's quotas; 0 = no limit.
type Limits struct {
	RPM          int
	TokensPerDay uint64
}

var (
	ErrRateLimited   = errors.New("requests per minute over the key's limit")
	ErrQuotaExceeded = errors.New("tokens per day over the key's quota")
)

type Counter struct {
	cfg Config

	mu      sync.Mutex
	cells   map[id]*Cell
	byDay   map[dayKey][]*Cell // Day cells of one bucket and key, every origin and model
	origins map[uint64]bool
	pruned  int64 // the minute of the last prune
}

type dayKey struct {
	bucket int64
	key    string
}

func New(cfg Config) *Counter {
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	if cfg.RetentionDays <= 0 {
		cfg.RetentionDays = 400
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Counter{cfg: cfg, cells: map[id]*Cell{}, byDay: map[dayKey][]*Cell{}, origins: map[uint64]bool{}}
}

// day is t's day number in the configured timezone.
func (c *Counter) day(t time.Time) int64 {
	y, m, d := t.In(c.cfg.Location).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400
}

func dayString(day int64) string { return time.Unix(day*86400, 0).UTC().Format(time.DateOnly) }

// cellLocked returns the cell, creating it empty.
func (c *Counter) cellLocked(i id) *Cell {
	if cell := c.cells[i]; cell != nil {
		return cell
	}
	cell := &Cell{Origin: i.origin, Kind: i.kind, Bucket: i.bucket, Key: i.key, Model: i.model}
	c.cells[i] = cell
	c.origins[i.origin] = true
	if i.kind == Day {
		k := dayKey{i.bucket, i.key}
		c.byDay[k] = append(c.byDay[k], cell)
	}
	return cell
}

// Admit counts a request against key's requests per minute, if it is within
// its limits. A rejected request isn't counted. wait says when to retry.
func (c *Counter) Admit(key string, l Limits) (wait time.Duration, err error) {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
	if l.TokensPerDay > 0 && c.tokensLocked(key, c.day(now)) >= l.TokensPerDay {
		y, m, d := now.In(c.cfg.Location).Date()
		return time.Date(y, m, d+1, 0, 0, 0, 0, c.cfg.Location).Sub(now), ErrQuotaExceeded
	}
	if l.RPM > 0 && c.rpmLocked(key, now) >= float64(l.RPM) {
		// ponytail: waits for the next minute; solve the window for the exact
		// moment if clients retry too early or too late.
		return time.Minute - now.Sub(now.Truncate(time.Minute)), ErrRateLimited
	}
	c.cellLocked(id{c.cfg.Origin, Minute, now.Unix() / 60, key, ""}).Requests++
	return 0, nil
}

// Record adds a served request's usage to key and model's day.
func (c *Counter) Record(key, model string, u engine.Usage) {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	cell := c.cellLocked(id{c.cfg.Origin, Day, c.day(now), key, model})
	cell.Requests++
	add := func(dst *uint64, o engine.Opt[int]) {
		if o.OK && o.V > 0 {
			*dst += uint64(o.V)
		}
	}
	add(&cell.PromptTok, u.PromptTokens)
	add(&cell.CachedTok, u.CachedTokens)
	add(&cell.CompletionTok, u.CompletionTokens)
	if !u.PromptTokens.OK || !u.CompletionTokens.OK {
		cell.Unmetered++
	}
}

// RPM is key's requests per minute over every origin: a sliding window,
// previous minute × (1 − elapsed fraction) + current minute.
func (c *Counter) RPM(key string) float64 {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rpmLocked(key, now)
}

func (c *Counter) rpmLocked(key string, now time.Time) float64 {
	minute := now.Unix() / 60
	frac := float64(now.Unix()%60) / 60
	var sum float64
	for o := range c.origins {
		if cell := c.cells[id{o, Minute, minute - 1, key, ""}]; cell != nil {
			sum += float64(cell.Requests) * (1 - frac)
		}
		if cell := c.cells[id{o, Minute, minute, key, ""}]; cell != nil {
			sum += float64(cell.Requests)
		}
	}
	return sum
}

// TokensToday is key's prompt plus completion tokens today, over every origin.
func (c *Counter) TokensToday(key string) uint64 {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tokensLocked(key, c.day(now))
}

func (c *Counter) tokensLocked(key string, day int64) uint64 {
	var sum uint64
	for _, cell := range c.byDay[dayKey{day, key}] {
		sum += cell.PromptTok + cell.CompletionTok
	}
	return sum
}

// pruneLocked drops Minute cells older than the previous minute and Day cells
// outside retention, at most once a minute: it walks every cell.
func (c *Counter) pruneLocked(now time.Time) {
	minute, oldest := now.Unix()/60, c.day(now)-int64(c.cfg.RetentionDays)
	if minute == c.pruned {
		return
	}
	c.pruned = minute
	for i := range c.cells {
		if i.kind == Minute && i.bucket < minute-1 || i.kind == Day && i.bucket <= oldest {
			delete(c.cells, i)
		}
	}
	maps.DeleteFunc(c.byDay, func(k dayKey, _ []*Cell) bool { return k.bucket <= oldest })
}

// Export returns this origin's cells for a peer delta: today's and
// yesterday's Day cells, and the current and previous Minute cells. Values
// are absolute, so a lost delta is repaired by the next.
func (c *Counter) Export() []Cell {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
	yesterday := c.day(now) - 1
	var out []Cell
	for _, cell := range c.cells {
		if cell.Origin == c.cfg.Origin && (cell.Kind == Minute || cell.Bucket >= yesterday) {
			out = append(out, *cell)
		}
	}
	return out
}

// Snapshot returns the Day cells of every origin within retention, for a
// peer snapshot and the state file.
func (c *Counter) Snapshot() []Cell {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
	var out []Cell
	for _, cell := range c.cells {
		if cell.Kind == Day {
			out = append(out, *cell)
		}
	}
	return out
}

// Merge applies cells from a peer or the state file: field-wise max per cell.
func (c *Counter) Merge(cells []Cell) {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	minute, oldest := now.Unix()/60, c.day(now)-int64(c.cfg.RetentionDays)
	for _, in := range cells {
		if in.Kind == Minute && in.Bucket < minute-1 || in.Kind == Day && in.Bucket <= oldest || in.Kind > Minute {
			continue
		}
		cell := c.cellLocked(in.id())
		cell.Requests = max(cell.Requests, in.Requests)
		cell.PromptTok = max(cell.PromptTok, in.PromptTok)
		cell.CachedTok = max(cell.CachedTok, in.CachedTok)
		cell.CompletionTok = max(cell.CompletionTok, in.CompletionTok)
		cell.Unmetered = max(cell.Unmetered, in.Unmetered)
	}
}

// By groups History rows.
type By uint8

const (
	ByKey By = iota
	ByModel
)

// Row is one day's usage of one key or model, over every origin.
type Row struct {
	Day           string `json:"day"` // YYYY-MM-DD
	Name          string `json:"name"`
	Requests      uint64 `json:"requests"`
	PromptTok     uint64 `json:"prompt_tokens"`
	CachedTok     uint64 `json:"cached_tokens"` // part of PromptTok
	CompletionTok uint64 `json:"completion_tokens"`
	Unmetered     uint64 `json:"unmetered_requests"` // their tokens are unknown
}

// History returns the usage of each day from from to to (both included), by
// key or by model, ordered by day then name.
func (c *Counter) History(from, to time.Time, by By) []Row {
	first, last := c.day(from), c.day(to)
	c.mu.Lock()
	defer c.mu.Unlock()
	type group struct {
		day  int64
		name string
	}
	sums := map[group]*Row{}
	for _, cell := range c.cells {
		if cell.Kind != Day || cell.Bucket < first || cell.Bucket > last {
			continue
		}
		g := group{cell.Bucket, cell.Key}
		if by == ByModel {
			g.name = cell.Model
		}
		r := sums[g]
		if r == nil {
			r = &Row{Day: dayString(g.day), Name: g.name}
			sums[g] = r
		}
		r.Requests += cell.Requests
		r.PromptTok += cell.PromptTok
		r.CachedTok += cell.CachedTok
		r.CompletionTok += cell.CompletionTok
		r.Unmetered += cell.Unmetered
	}
	var rows []Row
	for _, r := range sums {
		rows = append(rows, *r)
	}
	slices.SortFunc(rows, func(a, b Row) int { return cmp.Or(cmp.Compare(a.Day, b.Day), cmp.Compare(a.Name, b.Name)) })
	return rows
}

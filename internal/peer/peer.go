// Package peer shares state between Pharos instances without consensus, and
// keeps it across restarts in the state file (ARCHITECTURE §12). It only
// moves bytes and schedules merges: each replicated package owns its merge.
//
// Every tick an instance POSTs each peer a Delta: its absolute gauges and the
// prefix ops since the last tick. A lost delta is repaired by the next one;
// only prefix ops are lossy, and those are hints. A peer that was unreachable
// and answers again (or is new) gets its Snapshot pulled and merged.
package peer

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
	"github.com/nvcnvn/pharos/internal/usage"
)

// Proto is the wire version. gob ignores unknown fields, so mixed versions
// interoperate during a rolling update; bump it only for a breaking change.
const Proto = 1

// Delta is POSTed to every peer each tick.
type Delta struct {
	Proto       uint16
	Origin      uint64
	Fingerprint uint64       // hash of the backend and key sets; a mismatch is flagged
	Leaving     bool         // sent on shutdown: drop my gauges now
	Gauges      sched.Gauges // this origin's full current inflight and waiters
	Prefix      []prefix.Op  // records and corrections since the last tick (lossy)
	Cells       []usage.Cell // this origin's usage: today, yesterday, this and last minute
}

// Snapshot is everything a new or reconnected instance needs. It is also the
// state file.
type Snapshot struct {
	Proto  uint16
	Prefix []prefix.Entry // most recent first
	Stats  []state.TargetStats
	Cells  []usage.Cell // Day cells of every origin, within retention
}

type Config struct {
	Origin      uint64   // random per process
	Secret      string   // shared bearer token; required
	Members     []string // host:port of every instance; may include this one
	DNS         string   // host:port re-resolved every 10 s, instead of Members
	Fingerprint uint64
	Client      *http.Client  // nil = 2 s timeout
	Tick        time.Duration // default 200 ms
	Now         func() time.Time
}

// originHeader carries the answering instance's origin, so an instance finds
// itself in the member list.
const originHeader = "Pharos-Origin"

// maxPending bounds the prefix ops queued for one peer; more are dropped.
const maxPending = 10_000

type Node struct {
	cfg Config
	st  *state.State
	sc  *sched.Sched
	px  *prefix.Index
	u   *usage.Counter
	fp  atomic.Uint64 // Config.Fingerprint, or the last SetFingerprint

	mu      sync.Mutex
	members map[string]*member // addr -> member
}

type member struct {
	addr string

	mu       sync.Mutex
	pending  []prefix.Op
	dropped  int
	self     bool
	up       bool
	pull     bool      // pull its snapshot on the next successful delta
	origin   uint64    // from its deltas or answers
	mismatch bool      // its fingerprint differs
	badProto bool      // it refused our last delta's protocol
	heard    time.Time // its last delta; zero = never
}

// Status is one member as this instance sees it.
type Status struct {
	Addr     string
	Self     bool
	Up       bool
	Mismatch bool // different backend set: routing may differ
	// ProtoMismatch: it refused our deltas for an incompatible wire
	// protocol, so the two instances don't share state.
	ProtoMismatch bool
	Dropped       int // prefix ops dropped because it was slow
	// Heard is when its last delta arrived; zero = never. Until the next one,
	// this instance's quotas and routing miss what the peer served since.
	Heard time.Time
}

func New(cfg Config, st *state.State, sc *sched.Sched, px *prefix.Index, u *usage.Counter) *Node {
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 2 * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Tick <= 0 {
		cfg.Tick = 200 * time.Millisecond
	}
	n := &Node{cfg: cfg, st: st, sc: sc, px: px, u: u, members: map[string]*member{}}
	n.fp.Store(cfg.Fingerprint)
	n.setMembers(cfg.Members)
	return n
}

func (n *Node) setMembers(addrs []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	next := map[string]*member{}
	for _, a := range addrs {
		if m, ok := n.members[a]; ok {
			next[a] = m
		} else {
			next[a] = &member{addr: a, pull: true}
		}
	}
	n.members = next
}

func (n *Node) list() []*member {
	n.mu.Lock()
	defer n.mu.Unlock()
	ms := make([]*member, 0, len(n.members))
	for _, a := range slices.Sorted(maps.Keys(n.members)) {
		ms = append(ms, n.members[a])
	}
	return ms
}

// SetFingerprint replaces the hash of the backend and key sets, after a
// config reload.
func (n *Node) SetFingerprint(fp uint64) { n.fp.Store(fp) }

// Push queues a prefix op for every peer. It never blocks on the network:
// the scheduler calls it under its lock.
func (n *Node) Push(op prefix.Op) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, m := range n.members {
		m.mu.Lock()
		if !m.self {
			if len(m.pending) < maxPending {
				m.pending = append(m.pending, op)
			} else {
				m.dropped++
			}
		}
		m.mu.Unlock()
	}
}

// Status reports every member, in address order.
func (n *Node) Status() []Status {
	var out []Status
	for _, m := range n.list() {
		m.mu.Lock()
		out = append(out, Status{Addr: m.addr, Self: m.self, Up: m.up, Mismatch: m.mismatch, ProtoMismatch: m.badProto, Dropped: m.dropped, Heard: m.heard})
		m.mu.Unlock()
	}
	return out
}

// Handler serves the peer endpoints. It belongs on the private peer listener.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /peer/delta", n.auth(n.delta))
	mux.HandleFunc("GET /peer/snapshot", n.auth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		if err := gob.NewEncoder(w).Encode(n.Snapshot()); err != nil {
			slog.Warn("peer snapshot", "to", r.RemoteAddr, "err", err)
		}
	}))
	return mux
}

func (n *Node) auth(h http.HandlerFunc) http.HandlerFunc {
	want := []byte("Bearer " + n.cfg.Secret)
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set(originHeader, strconv.FormatUint(n.cfg.Origin, 10))
		h(w, r)
	}
}

// errProto: the peer answered 409, its wire protocol isn't ours.
var errProto = errors.New("incompatible protocol")

// maxDelta bounds a delta body: gauges plus ~10k prefix ops.
const maxDelta = 64 << 20

func (n *Node) delta(w http.ResponseWriter, r *http.Request) {
	var d Delta
	if err := gob.NewDecoder(io.LimitReader(r.Body, maxDelta)).Decode(&d); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if d.Origin == n.cfg.Origin {
		return // our own delta: the sender learns from the header that it's us
	}
	if d.Proto != Proto {
		// Debug: the sender flags us and warns once; this runs every tick.
		slog.Debug("peer on an incompatible protocol ignored", "from", r.RemoteAddr, "proto", d.Proto, "want", Proto)
		http.Error(w, "incompatible protocol", http.StatusConflict)
		return
	}
	for _, m := range n.list() {
		m.mu.Lock()
		if m.origin == d.Origin {
			m.heard = n.cfg.Now()
			if mis := d.Fingerprint != n.fp.Load(); mis != m.mismatch {
				m.mismatch = mis
				if mis {
					slog.Warn("peer has a different backend or key set; routing and quotas may differ", "peer", m.addr)
				}
			}
		}
		m.mu.Unlock()
	}
	if d.Leaving {
		n.sc.Forget(d.Origin)
	} else {
		n.sc.Merge(d.Origin, d.Gauges)
	}
	n.px.Merge(d.Prefix, n.resolve)
	n.u.Merge(d.Cells)
}

func (n *Node) resolve(key string) (uint16, uint32, bool) {
	if t := n.st.TargetByKey(key); t != nil {
		return t.ID, t.Gen(), true
	}
	return 0, 0, false
}

// Snapshot returns this instance's prefix index, speed estimates and usage history.
func (n *Node) Snapshot() Snapshot {
	return Snapshot{
		Proto: Proto,
		Prefix: n.px.Export(func(id uint16) (string, uint32, bool) {
			if t := n.st.Target(id); t != nil {
				return t.Key, t.Gen(), true
			}
			return "", 0, false
		}),
		Stats: n.st.ExportStats(),
		Cells: n.u.Snapshot(),
	}
}

// Merge applies a snapshot from a peer or the state file. Merging is
// idempotent, so restoring from several sources is safe. Targets must be
// registered (a first scrape round done) for their prefix entries to count;
// stats wait for targets not seen yet.
func (n *Node) Merge(s Snapshot) {
	n.st.MergeStats(s.Stats)
	n.px.MergeEntries(s.Prefix, n.resolve)
	n.u.Merge(s.Cells)
}

// Restore pulls and merges a snapshot from every reachable member, within
// ctx. Members it couldn't reach are pulled once they answer.
func (n *Node) Restore(ctx context.Context) {
	var wg sync.WaitGroup
	for _, m := range n.list() {
		wg.Go(func() {
			if err := n.pull(ctx, m); err != nil {
				slog.Info("peer snapshot not restored; will pull when it answers", "peer", m.addr, "err", err)
			}
		})
	}
	wg.Wait()
}

func (n *Node) pull(ctx context.Context, m *member) error {
	req, err := n.request(ctx, http.MethodGet, m.addr, "/peer/snapshot", nil)
	if err != nil {
		return err
	}
	resp, err := n.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	if n.isSelf(m, resp) {
		return nil
	}
	var s Snapshot
	if err := gob.NewDecoder(resp.Body).Decode(&s); err != nil {
		return err
	}
	if s.Proto != Proto {
		return fmt.Errorf("incompatible protocol %d", s.Proto)
	}
	n.Merge(s)
	m.mu.Lock()
	m.pull = false
	m.mu.Unlock()
	return nil
}

// isSelf records the answering instance's origin and reports whether it is us.
func (n *Node) isSelf(m *member, resp *http.Response) bool {
	o, err := strconv.ParseUint(resp.Header.Get(originHeader), 10, 64)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		m.origin = o
		m.self = o == n.cfg.Origin
	}
	if m.self {
		m.pending, m.pull = nil, false
	}
	return m.self
}

func (n *Node) request(ctx context.Context, method, addr, path string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+n.cfg.Secret)
	return req, nil
}

// send POSTs one delta to m, and pulls its snapshot if it is new or answers
// again after failing.
func (n *Node) send(ctx context.Context, m *member, leaving bool) {
	m.mu.Lock()
	if m.self {
		m.mu.Unlock()
		return
	}
	ops := m.pending
	m.pending = nil
	m.mu.Unlock()
	// A leaving delta still carries usage, so a graceful shutdown loses none.
	d := Delta{Proto: Proto, Origin: n.cfg.Origin, Fingerprint: n.fp.Load(), Leaving: leaving, Prefix: ops, Cells: n.u.Export()}
	if !leaving {
		d.Gauges = n.sc.Export()
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		slog.Error("peer delta", "err", err)
		return
	}
	err := func() error {
		req, err := n.request(ctx, http.MethodPost, m.addr, "/peer/delta", buf.Bytes())
		if err != nil {
			return err
		}
		resp, err := n.cfg.Client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusConflict {
			return errProto
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		n.isSelf(m, resp)
		return nil
	}()
	m.mu.Lock()
	wasUp, pull, wasBad := m.up, m.pull, m.badProto
	m.up = err == nil
	if err != nil {
		m.pull = true // it may miss our deltas meanwhile, and we its
	}
	switch {
	case err == nil:
		m.badProto = false
	case errors.Is(err, errProto):
		m.badProto = true
	} // unreachable: keep what it last said
	self := m.self
	m.mu.Unlock()
	switch {
	case self:
	case errors.Is(err, errProto):
		if !wasBad {
			slog.Warn("peer runs an incompatible protocol; not sharing state with it", "peer", m.addr, "proto", Proto)
		}
	case err != nil && wasUp:
		slog.Warn("peer unreachable; serving on our own", "peer", m.addr, "err", err)
	case err == nil && !wasUp:
		slog.Info("peer up", "peer", m.addr)
	}
	if err == nil && pull && !self && !leaving {
		if err := n.pull(ctx, m); err != nil {
			slog.Warn("peer snapshot", "peer", m.addr, "err", err)
		}
	}
}

// Tick sends one delta to every member, in turn. Run does this on a timer,
// one goroutine per member; tests call it to step by hand.
func (n *Node) Tick(ctx context.Context) {
	for _, m := range n.list() {
		n.send(ctx, m, false)
	}
}

// Leave tells every member this instance is going, so they drop its gauges
// now instead of after 2 s of silence.
func (n *Node) Leave(ctx context.Context) {
	var wg sync.WaitGroup
	for _, m := range n.list() {
		wg.Go(func() { n.send(ctx, m, true) })
	}
	wg.Wait()
}

// Run sends deltas every tick until ctx is done: one goroutine per member, so
// a slow peer delays only itself. With DNS it re-resolves members every 10 s.
// ponytail: full mesh; fine for ≤ ~5 instances, switch to gossip if someone runs more.
func (n *Node) Run(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	running := map[*member]context.CancelFunc{}
	refresh := time.NewTicker(10 * time.Second)
	defer refresh.Stop()
	for {
		if n.cfg.DNS != "" {
			n.resolveDNS(ctx)
		}
		ms := n.list()
		for _, m := range ms {
			if running[m] == nil {
				mctx, cancel := context.WithCancel(ctx)
				running[m] = cancel
				wg.Go(func() {
					t := time.NewTicker(n.cfg.Tick)
					defer t.Stop()
					for {
						n.send(mctx, m, false)
						select {
						case <-mctx.Done():
							return
						case <-t.C:
						}
					}
				})
			}
		}
		for m, cancel := range running {
			if !slices.Contains(ms, m) {
				cancel()
				delete(running, m)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-refresh.C:
		}
	}
}

func (n *Node) resolveDNS(ctx context.Context) {
	host, port, err := net.SplitHostPort(n.cfg.DNS)
	if err != nil {
		slog.Error("peers.dns wants host:port", "dns", n.cfg.DNS)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		slog.Warn("peers.dns", "dns", n.cfg.DNS, "err", err)
		return // keep the last members
	}
	var addrs []string
	for _, ip := range ips {
		addrs = append(addrs, net.JoinHostPort(ip, port))
	}
	n.setMembers(addrs)
}

// WriteFile saves a snapshot atomically: a temp file, then a rename.
func WriteFile(path string, s Snapshot) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := gob.NewEncoder(f).Encode(s); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// ReadFile loads a snapshot written by WriteFile. A missing file is an empty
// snapshot.
func ReadFile(path string) (Snapshot, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return Snapshot{Proto: Proto}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	defer f.Close()
	var s Snapshot
	if err := gob.NewDecoder(f).Decode(&s); err != nil {
		return Snapshot{}, fmt.Errorf("%s: %w", path, err)
	}
	if s.Proto != Proto {
		return Snapshot{}, fmt.Errorf("%s: protocol %d, want %d", path, s.Proto, Proto)
	}
	return s, nil
}

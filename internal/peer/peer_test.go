package peer

// Layer 3, multi-instance: three Pharos instances in one process, each with
// its own state, scheduler, prefix index and proxy, sharing fake engines. Peer
// traffic goes over an in-memory transport that can cut an instance off, and
// ticks are stepped by hand.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/proxy"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type inst struct {
	name string
	st   *state.State
	sc   *sched.Sched
	px   *prefix.Index
	node *Node
	srv  *httptest.Server // the public API
}

type cluster struct {
	t     *testing.T
	c     *clock
	specs []state.BackendSpec

	mu     sync.Mutex
	insts  map[string]*inst
	cut    map[string]bool // instance cut off from the others (dead or partitioned)
	origin uint64
}

var members = []string{"a:8081", "b:8081", "c:8081"}

func newCluster(t *testing.T, specs ...state.BackendSpec) *cluster {
	cl := &cluster{t: t, c: &clock{now: time.Unix(1_790_000_000, 0)}, specs: specs, insts: map[string]*inst{}, cut: map[string]bool{}}
	for _, m := range members {
		cl.start(m, 1)
	}
	return cl
}

// start runs a fresh instance at addr (a restart, if one ran there): new
// origin, empty state. The state scrapes its first round.
func (cl *cluster) start(addr string, fingerprint uint64) *inst {
	var sc *sched.Sched
	var node *Node
	st := state.New(cl.specs, state.Options{Now: cl.c.Now, OnUpdate: func() { sc.Kick() }})
	px := prefix.New(1000)
	sc = sched.New(st, px, sched.Config{Policy: policy.Defaults, OnPrefix: func(op prefix.Op) { node.Push(op) }})
	cl.mu.Lock()
	cl.origin++
	origin := cl.origin
	cl.mu.Unlock()
	node = New(Config{Origin: origin, Secret: "s3cret", Members: members, Fingerprint: fingerprint,
		Client: &http.Client{Transport: transport{cl, addr}}}, st, sc, px)
	srv := httptest.NewServer(proxy.New(st, sc, nil))
	cl.t.Cleanup(srv.Close)
	in := &inst{addr, st, sc, px, node, srv}
	st.ScrapeAll(context.Background())
	cl.mu.Lock()
	cl.insts[addr] = in
	cl.mu.Unlock()
	return in
}

func (cl *cluster) get(addr string) *inst {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.insts[addr]
}

func (cl *cluster) setCut(addr string, cut bool) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.cut[addr] = cut
}

// transport delivers one instance's peer requests to another's handler.
type transport struct {
	cl   *cluster
	from string
}

func (tr transport) RoundTrip(r *http.Request) (*http.Response, error) {
	cl := tr.cl
	cl.mu.Lock()
	to, cut := cl.insts[r.URL.Host], cl.cut[tr.from] || cl.cut[r.URL.Host]
	cl.mu.Unlock()
	if to == nil || cut && tr.from != r.URL.Host {
		return nil, fmt.Errorf("dial %s: connection refused", r.URL.Host)
	}
	rec := httptest.NewRecorder()
	to.node.Handler().ServeHTTP(rec, r)
	return rec.Result(), nil
}

// tick sends one delta from every live instance.
func (cl *cluster) tick() {
	for _, m := range members {
		cl.mu.Lock()
		dead := cl.cut[m]
		cl.mu.Unlock()
		if !dead {
			cl.get(m).node.Tick(context.Background())
		}
	}
}

// round steps the clock one fast interval; every instance scrapes.
func (cl *cluster) round() {
	cl.c.Add(time.Second)
	for _, m := range members {
		cl.get(m).st.ScrapeAll(context.Background())
	}
}

func fake(t *testing.T, slots int) *fakeengine.Engine {
	f := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}, Slots: slots,
		CacheTokens: 100_000, PrefillSecTok: 1e-5, Speed: 1})
	t.Cleanup(f.Close)
	return f
}

var long = strings.Repeat("A long system prompt that every turn of this conversation repeats. ", 60)

func chatBody(msgs ...string) string {
	var m []map[string]string
	for i, content := range msgs {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		m = append(m, map[string]string{"role": role, "content": content})
	}
	b, _ := json.Marshal(map[string]any{"model": "m", "stream": true, "messages": m})
	return string(b)
}

func post(ctx context.Context, url, body string) (*http.Response, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	return http.DefaultClient.Do(req)
}

// chat sends a request through an instance and reads the whole reply.
func (in *inst) chat(t *testing.T, body string) {
	t.Helper()
	resp, err := post(context.Background(), in.srv.URL, body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: status %d", in.name, resp.StatusCode)
	}
}

// served returns which engine served the request chat just sent.
func served(t *testing.T, before []int, engines ...*fakeengine.Engine) int {
	t.Helper()
	for i, e := range engines {
		if e.Counters().Requests == before[i]+1 {
			return i
		}
	}
	t.Fatal("no engine served the request")
	return -1
}

func requests(engines ...*fakeengine.Engine) []int {
	var n []int
	for _, e := range engines {
		n = append(n, e.Counters().Requests)
	}
	return n
}

// eventually waits for cond without sleeping, failing after 5 s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func TestConversationFollowsItsPrefixAcrossInstances(t *testing.T) {
	x, y := fake(t, 4), fake(t, 4)
	cl := newCluster(t, state.BackendSpec{URL: x.URL()}, state.BackendSpec{URL: y.URL()})
	a, b := cl.get("a:8081"), cl.get("b:8081")
	// Without sync, turn 2 lands on the turn-1 engine by chance half the time;
	// four conversations in a row make a pass by chance 1 in 16.
	for i := range 4 {
		sys := fmt.Sprintf("%s conversation %d", long, i)
		before := requests(x, y)
		a.chat(t, chatBody(sys, "q1"))
		first := served(t, before, x, y)
		cl.tick()
		before = requests(x, y)
		b.chat(t, chatBody(sys, "q1", "a1", "q2"))
		if got := served(t, before, x, y); got != first {
			t.Fatalf("conversation %d: turn 1 on engine %d via a, turn 2 on engine %d via b", i, first, got)
		}
	}
}

func TestPeerRequestsHoldSlotsUntilThePeerGoesSilent(t *testing.T) {
	x := fake(t, 4)
	cl := newCluster(t, state.BackendSpec{URL: x.URL(), Capacity: 1})
	a, b := cl.get("a:8081"), cl.get("b:8081")

	// A request streams through a and holds the one slot.
	release := x.Hold()
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := post(ctx, a.srv.URL, chatBody("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	cl.tick()

	// a dies mid-stream: its client's stream fails, the engine stops, and a
	// never reports again.
	cl.setCut("a:8081", true)
	cancel()
	resp.Body.Close()
	if err := x.WaitFor(func(c fakeengine.Counters) bool { return c.Running == 0 }, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	release()

	// b still counts a's last report: its request waits rather than overcommit.
	done := make(chan int, 1)
	go func() {
		resp, err := post(context.Background(), b.srv.URL, chatBody("hello"))
		if err != nil {
			done <- 0
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		done <- resp.StatusCode
	}()
	eventually(t, "b's request queued", func() bool { return b.sc.Waiting() == 1 })
	cl.round() // +1 s: a silent for 1 s
	cl.tick()  // b hears from c, not from a
	if len(done) != 0 || x.Counters().Requests != 1 {
		t.Fatal("b used the slot a held before a went silent for 2 s")
	}
	cl.round() // +2 s
	cl.round() // +3 s: a's report no longer counts
	if code := <-done; code != http.StatusOK {
		t.Fatalf("b's request: status %d", code)
	}
}

func TestLeavingPeerIsDroppedAtOnce(t *testing.T) {
	x := fake(t, 4)
	cl := newCluster(t, state.BackendSpec{URL: x.URL(), Capacity: 1})
	a, b := cl.get("a:8081"), cl.get("b:8081")
	l, err := a.sc.Acquire(context.Background(), "", sched.Request{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	cl.tick()
	granted := make(chan *sched.Lease, 1)
	go func() {
		l, _ := b.sc.Acquire(context.Background(), "", sched.Request{Model: "m"})
		granted <- l
	}()
	eventually(t, "b's request queued", func() bool { return b.sc.Waiting() == 1 })
	l.Release(sched.Feedback{}) // a drains its last request, then leaves
	a.node.Leave(context.Background())
	select {
	case l := <-granted:
		l.Release(sched.Feedback{})
	case <-time.After(5 * time.Second):
		t.Fatal("b still counts a after a left")
	}
}

func TestRestartedInstanceRestoresFromPeers(t *testing.T) {
	x, y := fake(t, 4), fake(t, 4)
	cl := newCluster(t, state.BackendSpec{URL: x.URL()}, state.BackendSpec{URL: y.URL()})
	a := cl.get("a:8081")
	before := requests(x, y)
	a.chat(t, chatBody(long, "q1"))
	first := served(t, before, x, y)
	cl.tick()

	a = cl.start("a:8081", 1) // a restarts: new origin, empty index and stats
	a.node.Restore(context.Background())
	if a.px.Len() == 0 {
		t.Fatal("nothing restored")
	}
	for _, tg := range a.st.Targets("m") {
		if _, _, sv := tg.Estimates(); sv.OK != (tg.Backend.Spec.URL == []string{x.URL(), y.URL()}[first]) {
			t.Errorf("%s: service estimate %v after restore", tg.Key, sv)
		}
	}
	before = requests(x, y)
	a.chat(t, chatBody(long, "q1", "a1", "q2"))
	if got := served(t, before, x, y); got != first {
		t.Errorf("turn 2 after the restart went to engine %d, turn 1 to %d", got, first)
	}
	for _, s := range a.node.Status() {
		if s.Self != (s.Addr == "a:8081") {
			t.Errorf("status %+v", s)
		}
	}
}

func TestStateFileRestoresASingleInstance(t *testing.T) {
	x, y := fake(t, 4), fake(t, 4)
	cl := newCluster(t, state.BackendSpec{URL: x.URL()}, state.BackendSpec{URL: y.URL()})
	cl.setCut("b:8081", true)
	cl.setCut("c:8081", true)
	a := cl.get("a:8081")
	before := requests(x, y)
	a.chat(t, chatBody(long, "q1"))
	first := served(t, before, x, y)
	path := filepath.Join(t.TempDir(), "pharos.state")
	if err := WriteFile(path, a.node.Snapshot()); err != nil {
		t.Fatal(err)
	}

	a = cl.start("a:8081", 1)
	s, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	a.node.Merge(s)
	before = requests(x, y)
	a.chat(t, chatBody(long, "q1", "a1", "q2"))
	if got := served(t, before, x, y); got != first {
		t.Errorf("turn 2 after the restart went to engine %d, turn 1 to %d", got, first)
	}
	if s, err := ReadFile(filepath.Join(t.TempDir(), "none")); err != nil || len(s.Prefix) != 0 {
		t.Errorf("a missing state file: %v, %d entries", err, len(s.Prefix))
	}
}

func TestPartitionServesThenHeals(t *testing.T) {
	x, y := fake(t, 4), fake(t, 4)
	cl := newCluster(t, state.BackendSpec{URL: x.URL()}, state.BackendSpec{URL: y.URL()})
	a, b, c := cl.get("a:8081"), cl.get("b:8081"), cl.get("c:8081")
	cl.tick() // everyone has met everyone

	cl.setCut("a:8081", true) // a partitioned from b and c; all keep serving
	before := requests(x, y)
	a.chat(t, chatBody(long, "q1"))
	first := served(t, before, x, y)
	b.chat(t, chatBody("unrelated"))
	c.chat(t, chatBody("also unrelated"))
	for _, m := range members {
		cl.get(m).node.Tick(context.Background()) // a's ops to b and c are lost
	}
	if b.px.Len() != 2 { // its own and c's, not a's
		t.Fatalf("b's index has %d prefixes during the partition", b.px.Len())
	}

	cl.setCut("a:8081", false)
	cl.tick() // b and c reach a again and pull its snapshot
	before = requests(x, y)
	b.chat(t, chatBody(long, "q1", "a1", "q2"))
	if got := served(t, before, x, y); got != first {
		t.Errorf("after the heal: turn 2 via b on engine %d, turn 1 via a on %d", got, first)
	}
}

func TestMismatchedBackendSetIsFlagged(t *testing.T) {
	x := fake(t, 4)
	cl := newCluster(t, state.BackendSpec{URL: x.URL()})
	cl.start("c:8081", 2) // c runs another config
	cl.tick()
	cl.tick()
	for _, s := range cl.get("a:8081").node.Status() {
		if s.Mismatch != (s.Addr == "c:8081") || !s.Up {
			t.Errorf("a sees %+v", s)
		}
	}
}

func TestPeerEndpointsNeedTheSecret(t *testing.T) {
	x := fake(t, 4)
	cl := newCluster(t, state.BackendSpec{URL: x.URL()})
	h := cl.get("a:8081").node.Handler()
	for _, auth := range []string{"", "Bearer wrong"} {
		r := httptest.NewRequest(http.MethodGet, "/peer/snapshot", nil)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("%q: status %d", auth, w.Code)
		}
	}
}

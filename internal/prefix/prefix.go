// Package prefix remembers which targets recently served which prompt
// prefixes, as a chain of hashes at message boundaries (ARCHITECTURE §6). It
// stores hashes only, never prompt text.
package prefix

import (
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"
)

// Link is one step of a prompt's hash chain: the hash of everything up to and
// including one message (or block), and that prefix's length in bytes.
type Link struct {
	H     uint64
	Bytes int
}

// seed makes the hashes Pharos-specific. It is fixed, so hashes agree across
// instances and restarts. hash/maphash can't take a fixed seed, hence SHA-256.
const seed = "pharos-prefix-v1"

// Chain hashes a prompt: h0 = H(seed, model, preamble), hi = H(h(i-1), part_i).
// preamble is what templates render first (tools); parts are the messages
// (role and raw content) or fixed blocks of a plain prompt, in order.
func Chain(model string, preamble []byte, parts [][]byte) []Link {
	h := sha256.New()
	var sum [sha256.Size]byte
	next := func(prev []byte, fields ...[]byte) []byte {
		h.Reset()
		h.Write(prev)
		for _, f := range fields {
			var n [8]byte
			binary.LittleEndian.PutUint64(n[:], uint64(len(f)))
			h.Write(n[:]) // length-prefixed, so field boundaries can't shift
			h.Write(f)
		}
		return h.Sum(sum[:0])
	}
	prev := append([]byte(nil), next([]byte(seed), []byte(model), preamble)...)
	chain := make([]Link, 0, len(parts))
	n := len(preamble)
	for _, p := range parts {
		prev = append(prev[:0], next(prev, p)...)
		n += len(p)
		chain = append(chain, Link{H: binary.LittleEndian.Uint64(prev), Bytes: n})
	}
	return chain
}

// Blocks splits a plain prompt into fixed-size parts for Chain.
func Blocks(prompt []byte, size int) [][]byte {
	var parts [][]byte
	for len(prompt) > size {
		parts = append(parts, prompt[:size])
		prompt = prompt[size:]
	}
	if len(prompt) > 0 {
		parts = append(parts, prompt)
	}
	return parts
}

// slotsPerEntry bounds how many targets one prefix remembers (~100 B per entry).
const slotsPerEntry = 4

// Source is how a slot reached this index, so prediction accuracy can be
// measured per source (ARCHITECTURE §13).
type Source uint8

const (
	Local    Source = iota // this instance routed the request
	Peer                   // a peer's delta
	Restored               // a snapshot: a peer's at (re)connect, or the state file
)

func (s Source) String() string { return [...]string{"local", "peer", "restored"}[s] }

type slot struct {
	target uint16
	src    Source
	gen    uint32
	used   int64 // unix nanoseconds; 0 = empty
}

type entry struct {
	h     uint64
	slots [slotsPerEntry]slot
}

// Index maps prefix hashes to the targets that recently served them, bounded
// by an LRU. Safe for concurrent use.
// ponytail: one mutex; shard by hash if lock contention shows in benchmarks.
type Index struct {
	mu  sync.Mutex
	cap int
	m   map[uint64]*list.Element
	lru list.List // front = most recently recorded
}

// New returns an index holding at most capacity prefixes.
func New(capacity int) *Index {
	return &Index{cap: capacity, m: map[uint64]*list.Element{}}
}

// Match is a target's longest recently served prefix of a chain.
type Match struct {
	Bytes  int
	Source Source // of the slot that matched
}

// Lookup returns, per target, the longest prefix of chain it recently served.
// A slot recorded under an older generation of its target (the model was
// unloaded since) doesn't count.
func (x *Index) Lookup(chain []Link, gen func(target uint16) uint32) map[uint16]Match {
	x.mu.Lock()
	defer x.mu.Unlock()
	var matched map[uint16]Match
	for i := len(chain) - 1; i >= 0; i-- {
		el, ok := x.m[chain[i].H]
		if !ok {
			continue
		}
		for _, s := range el.Value.(*entry).slots {
			if s.used == 0 || s.gen != gen(s.target) {
				continue
			}
			if _, seen := matched[s.target]; !seen {
				if matched == nil {
					matched = map[uint16]Match{}
				}
				matched[s.target] = Match{chain[i].Bytes, s.src}
			}
		}
	}
	return matched
}

// Record notes that target, at generation gen, now holds every prefix in chain.
func (x *Index) Record(chain []Link, target uint16, gen uint32, now time.Time) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, l := range chain {
		x.put(l.H, target, gen, now.UnixNano(), Local)
	}
	x.trim()
}

// put moves h to the front and gives target a slot: its own, else an empty
// one, else the oldest, unless every slot is newer than used. A target's own
// slot keeps its newer time and that time's source; on a tie, its source.
func (x *Index) put(h uint64, target uint16, gen uint32, used int64, src Source) {
	el, ok := x.m[h]
	if ok {
		x.lru.MoveToFront(el)
	} else {
		el = x.lru.PushFront(&entry{h: h})
		x.m[h] = el
	}
	e := el.Value.(*entry)
	i := 0
	for j, s := range e.slots {
		if s.used != 0 && s.target == target {
			i = j
			if s.used >= used { // a tie is the same record arriving again
				used, src = s.used, s.src
			}
			break
		}
		if s.used < e.slots[i].used {
			i = j
		}
	}
	if e.slots[i].used != 0 && e.slots[i].target != target && e.slots[i].used > used {
		return
	}
	e.slots[i] = slot{target: target, src: src, gen: gen, used: used}
}

func (x *Index) trim() {
	for x.lru.Len() > x.cap {
		el := x.lru.Back()
		x.lru.Remove(el)
		delete(x.m, el.Value.(*entry).h)
	}
}

// Remove forgets target for every prefix in chain: the engine reported that it
// didn't hold them after all.
func (x *Index) Remove(chain []Link, target uint16) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, l := range chain {
		x.remove(l.H, target)
	}
}

func (x *Index) remove(h uint64, target uint16) {
	el, ok := x.m[h]
	if !ok {
		return
	}
	e := el.Value.(*entry)
	for j, s := range e.slots {
		if s.used != 0 && s.target == target {
			e.slots[j] = slot{}
		}
	}
}

// Hashes returns the hashes of chain, for an Op.
func Hashes(chain []Link) []uint64 {
	hs := make([]uint64, len(chain))
	for i, l := range chain {
		hs[i] = l.H
	}
	return hs
}

// Op is one record or correction, sent to peers. Targets are named by key,
// because IDs are local to an instance.
type Op struct {
	Target string
	Hashes []uint64
	Used   int64 // unix nanoseconds of a record
	Remove bool
}

// Entry is one prefix and the targets that recently served it, for peer
// snapshots and the state file.
type Entry struct {
	H     uint64
	Slots []EntrySlot
}

type EntrySlot struct {
	Target string
	Used   int64 // unix nanoseconds
}

// Resolver maps a target key to the local target ID and its current
// generation; ok=false for a target this instance doesn't know.
type Resolver func(key string) (id uint16, gen uint32, ok bool)

// Merge applies a peer's ops as local records and removals. Ops for targets
// this instance doesn't know are dropped. Merged slots take the target's
// current generation here: if the peer recorded just before an unload seen
// here, the entry looks fresh by mistake, which costs one miss until a
// correction removes it.
func (x *Index) Merge(ops []Op, resolve Resolver) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, op := range ops {
		id, gen, ok := resolve(op.Target)
		if !ok {
			continue
		}
		for _, h := range op.Hashes {
			if op.Remove {
				x.remove(h, id)
			} else {
				x.put(h, id, gen, op.Used, Peer)
			}
		}
	}
	x.trim()
}

// MergeEntries applies a snapshot's entries (most recent first, as Export
// returns them), keeping their order ahead of the entries already here.
func (x *Index) MergeEntries(entries []Entry, resolve Resolver) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for i := len(entries) - 1; i >= 0; i-- {
		for _, s := range entries[i].Slots {
			if id, gen, ok := resolve(s.Target); ok {
				x.put(entries[i].H, id, gen, s.Used, Restored)
			}
		}
	}
	x.trim()
}

// Export returns every entry, most recently recorded first, with each slot's
// target named by key. key returns a target's key and current generation;
// slots of unknown targets or of an older generation are left out.
func (x *Index) Export(key func(id uint16) (string, uint32, bool)) []Entry {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := make([]Entry, 0, x.lru.Len())
	for el := x.lru.Front(); el != nil; el = el.Next() {
		e := el.Value.(*entry)
		var slots []EntrySlot
		for _, s := range e.slots {
			if k, gen, ok := key(s.target); ok && s.used != 0 && s.gen == gen {
				slots = append(slots, EntrySlot{Target: k, Used: s.used})
			}
		}
		if slots != nil {
			out = append(out, Entry{H: e.h, Slots: slots})
		}
	}
	return out
}

// Len returns the number of prefixes held.
func (x *Index) Len() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.lru.Len()
}

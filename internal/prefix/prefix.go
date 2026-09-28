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

type slot struct {
	target uint16
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

// Lookup returns, per target, the length in bytes of the longest prefix of
// chain it recently served. A slot recorded under an older generation of its
// target (the model was unloaded since) doesn't count.
func (x *Index) Lookup(chain []Link, gen func(target uint16) uint32) map[uint16]int {
	x.mu.Lock()
	defer x.mu.Unlock()
	var matched map[uint16]int
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
					matched = map[uint16]int{}
				}
				matched[s.target] = chain[i].Bytes
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
		el, ok := x.m[l.H]
		if ok {
			x.lru.MoveToFront(el)
		} else {
			el = x.lru.PushFront(&entry{h: l.H})
			x.m[l.H] = el
		}
		e := el.Value.(*entry)
		i := 0 // the target's own slot, else an empty one, else the oldest
		for j, s := range e.slots {
			if s.used != 0 && s.target == target {
				i = j
				break
			}
			if s.used < e.slots[i].used {
				i = j
			}
		}
		e.slots[i] = slot{target: target, gen: gen, used: now.UnixNano()}
	}
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
		el, ok := x.m[l.H]
		if !ok {
			continue
		}
		e := el.Value.(*entry)
		for j, s := range e.slots {
			if s.used != 0 && s.target == target {
				e.slots[j] = slot{}
			}
		}
	}
}

// Len returns the number of prefixes held.
func (x *Index) Len() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.lru.Len()
}

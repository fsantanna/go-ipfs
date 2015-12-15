
// package publist implements an object for bitswap that contains the topics
// that a given peer published.
package publist

import (
	"sort"
	"sync"
	key "github.com/ipfs/go-ipfs/blocks/key"
	sl "github.com/ipfs/go-ipfs/exchange/bitswap/sublist"
)

type Pub struct {
	Topic sl.Topic
	Value key.Key
}

type ThreadSafe struct {
	lk	  sync.RWMutex
	Publist Publist
}

// not threadsafe
type Publist struct {
	set map[Pub]Entry
	// TODO provide O(1) len accessor if cost becomes an issue
}

type Entry struct {
	// TODO consider making entries immutable so they can be shared safely and
	// slices can be copied efficiently.
	Pub      Pub
	Priority int
}

type entrySlice []Entry

func (es entrySlice) Len() int		   { return len(es) }
func (es entrySlice) Swap(i, j int)	  { es[i], es[j] = es[j], es[i] }
func (es entrySlice) Less(i, j int) bool { return es[i].Priority > es[j].Priority }

func NewThreadSafe() *ThreadSafe {
	return &ThreadSafe{
		Publist: *New(),
	}
}

func New() *Publist {
	return &Publist{
		set: make(map[Pub]Entry),
	}
}

func (w *ThreadSafe) Add(k Pub, priority int) {
	// TODO rm defer for perf
	w.lk.Lock()
	defer w.lk.Unlock()
	w.Publist.Add(k, priority)
}

func (w *ThreadSafe) Remove(k Pub) {
	// TODO rm defer for perf
	w.lk.Lock()
	defer w.lk.Unlock()
	w.Publist.Remove(k)
}

func (w *ThreadSafe) Contains(k Pub) (Entry, bool) {
	// TODO rm defer for perf
	w.lk.RLock()
	defer w.lk.RUnlock()
	return w.Publist.Contains(k)
}

func (w *ThreadSafe) Entries() []Entry {
	w.lk.RLock()
	defer w.lk.RUnlock()
	return w.Publist.Entries()
}

func (w *ThreadSafe) SortedEntries() []Entry {
	w.lk.RLock()
	defer w.lk.RUnlock()
	return w.Publist.SortedEntries()
}

func (w *ThreadSafe) Len() int {
	w.lk.RLock()
	defer w.lk.RUnlock()
	return w.Publist.Len()
}

func (w *Publist) Len() int {
	return len(w.set)
}

func (w *Publist) Add(k Pub, priority int) {
	if _, ok := w.set[k]; ok {
		return
	}
	w.set[k] = Entry{
		Pub:	  k,
		Priority: priority,
	}
}

func (w *Publist) Remove(k Pub) {
	delete(w.set, k)
}

func (w *Publist) Contains(k Pub) (Entry, bool) {
	e, ok := w.set[k]
	return e, ok
}

func (w *Publist) Entries() []Entry {
	var es entrySlice
	for _, e := range w.set {
		es = append(es, e)
	}
	return es
}

func (w *Publist) SortedEntries() []Entry {
	var es entrySlice
	for _, e := range w.set {
		es = append(es, e)
	}
	sort.Sort(es)
	return es
}

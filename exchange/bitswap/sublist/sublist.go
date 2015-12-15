
// package sublist implements an object for bitswap that contains the topics
// that a given peer subscribes.
package sublist

import (
	"sort"
	"sync"
)

type Topic string

type ThreadSafe struct {
	lk	  sync.RWMutex
	Sublist Sublist
}

// not threadsafe
type Sublist struct {
	set map[Topic]Entry
	// TODO provide O(1) len accessor if cost becomes an issue
}

type Entry struct {
	// TODO consider making entries immutable so they can be shared safely and
	// slices can be copied efficiently.
	Topic	Topic
	Priority int
}

type entrySlice []Entry

func (es entrySlice) Len() int		   { return len(es) }
func (es entrySlice) Swap(i, j int)	  { es[i], es[j] = es[j], es[i] }
func (es entrySlice) Less(i, j int) bool { return es[i].Priority > es[j].Priority }

func NewThreadSafe() *ThreadSafe {
	return &ThreadSafe{
		Sublist: *New(),
	}
}

func New() *Sublist {
	return &Sublist{
		set: make(map[Topic]Entry),
	}
}

func (w *ThreadSafe) Add(k Topic, priority int) {
	// TODO rm defer for perf
	w.lk.Lock()
	defer w.lk.Unlock()
	w.Sublist.Add(k, priority)
}

func (w *ThreadSafe) Remove(k Topic) {
	// TODO rm defer for perf
	w.lk.Lock()
	defer w.lk.Unlock()
	w.Sublist.Remove(k)
}

func (w *ThreadSafe) Contains(k Topic) (Entry, bool) {
	// TODO rm defer for perf
	w.lk.RLock()
	defer w.lk.RUnlock()
	return w.Sublist.Contains(k)
}

func (w *ThreadSafe) Entries() []Entry {
	w.lk.RLock()
	defer w.lk.RUnlock()
	return w.Sublist.Entries()
}

func (w *ThreadSafe) SortedEntries() []Entry {
	w.lk.RLock()
	defer w.lk.RUnlock()
	return w.Sublist.SortedEntries()
}

func (w *ThreadSafe) Len() int {
	w.lk.RLock()
	defer w.lk.RUnlock()
	return w.Sublist.Len()
}

func (w *Sublist) Len() int {
	return len(w.set)
}

func (w *Sublist) Add(k Topic, priority int) {
	if _, ok := w.set[k]; ok {
		return
	}
	w.set[k] = Entry{
		Topic:	  k,
		Priority: priority,
	}
}

func (w *Sublist) Remove(k Topic) {
	delete(w.set, k)
}

func (w *Sublist) Contains(k Topic) (Entry, bool) {
	e, ok := w.set[k]
	return e, ok
}

func (w *Sublist) Entries() []Entry {
	var es entrySlice
	for _, e := range w.set {
		es = append(es, e)
	}
	return es
}

func (w *Sublist) SortedEntries() []Entry {
	var es entrySlice
	for _, e := range w.set {
		es = append(es, e)
	}
	sort.Sort(es)
	return es
}

package decision

import (
	"time"

	key "github.com/ipfs/go-ipfs/blocks/key"
	wl "github.com/ipfs/go-ipfs/exchange/bitswap/wantlist"
	pl "github.com/ipfs/go-ipfs/exchange/bitswap/publist"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
)

// keySet is just a convenient alias for maps of keys, where we only care
// access/lookups.
type keySet map[key.Key]struct{}

func newLedger(p peer.ID) *ledger {
	return &ledger{
		wantList:   wl.New(),
		pubList:    pl.New(),
		Partner:    p,
		sentToPeer: make(map[key.Key]time.Time),
	}
}

// ledger stores the data exchange relationship between two peers.
// NOT threadsafe
type ledger struct {
	// Partner is the remote Peer.
	Partner peer.ID

	// Accounting tracks bytes sent and recieved.
	Accounting debtRatio

	// firstExchnage is the time of the first data exchange.
	firstExchange time.Time

	// lastExchange is the time of the last data exchange.
	lastExchange time.Time

	// exchangeCount is the number of exchanges with this peer
	exchangeCount uint64

	// wantList is a (bounded, small) set of keys that Partner desires.
	wantList *wl.Wantlist

	// pubList is a (bounded, small) set of topics that Partner published.
	pubList *pl.Publist

	// sentToPeer is a set of keys to ensure we dont send duplicate blocks
	// to a given peer
	sentToPeer map[key.Key]time.Time
}

type debtRatio struct {
	BytesSent uint64
	BytesRecv uint64
}

func (dr *debtRatio) Value() float64 {
	return float64(dr.BytesSent) / float64(dr.BytesRecv+1)
}

func (l *ledger) SentBytes(n int) {
	l.exchangeCount++
	l.lastExchange = time.Now()
	l.Accounting.BytesSent += uint64(n)
}

func (l *ledger) ReceivedBytes(n int) {
	l.exchangeCount++
	l.lastExchange = time.Now()
	l.Accounting.BytesRecv += uint64(n)
}

// TODO: this needs to be different. We need timeouts.
func (l *ledger) Wants(k key.Key, priority int) {
	log.Debugf("peer %s wants %s", l.Partner, k)
	l.wantList.Add(k, priority)
}

func (l *ledger) CancelWant(k key.Key) {
	l.wantList.Remove(k)
}

func (l *ledger) WantListContains(k key.Key) (wl.Entry, bool) {
	return l.wantList.Contains(k)
}

// TODO: this needs to be different. We need timeouts.
func (l *ledger) Pubs(k pl.Pub, priority int) {
	log.Debugf("peer %s pubs %s", l.Partner, k)
	l.pubList.Add(k, priority)
}

func (l *ledger) CancelPub(k pl.Pub) {
	l.pubList.Remove(k)
}

func (l *ledger) PubListContains(k pl.Pub) (pl.Entry, bool) {
	return l.pubList.Contains(k)
}

func (l *ledger) ExchangeCount() uint64 {
	return l.exchangeCount
}

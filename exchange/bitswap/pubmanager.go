package bitswap

import (
    //"sync"
    "fmt"
	"time"

	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	engine "github.com/ipfs/go-ipfs/exchange/bitswap/decision"
	bsmsg "github.com/ipfs/go-ipfs/exchange/bitswap/message"
	bsnet "github.com/ipfs/go-ipfs/exchange/bitswap/network"
	publist "github.com/ipfs/go-ipfs/exchange/bitswap/publist"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
)

type PubManager struct {
	// sync channels for Run loop
    incoming   chan []*bsmsg.EntryPub
	connect	chan peer.ID // notification channel for new peers connecting
	disconnect chan peer.ID // notification channel for peers disconnecting

	// synchronized by Run loop, only touch inside there
	peers map[peer.ID]*msgQueue
	pl	*publist.ThreadSafe

	network bsnet.BitSwapNetwork
	ctx	 context.Context
}

func NewPubManager(ctx context.Context, network bsnet.BitSwapNetwork) *PubManager {
	return &PubManager{
        incoming:   make(chan []*bsmsg.EntryPub, 10),
		connect:	make(chan peer.ID, 10),
		disconnect: make(chan peer.ID, 10),
		peers:	  make(map[peer.ID]*msgQueue),
		pl:		 publist.NewThreadSafe(),
		network:	network,
		ctx:		ctx,
	}
}

/*
type msgPair struct {
	to  peer.ID
	msg bsmsg.BitSwapMessage
}

type cancellation struct {
	who peer.ID
	blk publist.Topic
}

type msgQueue struct {
	p peer.ID

	outlk   sync.Mutex
	out	 bsmsg.BitSwapMessage
	network bsnet.BitSwapNetwork

	refcnt int

	work chan struct{}
	done chan struct{}
}
*/

func (pm *PubManager) PubPubs(ks []publist.Pub) {
	log.Infof("pub blocks: %s", ks)
	pm.addEntries(ks, false)
}

func (pm *PubManager) CancelPubs(ks []publist.Pub) {
	pm.addEntries(ks, true)
}

func (pm *PubManager) addEntries(ks []publist.Pub, cancel bool) {
    var entries []*bsmsg.EntryPub
	for i, k := range ks {
        entries = append(entries, &bsmsg.EntryPub{
            Cancel: cancel,
            Entry: publist.Entry{
				Pub:	  k,
				Priority: kMaxPriority - i,
			},
		})
	}
fmt.Printf("PUB-1 %v\n", entries);
	select {
	case pm.incoming <- entries:
	case <-pm.ctx.Done():
	}
}

func (pm *PubManager) SendBlock(ctx context.Context, env *engine.Envelope) {
	// Blocks need to be sent synchronously to maintain proper backpressure
	// throughout the network stack
	defer env.Sent()

	msg := bsmsg.New(false)
	msg.AddBlock(env.Block)
	log.Infof("Sending block %s to %s", env.Peer, env.Block)
	err := pm.network.SendMessage(ctx, env.Peer, msg)
	if err != nil {
		log.Infof("sendblock error: %s", err)
	}
}

func (pm *PubManager) startPeerHandler(p peer.ID) *msgQueue {
	mq, ok := pm.peers[p]
	if ok {
		mq.refcnt++
		return nil
	}

	mq = pm.newMsgQueue(p)

	// new peer, we will pub to give them our full publist
	fullpublist := bsmsg.New(true)
	for _, e := range pm.pl.Entries() {
		fullpublist.AddEntryPub(e.Pub, e.Priority)
	}
	mq.out = fullpublist
	mq.work <- struct{}{}

	pm.peers[p] = mq
    go mq.runQueuePub(pm.ctx)
	return mq
}

func (pm *PubManager) stopPeerHandler(p peer.ID) {
	pq, ok := pm.peers[p]
	if !ok {
		// TODO: log error?
		return
	}

	pq.refcnt--
	if pq.refcnt > 0 {
		return
	}

	close(pq.done)
	delete(pm.peers, p)
}

func (mq *msgQueue) runQueuePub(ctx context.Context) {
	for {
		select {
		case <-mq.work: // there is work to be done
            mq.doWorkPub(ctx)
		case <-mq.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (mq *msgQueue) doWorkPub(ctx context.Context) {
	// allow ten minutes for connections
	// this includes looking them up in the dht
	// dialing them, and handshaking
	conctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	err := mq.network.ConnectTo(conctx, mq.p)
	if err != nil {
		log.Infof("cant connect to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		return
	}

	// grab outgoing message
	mq.outlk.Lock()
	wlm := mq.out
	if wlm == nil || wlm.Empty() {
		mq.outlk.Unlock()
		return
	}
	mq.out = nil
	mq.outlk.Unlock()

	sendctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// send publist updates
	err = mq.network.SendMessage(sendctx, mq.p, wlm)
	if err != nil {
		log.Infof("bitswap send error: %s", err)
		// TODO: what do we do if this fails?
		return
	}
}

func (pm *PubManager) Connected(p peer.ID) {
	select {
	case pm.connect <- p:
	case <-pm.ctx.Done():
	}
}

func (pm *PubManager) Disconnected(p peer.ID) {
	select {
	case pm.disconnect <- p:
	case <-pm.ctx.Done():
	}
}

// TODO: use goprocess here once i trust it
func (pm *PubManager) Run() {
	tock := time.NewTicker(rebroadcastDelay.Get())
	defer tock.Stop()
	for {
		select {
		case entries := <-pm.incoming:

			// add changes to our publist
			for _, e := range entries {
				if e.Cancel {
					pm.pl.Remove(e.Pub)
				} else {
fmt.Printf("PUB-2 %v\n", e);
					pm.pl.Add(e.Pub, e.Priority)
				}
			}

			// broadcast those publist changes
fmt.Printf("PUB-PEERS-2 %v\n", entries);
			for k, p := range pm.peers {
fmt.Printf("\t %v\n", k.Pretty());
                p.addMessagePub(entries)
			}

		case <-tock.C:
			// resend entire publist every so often (REALLY SHOULDNT BE NECESSARY)
            var es []*bsmsg.EntryPub
			for _, e := range pm.pl.Entries() {
                es = append(es, &bsmsg.EntryPub{Entry: e})
			}
			for _, p := range pm.peers {
				p.outlk.Lock()
				p.out = bsmsg.New(true)
				p.outlk.Unlock()

                p.addMessagePub(es)
			}
		case p := <-pm.connect:
			pm.startPeerHandler(p)
		case p := <-pm.disconnect:
			pm.stopPeerHandler(p)
		case <-pm.ctx.Done():
			return
		}
	}
}

func (wm *PubManager) newMsgQueue(p peer.ID) *msgQueue {
	mq := new(msgQueue)
	mq.done = make(chan struct{})
	mq.work = make(chan struct{}, 1)
	mq.network = wm.network
	mq.p = p
	mq.refcnt = 1

	return mq
}

func (mq *msgQueue) addMessagePub(entries []*bsmsg.EntryPub) {
    mq.outlk.Lock()
    defer func() {
		mq.outlk.Unlock()
		select {
		case mq.work <- struct{}{}:
		default:
		}
	}()

	// if we have no message held, or the one we are given is full
	// overwrite the one we are holding
	if mq.out == nil {
		mq.out = bsmsg.New(false)
	}

	// TODO: add a msg.Combine(...) method
	// otherwise, combine the one we are holding with the
	// one passed in
	for _, e := range entries {
		if e.Cancel {
			mq.out.CancelPub(e.Pub)
		} else {
			mq.out.AddEntryPub(e.Pub, e.Priority)
		}
	}
}

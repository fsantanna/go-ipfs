package bitswap

import (
    //"sync"
    "fmt"
	"time"

	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	engine "github.com/ipfs/go-ipfs/exchange/bitswap/decision"
	bsmsg "github.com/ipfs/go-ipfs/exchange/bitswap/message"
	bsnet "github.com/ipfs/go-ipfs/exchange/bitswap/network"
	sublist "github.com/ipfs/go-ipfs/exchange/bitswap/sublist"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
)

type SubManager struct {
	// sync channels for Run loop
    incoming   chan []*bsmsg.EntrySub
	connect	chan peer.ID // notification channel for new peers connecting
	disconnect chan peer.ID // notification channel for peers disconnecting

	// synchronized by Run loop, only touch inside there
	peers map[peer.ID]*msgQueue
	sl	*sublist.ThreadSafe

	network bsnet.BitSwapNetwork
	ctx	 context.Context
}

func NewSubManager(ctx context.Context, network bsnet.BitSwapNetwork) *SubManager {
	return &SubManager{
        incoming:   make(chan []*bsmsg.EntrySub, 10),
		connect:	make(chan peer.ID, 10),
		disconnect: make(chan peer.ID, 10),
		peers:	  make(map[peer.ID]*msgQueue),
		sl:		 sublist.NewThreadSafe(),
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
	blk sublist.Topic
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

func (pm *SubManager) SubTopics(ks []sublist.Topic) {
	log.Infof("sub blocks: %s", ks)
	pm.addEntries(ks, false)
}

func (pm *SubManager) CancelSubs(ks []sublist.Topic) {
	pm.addEntries(ks, true)
}

func (pm *SubManager) addEntries(ks []sublist.Topic, cancel bool) {
    var entries []*bsmsg.EntrySub
	for i, k := range ks {
        entries = append(entries, &bsmsg.EntrySub{
            Cancel: cancel,
            Entry: sublist.Entry{
                Topic:	  k,
				Priority: kMaxPriority - i,
			},
		})
	}
fmt.Printf("ADD-1 %v\n", entries);
	select {
	case pm.incoming <- entries:
	case <-pm.ctx.Done():
	}
}

func (pm *SubManager) SendBlock(ctx context.Context, env *engine.Envelope) {
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

func (pm *SubManager) startPeerHandler(p peer.ID) *msgQueue {
	mq, ok := pm.peers[p]
	if ok {
		mq.refcnt++
		return nil
	}

	mq = pm.newMsgQueue(p)

	// new peer, we will sub to give them our full sublist
	fullsublist := bsmsg.New(true)
	for _, e := range pm.sl.Entries() {
        fullsublist.AddEntrySub(e.Topic, e.Priority)
	}
	mq.out = fullsublist
	mq.work <- struct{}{}

	pm.peers[p] = mq
    go mq.runQueueSub(pm.ctx)
	return mq
}

func (pm *SubManager) stopPeerHandler(p peer.ID) {
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

func (mq *msgQueue) runQueueSub(ctx context.Context) {
	for {
		select {
		case <-mq.work: // there is work to be done
            mq.doWorkSub(ctx)
		case <-mq.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (mq *msgQueue) doWorkSub(ctx context.Context) {
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

	// send sublist updates
	err = mq.network.SendMessage(sendctx, mq.p, wlm)
	if err != nil {
		log.Infof("bitswap send error: %s", err)
		// TODO: what do we do if this fails?
		return
	}
}

func (pm *SubManager) Connected(p peer.ID) {
	select {
	case pm.connect <- p:
	case <-pm.ctx.Done():
	}
}

func (pm *SubManager) Disconnected(p peer.ID) {
	select {
	case pm.disconnect <- p:
	case <-pm.ctx.Done():
	}
}

// TODO: use goprocess here once i trust it
func (pm *SubManager) Run() {
	tock := time.NewTicker(rebroadcastDelay.Get())
	defer tock.Stop()
	for {
		select {
		case entries := <-pm.incoming:

			// add changes to our sublist
			for _, e := range entries {
				if e.Cancel {
                    pm.sl.Remove(e.Topic)
				} else {
fmt.Printf("ADD-2 %v\n", e);
                    pm.sl.Add(e.Topic, e.Priority)
				}
			}

			// broadcast those sublist changes
fmt.Printf("PEERS-2 %v\n", entries);
			for k, p := range pm.peers {
fmt.Printf("\t %v\n", k.Pretty());
                p.addMessageSub(entries)
			}

		case <-tock.C:
			// resend entire sublist every so often (REALLY SHOULDNT BE NECESSARY)
            var es []*bsmsg.EntrySub
			for _, e := range pm.sl.Entries() {
                es = append(es, &bsmsg.EntrySub{Entry: e})
			}
			for _, p := range pm.peers {
				p.outlk.Lock()
				p.out = bsmsg.New(true)
				p.outlk.Unlock()

                p.addMessageSub(es)
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

func (wm *SubManager) newMsgQueue(p peer.ID) *msgQueue {
	mq := new(msgQueue)
	mq.done = make(chan struct{})
	mq.work = make(chan struct{}, 1)
	mq.network = wm.network
	mq.p = p
	mq.refcnt = 1

	return mq
}

func (mq *msgQueue) addMessageSub(entries []*bsmsg.EntrySub) {
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

fmt.Printf("XXX-3 %v\n", entries);
	// TODO: add a msg.Combine(...) method
	// otherwise, combine the one we are holding with the
	// one passed in
	for _, e := range entries {
		if e.Cancel {
fmt.Printf("REM-3 %v\n", e);
            mq.out.CancelSub(e.Topic)
		} else {
fmt.Printf("ADD-3 %v\n", e);
            mq.out.AddEntrySub(e.Topic, e.Priority)
		}
	}
}

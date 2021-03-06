package corerepo

import (
	"errors"
	"time"

	humanize "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/dustin/go-humanize"
	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	key "github.com/ipfs/go-ipfs/blocks/key"
	"github.com/ipfs/go-ipfs/core"
	repo "github.com/ipfs/go-ipfs/repo"
	logging "github.com/ipfs/go-ipfs/vendor/QmQg1J6vikuXF9oDvm4wpdeAUvvkVEKW1EYDw9HhTMnP2b/go-log"
)

var log = logging.Logger("corerepo")

var ErrMaxStorageExceeded = errors.New("Maximum storage limit exceeded. Maybe unpin some files?")

type KeyRemoved struct {
	Key key.Key
}

type GC struct {
	Node       *core.IpfsNode
	Repo       repo.Repo
	StorageMax uint64
	StorageGC  uint64
	SlackGB    uint64
	Storage    uint64
}

func NewGC(n *core.IpfsNode) (*GC, error) {
	r := n.Repo
	cfg, err := r.Config()
	if err != nil {
		return nil, err
	}

	// check if cfg has these fields initialized
	// TODO: there should be a general check for all of the cfg fields
	// maybe distinguish between user config file and default struct?
	if cfg.Datastore.StorageMax == "" {
		r.SetConfigKey("Datastore.StorageMax", "10GB")
		cfg.Datastore.StorageMax = "10GB"
	}
	if cfg.Datastore.StorageGCWatermark == 0 {
		r.SetConfigKey("Datastore.StorageGCWatermark", 90)
		cfg.Datastore.StorageGCWatermark = 90
	}

	storageMax, err := humanize.ParseBytes(cfg.Datastore.StorageMax)
	if err != nil {
		return nil, err
	}
	storageGC := storageMax * uint64(cfg.Datastore.StorageGCWatermark) / 100

	// calculate the slack space between StorageMax and StorageGCWatermark
	// used to limit GC duration
	slackGB := (storageMax - storageGC) / 10e9
	if slackGB < 1 {
		slackGB = 1
	}

	return &GC{
		Node:       n,
		Repo:       r,
		StorageMax: storageMax,
		StorageGC:  storageGC,
		SlackGB:    slackGB,
	}, nil
}

func GarbageCollect(n *core.IpfsNode, ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // in case error occurs during operation
	keychan, err := n.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}
	for k := range keychan { // rely on AllKeysChan to close chan
		if !n.Pinning.IsPinned(k) {
			if err := n.Blockstore.DeleteBlock(k); err != nil {
				return err
			}
		}
	}
	return nil
}

func GarbageCollectAsync(n *core.IpfsNode, ctx context.Context) (<-chan *KeyRemoved, error) {

	keychan, err := n.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}

	output := make(chan *KeyRemoved)
	go func() {
		defer close(output)
		for {
			select {
			case k, ok := <-keychan:
				if !ok {
					return
				}
				if !n.Pinning.IsPinned(k) {
					err := n.Blockstore.DeleteBlock(k)
					if err != nil {
						log.Debugf("Error removing key from blockstore: %s", err)
						continue
					}
					select {
					case output <- &KeyRemoved{k}:
					case <-ctx.Done():
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return output, nil
}

func PeriodicGC(ctx context.Context, node *core.IpfsNode) error {
	cfg, err := node.Repo.Config()
	if err != nil {
		return err
	}

	if cfg.Datastore.GCPeriod == "" {
		node.Repo.SetConfigKey("Datastore.GCPeriod", "1h")
		cfg.Datastore.GCPeriod = "1h"
	}

	period, err := time.ParseDuration(cfg.Datastore.GCPeriod)
	if err != nil {
		return err
	}
	if int64(period) == 0 {
		// if duration is 0, it means GC is disabled.
		return nil
	}

	gc, err := NewGC(node)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(period):
			// the private func maybeGC doesn't compute storageMax, storageGC, slackGC so that they are not re-computed for every cycle
			if err := gc.maybeGC(ctx, 0); err != nil {
				log.Error(err)
			}
		}
	}
}

func ConditionalGC(ctx context.Context, node *core.IpfsNode, offset uint64) error {
	gc, err := NewGC(node)
	if err != nil {
		return err
	}
	return gc.maybeGC(ctx, offset)
}

func (gc *GC) maybeGC(ctx context.Context, offset uint64) error {
	storage, err := gc.Repo.GetStorageUsage()
	if err != nil {
		return err
	}

	if storage+offset > gc.StorageGC {
		if storage+offset > gc.StorageMax {
			log.Warningf("pre-GC: %s", ErrMaxStorageExceeded)
		}

		// Do GC here
		log.Info("Watermark exceeded. Starting repo GC...")
		defer log.EventBegin(ctx, "repoGC").Done()
		// 1 minute is sufficient for ~1GB unlink() blocks each of 100kb in SSD
		_ctx, cancel := context.WithTimeout(ctx, time.Duration(gc.SlackGB)*time.Minute)
		defer cancel()

		if err := GarbageCollect(gc.Node, _ctx); err != nil {
			return err
		}
		newStorage, err := gc.Repo.GetStorageUsage()
		if err != nil {
			return err
		}
		log.Infof("Repo GC done. Released %s\n", humanize.Bytes(uint64(storage-newStorage)))
		if newStorage > gc.StorageGC {
			log.Warningf("post-GC: Watermark still exceeded")
			if newStorage > gc.StorageMax {
				err := ErrMaxStorageExceeded
				log.Error(err)
				return err
			}
		}
	}
	return nil
}

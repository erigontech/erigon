package lightclient

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
)

func (l *LightClient) BootstrapCheckpoint(ctx context.Context, finalized [32]byte) error {
	log.Info("[Checkpoint Sync] Retrieving lightclient bootstrap from sentinel",
		"root", common.Bytes2Hex(finalized[:]))
	retryInterval := time.NewTicker(200 * time.Millisecond)
	defer retryInterval.Stop()

	logInterval := time.NewTicker(10 * time.Second)
	defer logInterval.Stop()

	doneLogCh := make(chan struct{})
	var (
		b   *cltypes.LightClientBootstrap
		err error
	)
	// Start log go routine.

	go func() {
		for {
			select {
			case <-logInterval.C:
				peers, err := l.rpc.Peers()
				if err != nil {
					continue
				}
				log.Info("[Checkpoint Sync] P2P", "peers", peers)
			case <-doneLogCh:
				return
			}
		}
	}()

	for b == nil {
		b, err = l.rpc.SendLightClientBootstrapReqV1(finalized)
		if err != nil {
			log.Debug("[Checkpoint Sync] could not retrieve bootstrap", "err", err)
		}
	}

	s, err := NewLightClientStore(finalized, b)
	if err != nil {
		log.Warn("[Checkpoint Sync] could not create/validate store", "err", err)
		return err
	}

	l.store = s
	log.Info("Store Initialized successfully",
		"slot", l.store.finalizedHeader.Slot,
		"root", common.Bytes2Hex(l.store.finalizedHeader.Root[:]))
	return nil
}

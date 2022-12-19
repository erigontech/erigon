package lightclient

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
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

	b, err := rpc.SendLightClientBootstrapReqV1(ctx, &cltypes.SingleRoot{
		Root: finalized,
	}, l.sentinel)
	if err != nil {
		log.Trace("[Checkpoint Sync] could not retrieve bootstrap", "err", err)
		return err
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

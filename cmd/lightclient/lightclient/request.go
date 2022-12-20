package lightclient

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/log/v3"
)

func (l *LightClient) FetchUpdate(ctx context.Context, period uint64) (*cltypes.LightClientUpdate, error) {
	log.Info("[Lightclient] Fetching Sync Committee Period", "period", period)
	retryInterval := time.NewTicker(200 * time.Millisecond)
	defer retryInterval.Stop()

	logInterval := time.NewTicker(10 * time.Second)
	defer logInterval.Stop()

	update, err := rpc.SendLightClientUpdatesReqV1(ctx, period, l.sentinel)
	if err != nil {
		log.Trace("[Checkpoint Sync] could not retrieve bootstrap", "err", err)
		return nil, err
	}

	return update, nil

}

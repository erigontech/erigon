package lightclient

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/log/v3"
)

func (l *LightClient) FetchUpdate(ctx context.Context, period uint64, count uint64) ([]*cltypes.LightClientUpdate, error) {
	log.Info("[Lightclient] Fetching Sync Committee Period", "period", period)
	retryInterval := time.NewTicker(200 * time.Millisecond)
	defer retryInterval.Stop()

	logInterval := time.NewTicker(10 * time.Second)
	defer logInterval.Stop()

	var store atomic.Value
	for store.Load() == nil {
		select {
		case <-logInterval.C:
			peers, err := l.sentinel.GetPeers(ctx, &lightrpc.EmptyRequest{})
			if err != nil {
				return nil, err
			}
			log.Info("[LightClient] Fetching Sync Committee Period", "peers", peers.Amount)
		case <-retryInterval.C:
			// Async request
			go func() {
				updates, err := rpc.SendLightClientUpdatesReqV1(ctx, period, count, l.sentinel)
				if err != nil {
					log.Warn("oops", "err", err)
					log.Trace("[Checkpoint Sync] could not retrieve bootstrap", "err", err)
					return
				}
				if updates == nil {
					return
				}

				store.Store(updates)
			}()
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled")
		}
	}
	return store.Load().([]*cltypes.LightClientUpdate), nil

}

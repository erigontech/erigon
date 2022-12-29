package lightclient

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/log/v3"
)

func (l *LightClient) FetchUpdate(ctx context.Context, period uint64) (*cltypes.LightClientUpdate, error) {
	log.Info("[Lightclient] Fetching Sync Committee Period", "period", period)
	var (
		update *cltypes.LightClientUpdate
		err    error
	)
	for update == nil {
		update, err = l.rpc.SendLightClientUpdatesReqV1(period)
		if err != nil {
			log.Trace("[Checkpoint Sync] could not retrieve bootstrap", "err", err)
			return nil, err
		}
	}

	return update, nil

}

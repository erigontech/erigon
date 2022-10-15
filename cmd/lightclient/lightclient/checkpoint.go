package lightclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
)

func RetrieveBeaconState(ctx context.Context, uri string) (*cltypes.BeaconState, error) {
	log.Info("[Checkpoint Sync] Requesting beacon state", "uri", uri)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync failed %s", err)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = r.Body.Close()
	}()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("checkpoint sync failed, bad status code %d", r.StatusCode)
	}
	marshaled, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync failed %s", err)
	}
	beaconState := &cltypes.BeaconState{}
	err = beaconState.UnmarshalSSZ(marshaled)
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync failed %s", err)
	}
	return beaconState, nil

}

func (l *LightClient) BootstrapCheckpoint(ctx context.Context, finalized [32]byte) error {
	log.Info("[Checkpoint Sync] Retrieving lightclient bootstrap from sentinel",
		"root", common.Bytes2Hex(finalized[:]))
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
				return err
			}
			log.Info("[Checkpoint Sync] Retrieving bootstrap", "peers", peers.Amount)
		case <-retryInterval.C:
			// Async request
			go func() {
				b, err := rpc.SendLightClientBootstrapReqV1(ctx, &cltypes.SingleRoot{
					Root: finalized,
				}, l.sentinel)
				if err != nil {
					log.Trace("[Checkpoint Sync] could not retrieve bootstrap", "err", err)
					return
				}
				if b == nil {
					return
				}

				s, err := NewLightClientStore(finalized, b)
				if err != nil {
					log.Warn("[Checkpoint Sync] could not create/validate store", "err", err)
					return
				}
				store.Store(s)
			}()
		case <-ctx.Done():
			return fmt.Errorf("context cancelled")
		}
	}
	l.store = store.Load().(*LightClientStore)
	log.Info("Store Initialized successfully",
		"slot", l.store.finalizedHeader.Slot,
		"root", common.Bytes2Hex(l.store.finalizedHeader.Root[:]))
	return nil
}

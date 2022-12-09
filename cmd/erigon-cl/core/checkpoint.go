package core

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/log/v3"
)

func RetrieveBeaconState(ctx context.Context, uri string) (*cltypes.BeaconStateBellatrix, error) {
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

	beaconState := &cltypes.BeaconStateBellatrix{}
	err = beaconState.UnmarshalSSZ(marshaled)
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync failed %s", err)
	}
	return beaconState, nil
}

package core

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/cl/clparams"
)

func extractSlotFromSerializedBeaconState(beaconState []byte) (uint64, error) {
	if len(beaconState) < 48 {
		return 0, fmt.Errorf("checkpoint sync read failed, too short")
	}
	return binary.LittleEndian.Uint64(beaconState[40:48]), nil
}

func RetrieveBeaconState(ctx context.Context, beaconConfig *clparams.BeaconChainConfig, net clparams.NetworkType) (*state.CachingBeaconState, error) {
	uris := clparams.GetAllCheckpointSyncEndpoints(net)
	if len(uris) == 0 {
		return nil, fmt.Errorf("no uris for checkpoint sync")
	}

	fetchBeaconState := func(uri string) (*state.CachingBeaconState, error) {
		log.Info("[Checkpoint Sync] Requesting beacon state", "uri", uri)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Accept", "application/octet-stream")
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync request failed %s", err)
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
			return nil, fmt.Errorf("checkpoint sync read failed %s", err)
		}

		epoch, err := extractSlotFromSerializedBeaconState(marshaled)
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync read failed %s", err)
		}

		beaconState := state.New(beaconConfig)
		err = beaconState.DecodeSSZ(marshaled, int(beaconConfig.GetCurrentStateVersion(epoch)))
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync decode failed %s", err)
		}
		return beaconState, nil
	}

	// Try all uris until one succeeds
	var err error
	var beaconState *state.CachingBeaconState
	for _, uri := range uris {
		beaconState, err = fetchBeaconState(uri)
		if err == nil {
			return beaconState, nil
		}
		log.Warn("[Checkpoint Sync] Failed to fetch beacon state", "uri", uri, "err", err)
	}
	return nil, err
}

func RetrieveBlock(ctx context.Context, beaconConfig *clparams.BeaconChainConfig, uri string, expectedBlockRoot *libcommon.Hash) (*cltypes.SignedBeaconBlock, error) {
	log.Debug("[Checkpoint Sync] Requesting beacon block", "uri", uri)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync request failed %s", err)
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
		return nil, fmt.Errorf("checkpoint sync read failed %s", err)
	}
	if len(marshaled) < 108 {
		return nil, fmt.Errorf("checkpoint sync read failed, too short")
	}
	currentSlot := binary.LittleEndian.Uint64(marshaled[100:108])
	v := beaconConfig.GetCurrentStateVersion(currentSlot / beaconConfig.SlotsPerEpoch)

	block := cltypes.NewSignedBeaconBlock(beaconConfig)
	err = block.DecodeSSZ(marshaled, int(v))
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync decode failed %s", err)
	}
	if expectedBlockRoot != nil {
		has, err := block.Block.HashSSZ()
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync decode failed %s", err)
		}
		if has != *expectedBlockRoot {
			return nil, fmt.Errorf("checkpoint sync decode failed, unexpected block root %s", has)
		}
	}
	return block, nil
}

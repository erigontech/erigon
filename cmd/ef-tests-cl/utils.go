package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func decodeStateFromFile(filepath string) (*state.BeaconState, error) {
	sszSnappy, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	testState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappyWithVersion(testState, sszSnappy, int(testVersion)); err != nil {
		return nil, err
	}
	return testState, nil
}

func testBlocks() ([]*cltypes.SignedBeaconBlock, error) {
	i := 0
	blocks := []*cltypes.SignedBeaconBlock{}
	var err error
	for {
		var blockBytes []byte
		blockBytes, err = os.ReadFile(fmt.Sprintf("blocks_%d.ssz_snappy", i))
		if err != nil {
			break
		}
		blk := &cltypes.SignedBeaconBlock{}
		if err = utils.DecodeSSZSnappyWithVersion(blk, blockBytes, int(testVersion)); err != nil {
			return nil, err
		}
		blocks = append(blocks, blk)
		i++
	}
	if os.IsNotExist(err) {
		err = nil
	}
	return blocks, err
}

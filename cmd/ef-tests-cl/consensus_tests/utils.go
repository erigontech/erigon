package consensustests

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func decodeStateFromFile(context testContext, filepath string) (*state.BeaconState, error) {
	sszSnappy, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	config := clparams.MainnetBeaconConfig
	testState := state.New(&config)
	if err := utils.DecodeSSZSnappyWithVersion(testState, sszSnappy, int(context.version)); err != nil {
		return nil, err
	}
	return testState, nil
}

func decodeSSZObjectFromFile(obj ssz.Unmarshaler, version clparams.StateVersion, filepath string) error {
	sszSnappy, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}
	return utils.DecodeSSZSnappyWithVersion(obj, sszSnappy, int(version))
}

func testBlocks(context testContext) ([]*cltypes.SignedBeaconBlock, error) {
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
		if err = utils.DecodeSSZSnappyWithVersion(blk, blockBytes, int(context.version)); err != nil {
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

func testBlock(context testContext, index int) (*cltypes.SignedBeaconBlock, error) {
	var blockBytes []byte
	var err error
	blockBytes, err = os.ReadFile(fmt.Sprintf("blocks_%d.ssz_snappy", index))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	blk := &cltypes.SignedBeaconBlock{}
	if err = utils.DecodeSSZSnappyWithVersion(blk, blockBytes, int(context.version)); err != nil {
		return nil, err
	}

	return blk, nil
}

func testBlockSlot(index int) (uint64, error) {
	var blockBytes []byte
	var err error
	blockBytes, err = os.ReadFile(fmt.Sprintf("blocks_%d.ssz_snappy", index))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	blockBytes, err = utils.DecompressSnappy(blockBytes)
	if err != nil {
		return 0, err
	}
	return ssz.UnmarshalUint64SSZ(blockBytes[100:108]), nil
}

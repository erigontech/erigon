package rawdb

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func EncodeNumber(n uint64) []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, uint32(n))
	return ret
}

// WriteBeaconState writes beacon state for specific block to database.
func WriteBeaconState(tx kv.Putter, state *state.CachingBeaconState) error {
	data, err := utils.EncodeSSZSnappy(state)
	if err != nil {
		return err
	}

	return tx.Put(kv.BeaconState, EncodeNumber(state.Slot()), data)
}

func WriteBeaconBlock(tx kv.RwTx, signedBlock *cltypes.SignedBeaconBlock) error {
	block := signedBlock.Block

	blockRoot, err := block.HashSSZ()
	if err != nil {
		return err
	}
	// database key is is [slot + block root]
	slotBytes := EncodeNumber(block.Slot)
	key := append(slotBytes, blockRoot[:]...)
	value, err := signedBlock.EncodeSSZ(nil)
	if err != nil {
		return err
	}

	// Write block hashes
	// We write the block indexing
	if err := tx.Put(kv.RootSlotIndex, blockRoot[:], slotBytes); err != nil {
		return err
	}
	if err := tx.Put(kv.RootSlotIndex, block.StateRoot[:], key); err != nil {
		return err
	}
	// Finally write the beacon block
	return tx.Put(kv.BeaconBlocks, key, utils.CompressSnappy(value))
}

func ReadBeaconBlock(tx kv.RwTx, blockRoot libcommon.Hash, slot uint64, version clparams.StateVersion) (*cltypes.SignedBeaconBlock, uint64, libcommon.Hash, error) {
	encodedBeaconBlock, err := tx.GetOne(kv.BeaconBlocks, append(EncodeNumber(slot), blockRoot[:]...))
	if err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	if len(encodedBeaconBlock) == 0 {
		return nil, 0, libcommon.Hash{}, nil
	}
	if encodedBeaconBlock, err = utils.DecompressSnappy(encodedBeaconBlock); err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	signedBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	if err := signedBlock.DecodeSSZ(encodedBeaconBlock, int(version)); err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	var eth1Number uint64
	var eth1Hash libcommon.Hash
	if signedBlock.Block.Body.ExecutionPayload != nil {
		eth1Number = signedBlock.Block.Body.ExecutionPayload.BlockNumber
		eth1Hash = signedBlock.Block.Body.ExecutionPayload.BlockHash
	}
	return signedBlock, eth1Number, eth1Hash, err
}

func WriteFinalizedBlockRoot(tx kv.Putter, slot uint64, blockRoot libcommon.Hash) error {
	return tx.Put(kv.FinalizedBlockRoots, EncodeNumber(slot), blockRoot[:])
}

func ReadFinalizedBlockRoot(tx kv.Getter, slot uint64) (libcommon.Hash, error) {
	root, err := tx.GetOne(kv.FinalizedBlockRoots, EncodeNumber(slot))
	if err != nil {
		return libcommon.Hash{}, err
	}
	if len(root) == 0 {
		return libcommon.Hash{}, nil
	}
	if len(root) != length.Hash {
		return libcommon.Hash{}, fmt.Errorf("read block root with mismatching length")
	}
	return libcommon.BytesToHash(root), nil
}

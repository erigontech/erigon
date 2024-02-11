package beacon_indicies

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	_ "modernc.org/sqlite"
)

// make a buffer pool
var bufferPool = &sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// make a zstd writer pool
var zstdWriterPool = &sync.Pool{
	New: func() interface{} {
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
		if err != nil {
			panic(err)
		}
		return encoder
	},
}

func WriteHighestFinalized(tx kv.RwTx, slot uint64) error {
	return tx.Put(kv.HighestFinalized, kv.HighestFinalizedKey, base_encoding.Encode64ToBytes4(slot))
}

func ReadHighestFinalized(tx kv.Tx) (uint64, error) {
	val, err := tx.GetOne(kv.HighestFinalized, kv.HighestFinalizedKey)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return base_encoding.Decode64FromBytes4(val), nil
}

// WriteBlockRootSlot writes the slot associated with a block root.
func WriteHeaderSlot(tx kv.RwTx, blockRoot libcommon.Hash, slot uint64) error {
	return tx.Put(kv.BlockRootToSlot, blockRoot[:], base_encoding.Encode64ToBytes4(slot))
}

func ReadBlockSlotByBlockRoot(tx kv.Tx, blockRoot libcommon.Hash) (*uint64, error) {
	slotBytes, err := tx.GetOne(kv.BlockRootToSlot, blockRoot[:])
	if err != nil {
		return nil, err
	}
	if len(slotBytes) == 0 {
		return nil, nil
	}
	slot := new(uint64)
	*slot = base_encoding.Decode64FromBytes4(slotBytes)
	return slot, nil
}

// WriteBlockRootSlot writes the slot associated with a block root.
func WriteStateRoot(tx kv.RwTx, blockRoot libcommon.Hash, stateRoot libcommon.Hash) error {
	if err := tx.Put(kv.BlockRootToStateRoot, blockRoot[:], stateRoot[:]); err != nil {
		return err
	}
	return tx.Put(kv.StateRootToBlockRoot, stateRoot[:], blockRoot[:])
}

func ReadStateRootByBlockRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (libcommon.Hash, error) {
	var stateRoot libcommon.Hash

	sRoot, err := tx.GetOne(kv.BlockRootToStateRoot, blockRoot[:])
	if err != nil {
		return libcommon.Hash{}, err
	}

	copy(stateRoot[:], sRoot)

	return stateRoot, nil
}

func ReadBlockRootByStateRoot(tx kv.Tx, stateRoot libcommon.Hash) (libcommon.Hash, error) {
	var blockRoot libcommon.Hash

	bRoot, err := tx.GetOne(kv.StateRootToBlockRoot, stateRoot[:])
	if err != nil {
		return libcommon.Hash{}, err
	}

	copy(blockRoot[:], bRoot)

	return blockRoot, nil
}

func ReadCanonicalBlockRoot(tx kv.Tx, slot uint64) (libcommon.Hash, error) {
	var blockRoot libcommon.Hash

	bRoot, err := tx.GetOne(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return libcommon.Hash{}, err
	}

	copy(blockRoot[:], bRoot)
	return blockRoot, nil
}

func WriteLastBeaconSnapshot(tx kv.RwTx, slot uint64) error {
	return tx.Put(kv.LastBeaconSnapshot, []byte(kv.LastBeaconSnapshotKey), base_encoding.Encode64ToBytes4(slot))
}

func ReadLastBeaconSnapshot(tx kv.Tx) (uint64, error) {
	val, err := tx.GetOne(kv.LastBeaconSnapshot, []byte(kv.LastBeaconSnapshotKey))
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return base_encoding.Decode64FromBytes4(val), nil
}

func MarkRootCanonical(ctx context.Context, tx kv.RwTx, slot uint64, blockRoot libcommon.Hash) error {
	return tx.Put(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(slot), blockRoot[:])
}

func WriteExecutionBlockNumber(tx kv.RwTx, blockRoot libcommon.Hash, blockNumber uint64) error {
	return tx.Put(kv.BlockRootToBlockNumber, blockRoot[:], base_encoding.Encode64ToBytes4(blockNumber))
}

func WriteExecutionBlockHash(tx kv.RwTx, blockRoot, blockHash libcommon.Hash) error {
	return tx.Put(kv.BlockRootToBlockHash, blockRoot[:], blockHash[:])
}

func ReadExecutionBlockNumber(tx kv.Tx, blockRoot libcommon.Hash) (*uint64, error) {
	val, err := tx.GetOne(kv.BlockRootToBlockNumber, blockRoot[:])
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}
	ret := new(uint64)
	*ret = base_encoding.Decode64FromBytes4(val)
	return ret, nil
}

func ReadExecutionBlockHash(tx kv.Tx, blockRoot libcommon.Hash) (libcommon.Hash, error) {
	val, err := tx.GetOne(kv.BlockRootToBlockHash, blockRoot[:])
	if err != nil {
		return libcommon.Hash{}, err
	}
	if len(val) == 0 {
		return libcommon.Hash{}, nil
	}
	return libcommon.BytesToHash(val), nil
}

func WriteBeaconBlockHeader(ctx context.Context, tx kv.RwTx, signedHeader *cltypes.SignedBeaconBlockHeader) error {
	headersBytes, err := signedHeader.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	blockRoot, err := signedHeader.Header.HashSSZ()
	if err != nil {
		return err
	}
	return tx.Put(kv.BeaconBlockHeaders, blockRoot[:], headersBytes)
}

func WriteBeaconBlockHeaderAndIndicies(ctx context.Context, tx kv.RwTx, signedHeader *cltypes.SignedBeaconBlockHeader, forceCanonical bool) error {
	blockRoot, err := signedHeader.Header.HashSSZ()
	if err != nil {
		return err
	}
	if err := WriteBeaconBlockHeader(ctx, tx, signedHeader); err != nil {
		return err
	}
	if err := WriteHeaderSlot(tx, blockRoot, signedHeader.Header.Slot); err != nil {
		return err
	}
	if err := WriteStateRoot(tx, blockRoot, signedHeader.Header.Root); err != nil {
		return err
	}
	if err := WriteParentBlockRoot(ctx, tx, blockRoot, signedHeader.Header.ParentRoot); err != nil {
		return err
	}
	if forceCanonical {
		if err := MarkRootCanonical(ctx, tx, signedHeader.Header.Slot, blockRoot); err != nil {
			return err
		}
	}

	return nil
}

func ReadParentBlockRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (libcommon.Hash, error) {
	var parentRoot libcommon.Hash

	pRoot, err := tx.GetOne(kv.BlockRootToParentRoot, blockRoot[:])
	if err != nil {
		return libcommon.Hash{}, err
	}

	copy(parentRoot[:], pRoot)

	return parentRoot, nil
}

func WriteParentBlockRoot(ctx context.Context, tx kv.RwTx, blockRoot, parentRoot libcommon.Hash) error {
	return tx.Put(kv.BlockRootToParentRoot, blockRoot[:], parentRoot[:])
}

func TruncateCanonicalChain(ctx context.Context, tx kv.RwTx, slot uint64) error {
	return tx.ForEach(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(slot), func(k, _ []byte) error {
		return tx.Delete(kv.CanonicalBlockRoots, k)
	})
}

func PruneSignedHeaders(tx kv.RwTx, from uint64) error {
	cursor, err := tx.RwCursor(kv.BeaconBlockHeaders)
	if err != nil {
		return err
	}
	for k, _, err := cursor.Seek(base_encoding.Encode64ToBytes4(from)); err == nil && k != nil; k, _, err = cursor.Prev() {
		if err != nil {
			return err
		}
		if err := cursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	return nil
}

func RangeBlockRoots(ctx context.Context, tx kv.Tx, fromSlot, toSlot uint64, fn func(slot uint64, beaconBlockRoot libcommon.Hash) bool) error {
	cursor, err := tx.Cursor(kv.CanonicalBlockRoots)
	if err != nil {
		return err
	}
	for k, v, err := cursor.Seek(base_encoding.Encode64ToBytes4(fromSlot)); err == nil && k != nil && base_encoding.Decode64FromBytes4(k) <= toSlot; k, v, err = cursor.Next() {
		if !fn(base_encoding.Decode64FromBytes4(k), libcommon.BytesToHash(v)) {
			break
		}
	}
	return err
}

func PruneBlockRoots(ctx context.Context, tx kv.RwTx, fromSlot, toSlot uint64) error {
	cursor, err := tx.RwCursor(kv.CanonicalBlockRoots)
	if err != nil {
		return err
	}
	for k, _, err := cursor.Seek(base_encoding.Encode64ToBytes4(fromSlot)); err == nil && k != nil && base_encoding.Decode64FromBytes4(k) <= toSlot; k, _, err = cursor.Next() {
		if err := cursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	return err
}

func ReadBeaconBlockRootsInSlotRange(ctx context.Context, tx kv.Tx, fromSlot, count uint64) ([]libcommon.Hash, []uint64, error) {
	blockRoots := make([]libcommon.Hash, 0, count)
	slots := make([]uint64, 0, count)
	cursor, err := tx.Cursor(kv.CanonicalBlockRoots)
	if err != nil {
		return nil, nil, err
	}
	currentCount := uint64(0)
	for k, v, err := cursor.Seek(base_encoding.Encode64ToBytes4(fromSlot)); err == nil && k != nil && currentCount != count; k, v, err = cursor.Next() {
		currentCount++
		blockRoots = append(blockRoots, libcommon.BytesToHash(v))
		slots = append(slots, base_encoding.Decode64FromBytes4(k))
	}
	return blockRoots, slots, err
}

func WriteBeaconBlock(ctx context.Context, tx kv.RwTx, block *cltypes.SignedBeaconBlock) error {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	// take a buffer and encoder
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	encoder := zstdWriterPool.Get().(*zstd.Encoder)
	defer zstdWriterPool.Put(encoder)
	buf.Reset()
	encoder.Reset(buf)
	_, err = snapshot_format.WriteBlockForSnapshot(encoder, block, nil)
	if err != nil {
		return err
	}
	if err := encoder.Flush(); err != nil {
		return err
	}
	if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, blockRoot), libcommon.Copy(buf.Bytes())); err != nil {
		return err
	}
	return nil
}

func WriteBeaconBlockAndIndicies(ctx context.Context, tx kv.RwTx, block *cltypes.SignedBeaconBlock, canonical bool) error {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}

	if err := WriteBeaconBlock(ctx, tx, block); err != nil {
		return err
	}

	bodyRoot, err := block.Block.Body.HashSSZ()
	if err != nil {
		return err
	}
	if block.Version() >= clparams.BellatrixVersion {
		if err := WriteExecutionBlockNumber(tx, blockRoot, block.Block.Body.ExecutionPayload.BlockNumber); err != nil {
			return err
		}
		if err := WriteExecutionBlockHash(tx, blockRoot, block.Block.Body.ExecutionPayload.BlockHash); err != nil {
			return err
		}
	}

	if err := WriteBeaconBlockHeaderAndIndicies(ctx, tx, &cltypes.SignedBeaconBlockHeader{
		Signature: block.Signature,
		Header: &cltypes.BeaconBlockHeader{
			Slot:          block.Block.Slot,
			ParentRoot:    block.Block.ParentRoot,
			ProposerIndex: block.Block.ProposerIndex,
			Root:          block.Block.StateRoot,
			BodyRoot:      bodyRoot,
		},
	}, canonical); err != nil {
		return err
	}
	return nil
}

func PruneBlocks(ctx context.Context, tx kv.RwTx, to uint64) error {
	cursor, err := tx.RwCursor(kv.BeaconBlocks)
	if err != nil {
		return err
	}
	for k, _, err := cursor.First(); err == nil && k != nil; k, _, err = cursor.Prev() {
		if len(k) != 40 {
			continue
		}
		slot, err := dbutils.DecodeBlockNumber(k[:8])
		if err != nil {
			return err
		}
		if slot >= to {
			break
		}
		if err := cursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	return nil

}

func ReadSignedHeaderByBlockRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlockHeader, bool, error) {
	h := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{}}
	headerBytes, err := tx.GetOne(kv.BeaconBlockHeaders, blockRoot[:])
	if err != nil {
		return nil, false, err
	}
	if len(headerBytes) == 0 {
		return nil, false, nil
	}
	if err := h.DecodeSSZ(headerBytes, 0); err != nil {
		return nil, false, fmt.Errorf("failed to decode BeaconHeader: %v", err)
	}
	canonical, err := ReadCanonicalBlockRoot(tx, h.Header.Slot)
	if err != nil {
		return nil, false, err
	}
	return h, canonical == blockRoot, nil
}

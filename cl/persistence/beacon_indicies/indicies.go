package beacon_indicies

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	_ "modernc.org/sqlite"
)

func WriteHighestFinalized(tx kv.RwTx, slot uint64) error {
	return tx.Put(kv.HighestFinalized, kv.HighestFinalizedKey, base_encoding.Encode64(slot))
}

func ReadHighestFinalized(tx kv.Tx) (uint64, error) {
	val, err := tx.GetOne(kv.HighestFinalized, kv.HighestFinalizedKey)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return base_encoding.Decode64(val), nil
}

// WriteBlockRootSlot writes the slot associated with a block root.
func WriteHeaderSlot(tx kv.RwTx, blockRoot libcommon.Hash, slot uint64) error {
	return tx.Put(kv.BlockRootToSlot, blockRoot[:], base_encoding.Encode64(slot))
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
	*slot = base_encoding.Decode64(slotBytes)
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

	return stateRoot, nil
}

func ReadCanonicalBlockRoot(tx kv.Tx, slot uint64) (libcommon.Hash, error) {
	var blockRoot libcommon.Hash

	bRoot, err := tx.GetOne(kv.CanonicalBlockRoots, base_encoding.Encode64(slot))
	if err != nil {
		return libcommon.Hash{}, err
	}

	copy(blockRoot[:], bRoot)
	return blockRoot, nil
}

func MarkRootCanonical(ctx context.Context, tx kv.RwTx, slot uint64, blockRoot libcommon.Hash) error {
	return tx.Put(kv.CanonicalBlockRoots, base_encoding.Encode64(slot), blockRoot[:])
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
	return tx.ForEach(kv.CanonicalBlockRoots, base_encoding.Encode64(slot), func(k, _ []byte) error {
		return tx.Delete(kv.CanonicalBlockRoots, k)
	})
}

func RangeBlockRoots(ctx context.Context, tx kv.Tx, fromSlot, toSlot uint64, fn func(slot uint64, beaconBlockRoot libcommon.Hash) bool) error {
	cursor, err := tx.Cursor(kv.CanonicalBlockRoots)
	if err != nil {
		return err
	}
	for k, v, err := cursor.Seek(base_encoding.Encode64(fromSlot)); err == nil && k != nil && base_encoding.Decode64(k) <= toSlot; k, v, err = cursor.Next() {
		if !fn(base_encoding.Decode64(k), libcommon.BytesToHash(v)) {
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
	for k, _, err := cursor.Seek(base_encoding.Encode64(fromSlot)); err == nil && k != nil && base_encoding.Decode64(k) <= toSlot; k, _, err = cursor.Next() {
		if err := cursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	return err
}

func ReadBeaconBlockRootsInSlotRange(ctx context.Context, tx kv.Tx, fromSlot, count uint64) ([]libcommon.Hash, []uint64, error) {
	blockRoots := make([]libcommon.Hash, 0, count)
	slots := make([]uint64, 0, count)
	err := RangeBlockRoots(ctx, tx, fromSlot, fromSlot+count, func(slot uint64, beaconBlockRoot libcommon.Hash) bool {
		blockRoots = append(blockRoots, beaconBlockRoot)
		slots = append(slots, slot)
		return true
	})
	return blockRoots, slots, err
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

func ReadSignedHeaderByStateRoot(ctx context.Context, tx kv.Tx, stateRoot libcommon.Hash) (*cltypes.SignedBeaconBlockHeader, bool, error) {
	blockRoot, err := ReadBlockRootByStateRoot(tx, stateRoot)
	if err != nil {
		return nil, false, err
	}
	return ReadSignedHeaderByBlockRoot(ctx, tx, blockRoot)
}

// We dont support non-canonicals for the below methods

func ReadSignedHeadersBySlot(ctx context.Context, tx kv.Tx, slot uint64) ([]*cltypes.SignedBeaconBlockHeader, []bool, error) {
	root, err := ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return nil, nil, err
	}
	if root == (libcommon.Hash{}) {
		return nil, nil, nil
	}
	h, _, err := ReadSignedHeaderByBlockRoot(ctx, tx, root)
	if err != nil {
		return nil, nil, err
	}
	if h == nil {
		return nil, nil, nil
	}
	return []*cltypes.SignedBeaconBlockHeader{h}, []bool{true}, nil
}

func ReadSignedHeadersByParentRoot(ctx context.Context, tx kv.Tx, parentRoot libcommon.Hash) ([]*cltypes.SignedBeaconBlockHeader, []bool, error) {
	// Execute the query.
	slot, err := ReadBlockSlotByBlockRoot(tx, parentRoot)
	if err != nil {
		return nil, nil, err
	}
	if slot == nil {
		return nil, nil, nil
	}
	return ReadSignedHeadersBySlot(ctx, tx, *slot)
}

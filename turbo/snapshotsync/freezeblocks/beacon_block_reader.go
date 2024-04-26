package freezeblocks

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
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
)

var buffersPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

var decompressorPool = sync.Pool{
	New: func() interface{} {
		r, err := zstd.NewReader(nil)
		if err != nil {
			panic(err)
		}
		return r
	},
}

type BeaconSnapshotReader interface {
	// ReadBlock reads the block at the given slot.
	// If the block is not present, it returns nil.
	ReadBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error)
	ReadBlockByRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlock, error)
	ReadHeaderByRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlockHeader, error)
	ReadBlindedBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBlindedBeaconBlock, error)

	FrozenSlots() uint64
}

type beaconSnapshotReader struct {
	sn *CaplinSnapshots

	eth1Getter snapshot_format.ExecutionBlockReaderByNumber
	cfg        *clparams.BeaconChainConfig
}

func NewBeaconSnapshotReader(snapshots *CaplinSnapshots, eth1Getter snapshot_format.ExecutionBlockReaderByNumber, cfg *clparams.BeaconChainConfig) BeaconSnapshotReader {
	return &beaconSnapshotReader{sn: snapshots, eth1Getter: eth1Getter, cfg: cfg}
}

func (r *beaconSnapshotReader) FrozenSlots() uint64 {
	return r.sn.BlocksAvailable()
}

func (r *beaconSnapshotReader) ReadBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	if r.eth1Getter == nil {
		return nil, nil
	}
	view := r.sn.View()
	defer view.Close()

	var buf []byte
	if slot > r.sn.BlocksAvailable() {
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return nil, err
		}
		if blockRoot == (libcommon.Hash{}) {
			return nil, nil
		}
		buf, err = tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, blockRoot))
		if err != nil {
			return nil, err
		}
	} else {
		seg, ok := view.BeaconBlocksSegment(slot)
		if !ok {
			return nil, nil
		}

		idxSlot := seg.Index()

		if idxSlot == nil {
			return nil, nil
		}
		if slot < idxSlot.BaseDataID() {
			return nil, fmt.Errorf("slot %d is before the base data id %d", slot, idxSlot.BaseDataID())
		}
		blockOffset := idxSlot.OrdinalLookup(slot - idxSlot.BaseDataID())

		gg := seg.MakeGetter()
		gg.Reset(blockOffset)
		if !gg.HasNext() {
			return nil, nil
		}

		buf = buf[:0]
		buf, _ = gg.Next(buf)
	}
	if len(buf) == 0 {
		return nil, nil
	}

	// Decompress this thing
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)

	buffer.Reset()
	buffer.Write(buf)
	reader := decompressorPool.Get().(*zstd.Decoder)
	defer decompressorPool.Put(reader)
	reader.Reset(buffer)

	// Use pooled buffers and readers to avoid allocations.
	return snapshot_format.ReadBlockFromSnapshot(reader, r.eth1Getter, r.cfg)
}

func (r *beaconSnapshotReader) ReadBlindedBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBlindedBeaconBlock, error) {
	view := r.sn.View()
	defer view.Close()

	var buf []byte
	if slot > r.sn.BlocksAvailable() {
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return nil, err
		}
		if blockRoot == (libcommon.Hash{}) {
			return nil, nil
		}
		buf, err = tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, blockRoot))
		if err != nil {
			return nil, err
		}
	} else {
		seg, ok := view.BeaconBlocksSegment(slot)
		if !ok {
			return nil, nil
		}

		idxSlot := seg.Index()

		if idxSlot == nil {
			return nil, nil
		}
		if slot < idxSlot.BaseDataID() {
			return nil, fmt.Errorf("slot %d is before the base data id %d", slot, idxSlot.BaseDataID())
		}
		blockOffset := idxSlot.OrdinalLookup(slot - idxSlot.BaseDataID())

		gg := seg.MakeGetter()
		gg.Reset(blockOffset)
		if !gg.HasNext() {
			return nil, nil
		}

		buf = buf[:0]
		buf, _ = gg.Next(buf)
	}
	if len(buf) == 0 {
		return nil, nil
	}

	// Decompress this thing
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)

	buffer.Reset()
	buffer.Write(buf)
	reader := decompressorPool.Get().(*zstd.Decoder)
	defer decompressorPool.Put(reader)
	reader.Reset(buffer)

	// Use pooled buffers and readers to avoid allocations.
	return snapshot_format.ReadBlindedBlockFromSnapshot(reader, r.cfg)
}

func (r *beaconSnapshotReader) ReadBlockByRoot(ctx context.Context, tx kv.Tx, root libcommon.Hash) (*cltypes.SignedBeaconBlock, error) {
	if r.eth1Getter == nil {
		return nil, nil
	}
	view := r.sn.View()
	defer view.Close()

	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, nil
	}

	var buf []byte
	if *slot > r.sn.BlocksAvailable() {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		if err != nil {
			return nil, err
		}
		if slot == nil {
			return nil, nil
		}
		buf, err = tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(*slot, root))
		if err != nil {
			return nil, err
		}
	} else {
		// Find canonical block
		canonicalBlockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
		if err != nil {
			return nil, err
		}
		// root non-canonical? BAD
		if canonicalBlockRoot != root {
			return nil, nil
		}

		seg, ok := view.BeaconBlocksSegment(*slot)
		if !ok {
			return nil, nil
		}

		idxSlot := seg.Index()

		if idxSlot == nil {
			return nil, nil
		}
		if *slot < idxSlot.BaseDataID() {
			return nil, fmt.Errorf("slot %d is before the base data id %d", slot, idxSlot.BaseDataID())
		}
		blockOffset := idxSlot.OrdinalLookup(*slot - idxSlot.BaseDataID())

		gg := seg.MakeGetter()
		gg.Reset(blockOffset)
		if !gg.HasNext() {
			return nil, nil
		}

		buf = buf[:0]
		buf, _ = gg.Next(buf)
	}

	if len(buf) == 0 {
		return nil, nil
	}
	// Decompress this thing
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)

	buffer.Reset()
	buffer.Write(buf)
	reader := decompressorPool.Get().(*zstd.Decoder)
	defer decompressorPool.Put(reader)
	reader.Reset(buffer)

	// Use pooled buffers and readers to avoid allocations.
	return snapshot_format.ReadBlockFromSnapshot(reader, r.eth1Getter, r.cfg)
}

func (r *beaconSnapshotReader) ReadHeaderByRoot(ctx context.Context, tx kv.Tx, root libcommon.Hash) (*cltypes.SignedBeaconBlockHeader, error) {
	view := r.sn.View()
	defer view.Close()

	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, nil
	}

	if *slot > r.sn.BlocksAvailable() {
		h, _, err := beacon_indicies.ReadSignedHeaderByBlockRoot(ctx, tx, root)
		return h, err
	}
	// Find canonical block
	canonicalBlockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}
	// root non-canonical? BAD
	if canonicalBlockRoot != root {
		return nil, nil
	}

	h, _, _, err := r.sn.ReadHeader(*slot)
	// Use pooled buffers and readers to avoid allocations.
	return h, err
}

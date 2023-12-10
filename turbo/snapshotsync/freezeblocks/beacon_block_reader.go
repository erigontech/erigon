package freezeblocks

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	"github.com/pierrec/lz4"
)

var buffersPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

var lz4ReaderPool = sync.Pool{
	New: func() interface{} {
		return lz4.NewReader(nil)
	},
}

type BeaconSnapshotReader interface {
	// ReadBlock reads the block at the given slot.
	// If the block is not present, it returns nil.
	ReadBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error)
	ReadBlockByRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlock, error)
	ReadHeaderByRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlockHeader, error)

	FrozenSlots() uint64
}

type beaconSnapshotReader struct {
	sn *CaplinSnapshots

	eth1Getter snapshot_format.ExecutionBlockReaderByNumber
	beaconDB   persistence.BlockSource
	cfg        *clparams.BeaconChainConfig
}

func NewBeaconSnapshotReader(snapshots *CaplinSnapshots, eth1Getter snapshot_format.ExecutionBlockReaderByNumber, beaconDB persistence.BlockSource, cfg *clparams.BeaconChainConfig) BeaconSnapshotReader {
	return &beaconSnapshotReader{sn: snapshots, eth1Getter: eth1Getter, cfg: cfg, beaconDB: beaconDB}
}

func (r *beaconSnapshotReader) FrozenSlots() uint64 {
	return r.sn.BlocksAvailable()
}

func (r *beaconSnapshotReader) ReadBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	view := r.sn.View()
	defer view.Close()

	var buf []byte
	if slot > r.sn.BlocksAvailable() {
		data, err := r.beaconDB.GetBlock(ctx, tx, slot)
		if data == nil {
			return nil, err
		}
		return data.Data, err
	}
	if r.eth1Getter == nil {
		return nil, nil
	}

	seg, ok := view.BeaconBlocksSegment(slot)
	if !ok {
		return nil, nil
	}

	if seg.idxSlot == nil {
		return nil, nil
	}
	if slot < seg.idxSlot.BaseDataID() {
		return nil, fmt.Errorf("slot %d is before the base data id %d", slot, seg.idxSlot.BaseDataID())
	}
	blockOffset := seg.idxSlot.OrdinalLookup(slot - seg.idxSlot.BaseDataID())

	gg := seg.seg.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}

	buf = buf[:0]
	buf, _ = gg.Next(buf)
	if len(buf) == 0 {
		return nil, nil
	}
	// Decompress this thing
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)

	buffer.Reset()
	buffer.Write(buf)
	lzReader := lz4ReaderPool.Get().(*lz4.Reader)
	defer lz4ReaderPool.Put(lzReader)
	lzReader.Reset(buffer)

	// Use pooled buffers and readers to avoid allocations.
	return snapshot_format.ReadBlockFromSnapshot(lzReader, r.eth1Getter, r.cfg)
}

func (r *beaconSnapshotReader) ReadBlockByRoot(ctx context.Context, tx kv.Tx, root libcommon.Hash) (*cltypes.SignedBeaconBlock, error) {
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
		data, err := r.beaconDB.GetBlock(ctx, tx, *slot)
		return data.Data, err
	}
	if r.eth1Getter == nil {
		return nil, nil
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

	seg, ok := view.BeaconBlocksSegment(*slot)
	if !ok {
		return nil, nil
	}

	if seg.idxSlot == nil {
		return nil, nil
	}
	if *slot < seg.idxSlot.BaseDataID() {
		return nil, fmt.Errorf("slot %d is before the base data id %d", slot, seg.idxSlot.BaseDataID())
	}
	blockOffset := seg.idxSlot.OrdinalLookup(*slot - seg.idxSlot.BaseDataID())

	gg := seg.seg.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}

	buf = buf[:0]
	buf, _ = gg.Next(buf)
	if len(buf) == 0 {
		return nil, nil
	}
	// Decompress this thing
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)

	buffer.Reset()
	buffer.Write(buf)
	lzReader := lz4ReaderPool.Get().(*lz4.Reader)
	defer lz4ReaderPool.Put(lzReader)
	lzReader.Reset(buffer)

	// Use pooled buffers and readers to avoid allocations.
	return snapshot_format.ReadBlockFromSnapshot(lzReader, r.eth1Getter, r.cfg)
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

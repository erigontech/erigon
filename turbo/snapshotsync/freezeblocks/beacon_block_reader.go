package freezeblocks

import (
	"bytes"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
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
	ReadBlock(slot uint64) (*cltypes.SignedBeaconBlock, error)
	ReadHeader(slot uint64) (*cltypes.SignedBeaconBlockHeader, uint64, libcommon.Hash, error)

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

func (r *beaconSnapshotReader) ReadBlock(slot uint64) (*cltypes.SignedBeaconBlock, error) {
	view := r.sn.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.BeaconBlocksSegment(slot)
	if !ok {
		return nil, nil
	}

	if seg.idxSlot == nil {
		return nil, nil
	}
	blockOffset := seg.idxSlot.OrdinalLookup(slot - seg.idxSlot.BaseDataID())

	gg := seg.seg.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}

	buf, _ = gg.Next(buf)
	if buf == nil {
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
	return snapshot_format.ReadBlockFromSnapshot(buffer, r.eth1Getter, r.cfg)
}

func (r *beaconSnapshotReader) ReadHeader(slot uint64) (*cltypes.SignedBeaconBlockHeader, uint64, libcommon.Hash, error) {
	view := r.sn.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.BeaconBlocksSegment(slot)
	if !ok {
		return nil, 0, libcommon.Hash{}, nil
	}

	if seg.idxSlot == nil {
		return nil, 0, libcommon.Hash{}, nil
	}
	blockOffset := seg.idxSlot.OrdinalLookup(slot - seg.idxSlot.BaseDataID())

	gg := seg.seg.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, 0, libcommon.Hash{}, nil
	}

	buf, _ = gg.Next(buf)
	if buf == nil {
		return nil, 0, libcommon.Hash{}, nil
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
	return snapshot_format.ReadBlockHeaderFromSnapshotWithExecutionData(buffer, r.cfg)
}

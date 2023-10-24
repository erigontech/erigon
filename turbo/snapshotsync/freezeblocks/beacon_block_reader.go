package freezeblocks

import (
	"bytes"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
)

type BeaconSnapshotReader interface {
	// ReadBlock reads the block at the given slot.
	// If the block is not present, it returns nil.
	ReadBlock(slot uint64) (*cltypes.SignedBeaconBlock, error)
	RawBlockSSZ(slot uint64) ([]byte, error)

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
	buf, err := r.RawBlockSSZ(slot)
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return snapshot_format.ReadBlockFromSnapshot(bytes.NewReader(buf), r.eth1Getter, r.cfg)
}

func (r *beaconSnapshotReader) RawBlockSSZ(slot uint64) ([]byte, error) {
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
	return buf, nil
}

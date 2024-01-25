package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

// SyncAggregatorSelectionData data, contains if we were on bellatrix/alteir/phase0 and transition epoch.
type SyncAggregatorSelectionData struct {
	Slot              uint64 `json:"slot,string"`
	SubcommitteeIndex uint64 `json:"subcommittee_index,string"`
}

func (*SyncAggregatorSelectionData) Static() bool {
	return true
}

func (f *SyncAggregatorSelectionData) Copy() *SyncAggregatorSelectionData {
	return &SyncAggregatorSelectionData{
		Slot:              f.Slot,
		SubcommitteeIndex: f.SubcommitteeIndex,
	}
}

func (f *SyncAggregatorSelectionData) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, f.Slot, f.SubcommitteeIndex)
}

func (f *SyncAggregatorSelectionData) DecodeSSZ(buf []byte, _ int) error {
	return ssz2.UnmarshalSSZ(buf, 0, &f.Slot, &f.SubcommitteeIndex)

}

func (f *SyncAggregatorSelectionData) EncodingSizeSSZ() int {
	return 16
}

func (f *SyncAggregatorSelectionData) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(f.Slot, f.SubcommitteeIndex)
}

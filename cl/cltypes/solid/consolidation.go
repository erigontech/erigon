package solid

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	_ ssz.EncodableSSZ = (*PendingConsolidation)(nil)
	_ ssz.HashableSSZ  = (*PendingConsolidation)(nil)
)

type PendingConsolidation struct {
	SourceIndex uint64 // validator index
	TargetIndex uint64 // validator index
}

func (p *PendingConsolidation) EncodingSizeSSZ() int {
	return 16
}

func (p *PendingConsolidation) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.SourceIndex, p.TargetIndex)
}

func (p *PendingConsolidation) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &p.SourceIndex, &p.TargetIndex)
}

func (p PendingConsolidation) Clone() clonable.Clonable {
	return &PendingConsolidation{}
}

func (p *PendingConsolidation) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.SourceIndex, p.TargetIndex)
}

type ConsolidationRequest struct {
	SourceAddress common.Address
	SourcePubKey  common.Bytes48 // BLS public key
	TargetPubKey  common.Bytes48 // BLS public key
}

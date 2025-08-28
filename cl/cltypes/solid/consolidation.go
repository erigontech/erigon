package solid

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	_ EncodableHashableSSZ = (*ConsolidationRequest)(nil)
	_ ssz2.SizedObjectSSZ  = (*ConsolidationRequest)(nil)

	_ EncodableHashableSSZ = (*PendingConsolidation)(nil)
	_ ssz2.SizedObjectSSZ  = (*PendingConsolidation)(nil)
)

const (
	SizeConsolidationRequest = length.Addr + length.Bytes48 + length.Bytes48
	SizePendingConsolidation = 8 + 8
)

type ConsolidationRequest struct {
	SourceAddress common.Address `json:"source_address"`
	SourcePubKey  common.Bytes48 `json:"source_pubkey"` // BLS public key
	TargetPubKey  common.Bytes48 `json:"target_pubkey"` // BLS public key
}

func (p *ConsolidationRequest) EncodingSizeSSZ() int {
	return SizeConsolidationRequest
}

func (p *ConsolidationRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.SourceAddress[:], p.SourcePubKey[:], p.TargetPubKey[:])
}

func (p *ConsolidationRequest) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, p.SourceAddress[:], p.SourcePubKey[:], p.TargetPubKey[:])
}

func (p *ConsolidationRequest) Clone() clonable.Clonable {
	return &ConsolidationRequest{}
}

func (p *ConsolidationRequest) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.SourceAddress[:], p.SourcePubKey[:], p.TargetPubKey[:])
}

func (p *ConsolidationRequest) Static() bool {
	return true
}

type PendingConsolidation struct {
	SourceIndex uint64 `json:"source_index"` // validator index
	TargetIndex uint64 `json:"target_index"` // validator index
}

func (p *PendingConsolidation) EncodingSizeSSZ() int {
	return 16
}

func (p *PendingConsolidation) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, &p.SourceIndex, &p.TargetIndex)
}

func (p *PendingConsolidation) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &p.SourceIndex, &p.TargetIndex)
}

func (p *PendingConsolidation) Clone() clonable.Clonable {
	return &PendingConsolidation{}
}

func (p *PendingConsolidation) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(&p.SourceIndex, &p.TargetIndex)
}

func (p *PendingConsolidation) Static() bool {
	return true
}

func NewPendingConsolidationList(cfg *clparams.BeaconChainConfig) *ListSSZ[*PendingConsolidation] {
	return NewStaticListSSZ[*PendingConsolidation](int(cfg.PendingConsolidationsLimit), SizePendingConsolidation)
}

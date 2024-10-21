package solid

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
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
	return 0
}

func (p *PendingConsolidation) EncodeSSZ(buf []byte) ([]byte, error) {
	return nil, nil
}

func (p *PendingConsolidation) DecodeSSZ(buf []byte, version int) error {
	return nil
}

func (p PendingConsolidation) Clone() clonable.Clonable {
	return &PendingConsolidation{
		SourceIndex: p.SourceIndex,
		TargetIndex: p.TargetIndex,
	}
}

func (p *PendingConsolidation) HashSSZ() ([32]byte, error) {
	//	return ssz.Hash(p)
	return [32]byte{}, nil
}

type ConsolidationRequest struct {
	SourceAddress common.Address
	SourcePubKey  common.Bytes48 // BLS public key
	TargetPubKey  common.Bytes48 // BLS public key
}

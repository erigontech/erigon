package solid

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	_ ssz.EncodableSSZ = (*PendingPartialWithdrawal)(nil)
	_ ssz.HashableSSZ  = (*PendingPartialWithdrawal)(nil)
)

const (
	SizePendingPartialWithdrawal = 8 + 8 + 8
)

type PendingPartialWithdrawal struct {
	Index             uint64 // validator index
	Amount            uint64 // Gwei
	WithdrawableEpoch uint64
}

func (p *PendingPartialWithdrawal) EncodingSizeSSZ() int {
	return SizePendingPartialWithdrawal
}

func (p *PendingPartialWithdrawal) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.Index, p.Amount, p.WithdrawableEpoch)
}

func (p *PendingPartialWithdrawal) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &p.Index, &p.Amount, &p.WithdrawableEpoch)
}

func (p PendingPartialWithdrawal) Clone() clonable.Clonable {
	return &PendingPartialWithdrawal{}
}

func (p *PendingPartialWithdrawal) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.Index, p.Amount, p.WithdrawableEpoch)
}

type WithdrawalRequest struct {
	SourceAddress   common.Address
	ValidatorPubKey common.Bytes48 // BLS public key
	Amount          uint64         // Gwei
}

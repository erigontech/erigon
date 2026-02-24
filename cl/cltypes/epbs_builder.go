package cltypes

import (
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/ssz"
)

var (
	_ ssz.HashableSSZ = (*Builder)(nil)
	_ ssz.HashableSSZ = (*BuilderPendingWithdrawal)(nil)
	_ ssz.HashableSSZ = (*BuilderPendingPayment)(nil)

	_ ssz2.SizedObjectSSZ = (*Builder)(nil)
	_ ssz2.SizedObjectSSZ = (*BuilderPendingWithdrawal)(nil)
	_ ssz2.SizedObjectSSZ = (*BuilderPendingPayment)(nil)
)

// Builder represents a builder in the consensus layer.
type Builder struct {
	Pubkey            common.Bytes48 `json:"pubkey"`
	Version           uint8          `json:"version"`
	ExecutionAddress  common.Address `json:"execution_address"`
	Balance           uint64         `json:"balance"`
	DepositEpoch      uint64         `json:"deposit_epoch"`
	WithdrawableEpoch uint64         `json:"withdrawable_epoch"`
}

func (b *Builder) EncodingSizeSSZ() int {
	return length.Bytes48 + 1 + length.Addr + 8 + 8 + 8
}

func (b *Builder) Static() bool {
	return true
}

func (b *Builder) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.Pubkey[:], b.Version, b.ExecutionAddress[:], b.Balance, b.DepositEpoch, b.WithdrawableEpoch)
}

func (b *Builder) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.Pubkey[:], &b.Version, b.ExecutionAddress[:], &b.Balance, &b.DepositEpoch, &b.WithdrawableEpoch)
}

func (b *Builder) Clone() clonable.Clonable {
	return &Builder{}
}

func (b *Builder) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Pubkey[:], uint64(b.Version), b.ExecutionAddress[:], b.Balance, b.DepositEpoch, b.WithdrawableEpoch)
}

// BuilderPendingWithdrawal represents a pending withdrawal for a builder.
type BuilderPendingWithdrawal struct {
	FeeRecipient common.Address `json:"fee_recipient"`
	Amount       uint64         `json:"amount"`
	BuilderIndex uint64         `json:"builder_index"`
}

func (b *BuilderPendingWithdrawal) EncodingSizeSSZ() int {
	return length.Addr + 8 + 8
}

func (b *BuilderPendingWithdrawal) Static() bool {
	return true
}

func (b *BuilderPendingWithdrawal) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.FeeRecipient[:], b.Amount, b.BuilderIndex)
}

func (b *BuilderPendingWithdrawal) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.FeeRecipient[:], &b.Amount, &b.BuilderIndex)
}

func (b *BuilderPendingWithdrawal) Clone() clonable.Clonable {
	return &BuilderPendingWithdrawal{}
}

func (b *BuilderPendingWithdrawal) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.FeeRecipient[:], b.Amount, b.BuilderIndex)
}

// BuilderPendingPayment represents a pending payment for a builder.
type BuilderPendingPayment struct {
	Weight     uint64                    `json:"weight"`
	Withdrawal *BuilderPendingWithdrawal `json:"withdrawal"`
}

func (b *BuilderPendingPayment) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(&b.Weight, b.Withdrawal)
}

func (b *BuilderPendingPayment) EncodingSizeSSZ() int {
	if b.Withdrawal == nil {
		return 8
	}
	return 8 + b.Withdrawal.EncodingSizeSSZ()
}

func (b *BuilderPendingPayment) Static() bool {
	return true
}

func (b *BuilderPendingPayment) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.Weight, b.Withdrawal)
}

func (b *BuilderPendingPayment) DecodeSSZ(buf []byte, version int) error {
	b.Withdrawal = new(BuilderPendingWithdrawal)
	return ssz2.UnmarshalSSZ(buf, version, &b.Weight, b.Withdrawal)
}

func (b *BuilderPendingPayment) Clone() clonable.Clonable {
	return &BuilderPendingPayment{
		Weight:     0,
		Withdrawal: &BuilderPendingWithdrawal{},
	}
}

package solid

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	_ ssz.EncodableSSZ = (*PendingDeposit)(nil)
	_ ssz.HashableSSZ  = (*PendingDeposit)(nil)
)

const (
	SizePendingDeposit = length.Bytes48 + length.Hash + 8 + length.Bytes96 + 8
)

type PendingDeposit struct {
	PubKey                common.Bytes48 // BLS public key
	WithdrawalCredentials common.Hash
	Amount                uint64         // Gwei
	Signature             common.Bytes96 // BLS signature
	Slot                  uint64
}

func (p *PendingDeposit) EncodingSizeSSZ() int {
	return SizePendingDeposit
}

func (p *PendingDeposit) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.PubKey, p.WithdrawalCredentials, p.Amount, p.Signature, p.Slot)
}

func (p *PendingDeposit) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:], &p.Slot)
}

func (p PendingDeposit) Clone() clonable.Clonable {
	return &PendingDeposit{}
}

func (p *PendingDeposit) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.PubKey, p.WithdrawalCredentials, p.Amount, p.Signature, p.Slot)
}

type DepositRequest struct {
	PubKey                common.Bytes48 // BLS public key
	WithdrawalCredentials common.Hash
	Amount                uint64         // Gwei
	Signature             common.Bytes96 // BLS signature
	Index                 uint64
}

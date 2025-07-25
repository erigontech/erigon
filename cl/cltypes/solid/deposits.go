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
	_ EncodableHashableSSZ = (*DepositRequest)(nil)
	_ ssz2.SizedObjectSSZ  = (*DepositRequest)(nil)

	_ EncodableHashableSSZ = (*PendingDeposit)(nil)
	_ ssz2.SizedObjectSSZ  = (*PendingDeposit)(nil)
)

const (
	SizeDepositRequest = length.Bytes48 + length.Hash + 8 + length.Bytes96 + 8
	SizePendingDeposit = length.Bytes48 + length.Hash + 8 + length.Bytes96 + 8
)

type DepositRequest struct {
	PubKey                common.Bytes48 `json:"pubkey"` // BLS public key
	WithdrawalCredentials common.Hash    `json:"withdrawal_credentials"`
	Amount                uint64         `json:"amount"`    // Gwei
	Signature             common.Bytes96 `json:"signature"` // BLS signature
	Index                 uint64         `json:"index"`     // validator index
}

func (p *DepositRequest) EncodingSizeSSZ() int {
	return SizeDepositRequest
}

func (p *DepositRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:], &p.Index)
}

func (p *DepositRequest) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:], &p.Index)
}

func (p *DepositRequest) Clone() clonable.Clonable {
	return &DepositRequest{}
}

func (p *DepositRequest) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:], &p.Index)
}

func (p *DepositRequest) Static() bool {
	return true
}

type PendingDeposit struct {
	PubKey                common.Bytes48 `json:"pubkey"` // BLS public key
	WithdrawalCredentials common.Hash    `json:"withdrawal_credentials"`
	Amount                uint64         `json:"amount"`    // Gwei
	Signature             common.Bytes96 `json:"signature"` // BLS signature
	Slot                  uint64         `json:"slot"`
}

func (p *PendingDeposit) EncodingSizeSSZ() int {
	return SizePendingDeposit
}

func (p *PendingDeposit) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:], &p.Slot)
}

func (p *PendingDeposit) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:], &p.Slot)
}

func (p *PendingDeposit) Clone() clonable.Clonable {
	return &PendingDeposit{}
}

func (p *PendingDeposit) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:], &p.Slot)
}

func (p *PendingDeposit) Static() bool {
	return true
}

func NewPendingDepositList(cfg *clparams.BeaconChainConfig) *ListSSZ[*PendingDeposit] {
	return NewStaticListSSZ[*PendingDeposit](int(cfg.PendingDepositLimits), SizePendingDeposit)
}

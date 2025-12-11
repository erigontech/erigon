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
	_ EncodableHashableSSZ = (*WithdrawalRequest)(nil)
	_ ssz2.SizedObjectSSZ  = (*WithdrawalRequest)(nil)

	_ EncodableHashableSSZ = (*PendingPartialWithdrawal)(nil)
	_ ssz2.SizedObjectSSZ  = (*PendingPartialWithdrawal)(nil)
)

const (
	SizeWithdrawalRequest        = length.Addr + length.Bytes48 + 8
	SizePendingPartialWithdrawal = 8 + 8 + 8
)

type WithdrawalRequest struct {
	SourceAddress   common.Address `json:"source_address"`
	ValidatorPubKey common.Bytes48 `json:"validator_pubkey"` // BLS public key
	Amount          uint64         `json:"amount"`           // Gwei
}

func (p *WithdrawalRequest) EncodingSizeSSZ() int {
	return SizeWithdrawalRequest
}

func (p *WithdrawalRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.SourceAddress[:], p.ValidatorPubKey[:], &p.Amount)
}

func (p *WithdrawalRequest) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, p.SourceAddress[:], p.ValidatorPubKey[:], &p.Amount)
}

func (p *WithdrawalRequest) Clone() clonable.Clonable {
	return &WithdrawalRequest{}
}

func (p *WithdrawalRequest) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.SourceAddress[:], p.ValidatorPubKey[:], &p.Amount)
}

func (p *WithdrawalRequest) Static() bool {
	return true
}

type PendingPartialWithdrawal struct {
	ValidatorIndex    uint64 `json:"validator_index"`    // validator index
	Amount            uint64 `json:"amount"`             // Gwei
	WithdrawableEpoch uint64 `json:"withdrawable_epoch"` // epoch when the withdrawal can be processed
}

func (p *PendingPartialWithdrawal) EncodingSizeSSZ() int {
	return SizePendingPartialWithdrawal
}

func (p *PendingPartialWithdrawal) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, &p.ValidatorIndex, &p.Amount, &p.WithdrawableEpoch)
}

func (p *PendingPartialWithdrawal) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &p.ValidatorIndex, &p.Amount, &p.WithdrawableEpoch)
}

func (p *PendingPartialWithdrawal) Clone() clonable.Clonable {
	return &PendingPartialWithdrawal{}
}

func (p *PendingPartialWithdrawal) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(&p.ValidatorIndex, &p.Amount, &p.WithdrawableEpoch)
}

func (p *PendingPartialWithdrawal) Static() bool {
	return true
}

func NewPendingWithdrawalList(cfg *clparams.BeaconChainConfig) *ListSSZ[*PendingPartialWithdrawal] {
	return NewStaticListSSZ[*PendingPartialWithdrawal](int(cfg.PendingPartialWithdrawalsLimit), SizePendingPartialWithdrawal)
}

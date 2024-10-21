package solid

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
)

var (
	_ ssz.EncodableSSZ = (*PendingPartialWithdrawal)(nil)
	_ ssz.HashableSSZ  = (*PendingPartialWithdrawal)(nil)
)

type PendingPartialWithdrawal struct {
	Index             uint64 // validator index
	Amount            uint64 // Gwei
	WithdrawableEpoch uint64
}

func (p *PendingPartialWithdrawal) EncodingSizeSSZ() int {
	return 0
}

func (p *PendingPartialWithdrawal) EncodeSSZ(buf []byte) ([]byte, error) {
	return nil, nil
}

func (p *PendingPartialWithdrawal) DecodeSSZ(buf []byte, version int) error {
	return nil
}

func (p PendingPartialWithdrawal) Clone() clonable.Clonable {
	return &PendingPartialWithdrawal{
		Index:             p.Index,
		Amount:            p.Amount,
		WithdrawableEpoch: p.WithdrawableEpoch,
	}
}

func (p *PendingPartialWithdrawal) HashSSZ() ([32]byte, error) {
	//	return ssz.Hash(p)
	return [32]byte{}, nil
}

type WithdrawalRequest struct {
	SourceAddress   common.Address
	ValidatorPubKey common.Bytes48 // BLS public key
	Amount          uint64         // Gwei
}

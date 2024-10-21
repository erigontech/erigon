package solid

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
)

var (
	_ ssz.EncodableSSZ = (*PendingDeposit)(nil)
	_ ssz.HashableSSZ  = (*PendingDeposit)(nil)
)

type PendingDeposit struct {
	PubKey                common.Bytes48 // BLS public key
	WithdrawalCredentials common.Hash
	Amount                uint64         // Gwei
	Signature             common.Bytes96 // BLS signature
	Slot                  uint64
}

func (p *PendingDeposit) EncodingSizeSSZ() int {
	return 0
}

func (p *PendingDeposit) EncodeSSZ(buf []byte) ([]byte, error) {
	return nil, nil
}

func (p *PendingDeposit) DecodeSSZ(buf []byte, version int) error {
	return nil
}

func (p PendingDeposit) Clone() clonable.Clonable {
	return &PendingDeposit{
		PubKey:                p.PubKey,
		WithdrawalCredentials: p.WithdrawalCredentials,
		Amount:                p.Amount,
		Signature:             p.Signature,
		Slot:                  p.Slot,
	}
}

func (p *PendingDeposit) HashSSZ() ([32]byte, error) {
	//	return ssz.Hash(p)
	return [32]byte{}, nil
}

type DepositRequest struct {
	PubKey                common.Bytes48 // BLS public key
	WithdrawalCredentials common.Hash
	Amount                uint64         // Gwei
	Signature             common.Bytes96 // BLS signature
	Index                 uint64
}

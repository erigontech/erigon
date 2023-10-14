package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/core/types"
)

type Withdrawal struct {
	Index     uint64            `json:"index"`          // monotonically increasing identifier issued by consensus layer
	Validator uint64            `json:"validatorIndex"` // index of validator associated with withdrawal
	Address   libcommon.Address `json:"address"`        // target address for withdrawn ether
	Amount    uint64            `json:"amount"`         // value of withdrawal in GWei
}

func (obj *Withdrawal) EncodeSSZ(buf []byte) ([]byte, error) {
	buf = append(buf, ssz.Uint64SSZ(obj.Index)...)
	buf = append(buf, ssz.Uint64SSZ(obj.Validator)...)
	buf = append(buf, obj.Address[:]...)
	buf = append(buf, ssz.Uint64SSZ(obj.Amount)...)
	return buf, nil
}

func (obj *Withdrawal) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < obj.EncodingSizeSSZ() {
		return fmt.Errorf("[Withdrawal] err: %s", ssz.ErrLowBufferSize)
	}
	obj.Index = ssz.UnmarshalUint64SSZ(buf)
	obj.Validator = ssz.UnmarshalUint64SSZ(buf[8:])
	copy(obj.Address[:], buf[16:])
	obj.Amount = ssz.UnmarshalUint64SSZ(buf[36:])
	return nil
}

func (obj *Withdrawal) EncodingSizeSSZ() int {
	// Validator Index (8 bytes) + Index (8 bytes) + Amount (8 bytes) + address length
	return 24 + length.Addr
}

func (obj *Withdrawal) HashSSZ() ([32]byte, error) { // the [32]byte is temporary
	return merkle_tree.HashTreeRoot(obj.Index, obj.Validator, obj.Address[:], obj.Amount)
}

func convertExecutionWithdrawalToConsensusWithdrawal(executionWithdrawal *types.Withdrawal) *Withdrawal {
	return &Withdrawal{
		Index:     executionWithdrawal.Index,
		Validator: executionWithdrawal.Validator,
		Address:   executionWithdrawal.Address,
		Amount:    executionWithdrawal.Amount,
	}
}

func convertConsensusWithdrawalToExecutionWithdrawal(consensusWithdrawal *Withdrawal) *types.Withdrawal {
	return &types.Withdrawal{
		Index:     consensusWithdrawal.Index,
		Validator: consensusWithdrawal.Validator,
		Address:   consensusWithdrawal.Address,
		Amount:    consensusWithdrawal.Amount,
	}
}

func convertExecutionWithdrawalsToConsensusWithdrawals(executionWithdrawal []*types.Withdrawal) []*Withdrawal {
	ret := make([]*Withdrawal, len(executionWithdrawal))
	for i, w := range executionWithdrawal {
		ret[i] = convertExecutionWithdrawalToConsensusWithdrawal(w)
	}
	return ret
}

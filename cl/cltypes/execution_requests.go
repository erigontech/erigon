package cltypes

import (
	"crypto/sha256"
	"encoding/json"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	_ solid.EncodableHashableSSZ = (*ExecutionRequests)(nil)
	_ ssz2.SizedObjectSSZ        = (*ExecutionRequests)(nil)
)

// class ExecutionRequests(Container):
//
//	deposits: List[DepositRequest, MAX_DEPOSIT_REQUESTS_PER_PAYLOAD]  # [New in Electra:EIP6110]
//	withdrawals: List[WithdrawalRequest, MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD]  # [New in Electra:EIP7002:EIP7251]
//	consolidations: List[ConsolidationRequest, MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD]  # [New in Electra:EIP7251]
type ExecutionRequests struct {
	Deposits       *solid.ListSSZ[*solid.DepositRequest]       `json:"deposits"`
	Withdrawals    *solid.ListSSZ[*solid.WithdrawalRequest]    `json:"withdrawals"`
	Consolidations *solid.ListSSZ[*solid.ConsolidationRequest] `json:"consolidations"`

	cfg *clparams.BeaconChainConfig
}

func NewExecutionRequests(cfg *clparams.BeaconChainConfig) *ExecutionRequests {
	return &ExecutionRequests{
		Deposits:       solid.NewStaticListSSZ[*solid.DepositRequest](int(cfg.MaxDepositRequestsPerPayload), solid.SizeDepositRequest),
		Withdrawals:    solid.NewStaticListSSZ[*solid.WithdrawalRequest](int(cfg.MaxWithdrawalRequestsPerPayload), solid.SizeWithdrawalRequest),
		Consolidations: solid.NewStaticListSSZ[*solid.ConsolidationRequest](int(cfg.MaxConsolidationRequestsPerPayload), solid.SizeConsolidationRequest),
		cfg:            cfg,
	}
}

func (e *ExecutionRequests) EncodingSizeSSZ() int {
	return e.Deposits.EncodingSizeSSZ() + e.Withdrawals.EncodingSizeSSZ() + e.Consolidations.EncodingSizeSSZ()
}

func (e *ExecutionRequests) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, e.Deposits, e.Withdrawals, e.Consolidations)
}

func (e *ExecutionRequests) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, e.Deposits, e.Withdrawals, e.Consolidations)
}

func (e *ExecutionRequests) Clone() clonable.Clonable {
	return NewExecutionRequests(e.cfg)
}

func (e *ExecutionRequests) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(e.Deposits, e.Withdrawals, e.Consolidations)
}

func (e *ExecutionRequests) Static() bool {
	return false
}

func (e *ExecutionRequests) UnmarshalJSON(b []byte) error {
	c := struct {
		Deposits       *solid.ListSSZ[*solid.DepositRequest]       `json:"deposits"`
		Withdrawals    *solid.ListSSZ[*solid.WithdrawalRequest]    `json:"withdrawals"`
		Consolidations *solid.ListSSZ[*solid.ConsolidationRequest] `json:"consolidations"`
	}{
		Deposits:       solid.NewStaticListSSZ[*solid.DepositRequest](int(e.cfg.MaxDepositRequestsPerPayload), solid.SizeDepositRequest),
		Withdrawals:    solid.NewStaticListSSZ[*solid.WithdrawalRequest](int(e.cfg.MaxWithdrawalRequestsPerPayload), solid.SizeWithdrawalRequest),
		Consolidations: solid.NewStaticListSSZ[*solid.ConsolidationRequest](int(e.cfg.MaxConsolidationRequestsPerPayload), solid.SizeConsolidationRequest),
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return err
	}

	e.Deposits = c.Deposits
	e.Withdrawals = c.Withdrawals
	e.Consolidations = c.Consolidations
	return nil
}

func ComputeExecutionRequestHash(executionRequests []hexutil.Bytes) common.Hash {
	sha := sha256.New()
	for _, r := range executionRequests {
		hi := sha256.Sum256(r)
		sha.Write(hi[:])
	}
	h := common.BytesToHash(sha.Sum(nil))
	return h
}

package cltypes

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
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
//	builder_deposits: List[BuilderDepositRequest, MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD]  # [New in Gloas:EIP8282]
//	builder_exits: List[BuilderExitRequest, MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD]  # [New in Gloas:EIP8282]
type ExecutionRequests struct {
	Deposits        *solid.ListSSZ[*solid.DepositRequest]        `json:"deposits"`
	Withdrawals     *solid.ListSSZ[*solid.WithdrawalRequest]     `json:"withdrawals"`
	Consolidations  *solid.ListSSZ[*solid.ConsolidationRequest]  `json:"consolidations"`
	BuilderDeposits *solid.ListSSZ[*solid.BuilderDepositRequest] `json:"builder_deposits"`
	BuilderExits    *solid.ListSSZ[*solid.BuilderExitRequest]    `json:"builder_exits"`

	cfg     *clparams.BeaconChainConfig
	version clparams.StateVersion
}

func NewExecutionRequests(cfg *clparams.BeaconChainConfig) *ExecutionRequests {
	return NewExecutionRequestsWithVersion(cfg, clparams.ElectraVersion)
}

func NewExecutionRequestsWithVersion(cfg *clparams.BeaconChainConfig, version clparams.StateVersion) *ExecutionRequests {
	e := &ExecutionRequests{cfg: cfg, version: version}
	e.ensureLists()
	return e
}

func (e *ExecutionRequests) effectiveVersion() clparams.StateVersion {
	if e.version == 0 {
		return clparams.GloasVersion
	}
	return e.version
}

func (e *ExecutionRequests) ensureLists() {
	if e.cfg == nil {
		e.cfg = &clparams.MainnetBeaconConfig
	}
	if e.Deposits == nil {
		e.Deposits = solid.NewStaticListSSZ[*solid.DepositRequest](int(e.cfg.MaxDepositRequestsPerPayload), solid.SizeDepositRequest)
	}
	if e.Withdrawals == nil {
		e.Withdrawals = solid.NewStaticListSSZ[*solid.WithdrawalRequest](int(e.cfg.MaxWithdrawalRequestsPerPayload), solid.SizeWithdrawalRequest)
	}
	if e.Consolidations == nil {
		e.Consolidations = solid.NewStaticListSSZ[*solid.ConsolidationRequest](int(e.cfg.MaxConsolidationRequestsPerPayload), solid.SizeConsolidationRequest)
	}
	if e.BuilderDeposits == nil {
		e.BuilderDeposits = solid.NewStaticListSSZ[*solid.BuilderDepositRequest](int(e.cfg.MaxBuilderDepositRequestsPerPayload), solid.SizeBuilderDepositRequest)
	}
	if e.BuilderExits == nil {
		e.BuilderExits = solid.NewStaticListSSZ[*solid.BuilderExitRequest](int(e.cfg.MaxBuilderExitRequestsPerPayload), solid.SizeBuilderExitRequest)
	}
}

func (e *ExecutionRequests) EncodingSizeSSZ() int {
	e.ensureLists()
	if e.effectiveVersion() < clparams.GloasVersion {
		return e.Deposits.EncodingSizeSSZ() + e.Withdrawals.EncodingSizeSSZ() + e.Consolidations.EncodingSizeSSZ()
	}
	return e.Deposits.EncodingSizeSSZ() + e.Withdrawals.EncodingSizeSSZ() + e.Consolidations.EncodingSizeSSZ() + e.BuilderDeposits.EncodingSizeSSZ() + e.BuilderExits.EncodingSizeSSZ()
}

func (e *ExecutionRequests) EncodeSSZ(buf []byte) ([]byte, error) {
	e.ensureLists()
	if e.effectiveVersion() < clparams.GloasVersion {
		return ssz2.MarshalSSZ(buf, e.Deposits, e.Withdrawals, e.Consolidations)
	}
	return ssz2.MarshalSSZ(buf, e.Deposits, e.Withdrawals, e.Consolidations, e.BuilderDeposits, e.BuilderExits)
}

func (e *ExecutionRequests) DecodeSSZ(buf []byte, version int) error {
	e.version = clparams.StateVersion(version)
	e.ensureLists()
	if e.effectiveVersion() < clparams.GloasVersion {
		return ssz2.UnmarshalSSZ(buf, version, e.Deposits, e.Withdrawals, e.Consolidations)
	}
	return ssz2.UnmarshalSSZ(buf, version, e.Deposits, e.Withdrawals, e.Consolidations, e.BuilderDeposits, e.BuilderExits)
}

func (e *ExecutionRequests) Clone() clonable.Clonable {
	return NewExecutionRequestsWithVersion(e.cfg, e.version)
}

func (e *ExecutionRequests) HashSSZ() ([32]byte, error) {
	e.ensureLists()
	if e.effectiveVersion() < clparams.GloasVersion {
		return merkle_tree.HashTreeRoot(e.Deposits, e.Withdrawals, e.Consolidations)
	}
	return merkle_tree.HashTreeRoot(e.Deposits, e.Withdrawals, e.Consolidations, e.BuilderDeposits, e.BuilderExits)
}

func (e *ExecutionRequests) Static() bool {
	return false
}

func (e *ExecutionRequests) UnmarshalJSON(b []byte) error {
	e.ensureLists()
	c := struct {
		Deposits        *solid.ListSSZ[*solid.DepositRequest]        `json:"deposits"`
		Withdrawals     *solid.ListSSZ[*solid.WithdrawalRequest]     `json:"withdrawals"`
		Consolidations  *solid.ListSSZ[*solid.ConsolidationRequest]  `json:"consolidations"`
		BuilderDeposits *solid.ListSSZ[*solid.BuilderDepositRequest] `json:"builder_deposits"`
		BuilderExits    *solid.ListSSZ[*solid.BuilderExitRequest]    `json:"builder_exits"`
	}{
		Deposits:        solid.NewStaticListSSZ[*solid.DepositRequest](int(e.cfg.MaxDepositRequestsPerPayload), solid.SizeDepositRequest),
		Withdrawals:     solid.NewStaticListSSZ[*solid.WithdrawalRequest](int(e.cfg.MaxWithdrawalRequestsPerPayload), solid.SizeWithdrawalRequest),
		Consolidations:  solid.NewStaticListSSZ[*solid.ConsolidationRequest](int(e.cfg.MaxConsolidationRequestsPerPayload), solid.SizeConsolidationRequest),
		BuilderDeposits: solid.NewStaticListSSZ[*solid.BuilderDepositRequest](int(e.cfg.MaxBuilderDepositRequestsPerPayload), solid.SizeBuilderDepositRequest),
		BuilderExits:    solid.NewStaticListSSZ[*solid.BuilderExitRequest](int(e.cfg.MaxBuilderExitRequestsPerPayload), solid.SizeBuilderExitRequest),
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return err
	}

	e.Deposits = c.Deposits
	e.Withdrawals = c.Withdrawals
	e.Consolidations = c.Consolidations
	e.BuilderDeposits = c.BuilderDeposits
	e.BuilderExits = c.BuilderExits
	if e.effectiveVersion() < clparams.GloasVersion && (e.BuilderDeposits.Len() > 0 || e.BuilderExits.Len() > 0) {
		return fmt.Errorf("builder execution requests before gloas")
	}
	return nil
}

func DecodeExecutionRequestsList(cfg *clparams.BeaconChainConfig, requests []hexutil.Bytes, version clparams.StateVersion) (*ExecutionRequests, error) {
	out := NewExecutionRequestsWithVersion(cfg, version)
	lastType := -1
	for i, request := range requests {
		if len(request) <= 1 {
			return nil, fmt.Errorf("execution request %d has no request data", i)
		}
		requestType := int(request[0])
		if requestType <= lastType {
			return nil, fmt.Errorf("execution request type %d is not strictly ascending", request[0])
		}
		lastType = requestType
		data := request[1:]
		switch request[0] {
		case byte(cfg.DepositRequestType):
			if err := out.Deposits.DecodeSSZ(data, int(version)); err != nil {
				return nil, err
			}
		case byte(cfg.WithdrawalRequestType):
			if err := out.Withdrawals.DecodeSSZ(data, int(version)); err != nil {
				return nil, err
			}
		case byte(cfg.ConsolidationRequestType):
			if err := out.Consolidations.DecodeSSZ(data, int(version)); err != nil {
				return nil, err
			}
		case byte(cfg.BuilderDepositRequestType):
			if version < clparams.GloasVersion {
				return nil, fmt.Errorf("builder deposit request before gloas")
			}
			if err := out.BuilderDeposits.DecodeSSZ(data, int(version)); err != nil {
				return nil, err
			}
		case byte(cfg.BuilderExitRequestType):
			if version < clparams.GloasVersion {
				return nil, fmt.Errorf("builder exit request before gloas")
			}
			if err := out.BuilderExits.DecodeSSZ(data, int(version)); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown execution request type %d", request[0])
		}
	}
	return out, nil
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

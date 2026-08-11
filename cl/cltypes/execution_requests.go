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

// ExecutionRequests groups execution-layer requests carried by a payload.
// Electra defines deposits, withdrawals, and consolidations; Gloas adds builder
// deposits and exits.
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
	if cfg == nil {
		panic("execution requests beacon config is nil")
	}
	e := &ExecutionRequests{cfg: cfg, version: version}
	e.ensureLists()
	return e
}

func (e *ExecutionRequests) effectiveVersion() clparams.StateVersion {
	if e.version == 0 {
		return clparams.ElectraVersion
	}
	return e.version
}

func (e *ExecutionRequests) Version() clparams.StateVersion {
	return e.effectiveVersion()
}

func (e *ExecutionRequests) ensureLists() {
	if e.cfg == nil {
		panic("execution requests beacon config is nil")
	}
	progressive := e.effectiveVersion() >= clparams.GloasVersion
	if e.Deposits == nil && progressive {
		e.Deposits = solid.NewStaticProgressiveListSSZ[*solid.DepositRequest](int(e.cfg.MaxDepositRequestsPerPayload), solid.SizeDepositRequest)
	} else if e.Deposits == nil {
		e.Deposits = solid.NewStaticListSSZ[*solid.DepositRequest](int(e.cfg.MaxDepositRequestsPerPayload), solid.SizeDepositRequest)
	}
	if e.Withdrawals == nil && progressive {
		e.Withdrawals = solid.NewStaticProgressiveListSSZ[*solid.WithdrawalRequest](int(e.cfg.MaxWithdrawalRequestsPerPayload), solid.SizeWithdrawalRequest)
	} else if e.Withdrawals == nil {
		e.Withdrawals = solid.NewStaticListSSZ[*solid.WithdrawalRequest](int(e.cfg.MaxWithdrawalRequestsPerPayload), solid.SizeWithdrawalRequest)
	}
	if e.Consolidations == nil && progressive {
		e.Consolidations = solid.NewStaticProgressiveListSSZ[*solid.ConsolidationRequest](int(e.cfg.MaxConsolidationRequestsPerPayload), solid.SizeConsolidationRequest)
	} else if e.Consolidations == nil {
		e.Consolidations = solid.NewStaticListSSZ[*solid.ConsolidationRequest](int(e.cfg.MaxConsolidationRequestsPerPayload), solid.SizeConsolidationRequest)
	}
	if e.BuilderDeposits == nil && progressive {
		e.BuilderDeposits = solid.NewStaticProgressiveListSSZ[*solid.BuilderDepositRequest](int(e.cfg.MaxBuilderDepositRequestsPerPayload), solid.SizeBuilderDepositRequest)
	} else if e.BuilderDeposits == nil {
		e.BuilderDeposits = solid.NewStaticListSSZ[*solid.BuilderDepositRequest](int(e.cfg.MaxBuilderDepositRequestsPerPayload), solid.SizeBuilderDepositRequest)
	}
	if e.BuilderExits == nil && progressive {
		e.BuilderExits = solid.NewStaticProgressiveListSSZ[*solid.BuilderExitRequest](int(e.cfg.MaxBuilderExitRequestsPerPayload), solid.SizeBuilderExitRequest)
	} else if e.BuilderExits == nil {
		e.BuilderExits = solid.NewStaticListSSZ[*solid.BuilderExitRequest](int(e.cfg.MaxBuilderExitRequestsPerPayload), solid.SizeBuilderExitRequest)
	}
}

func (e *ExecutionRequests) EncodingSizeSSZ() int {
	e.ensureLists()
	// Every field is a dynamic list, so each contributes a 4-byte offset.
	const dynamicOffsetSize = 4
	size := 3*dynamicOffsetSize +
		e.Deposits.EncodingSizeSSZ() +
		e.Withdrawals.EncodingSizeSSZ() +
		e.Consolidations.EncodingSizeSSZ()
	if e.effectiveVersion() < clparams.GloasVersion {
		return size
	}
	return size +
		2*dynamicOffsetSize +
		e.BuilderDeposits.EncodingSizeSSZ() +
		e.BuilderExits.EncodingSizeSSZ()
}

func (e *ExecutionRequests) EncodeSSZ(buf []byte) ([]byte, error) {
	e.ensureLists()
	if e.effectiveVersion() < clparams.GloasVersion {
		return ssz2.MarshalSSZ(buf, e.Deposits, e.Withdrawals, e.Consolidations)
	}
	return ssz2.MarshalSSZ(buf, e.Deposits, e.Withdrawals, e.Consolidations, e.BuilderDeposits, e.BuilderExits)
}

func (e *ExecutionRequests) DecodeSSZ(buf []byte, version int) error {
	return e.decodeSSZ(buf, version, false)
}

func (e *ExecutionRequests) DecodeSSZStrict(buf []byte, version int) error {
	return e.decodeSSZ(buf, version, true)
}

func (e *ExecutionRequests) decodeSSZ(buf []byte, version int, strict bool) error {
	decodedVersion := clparams.StateVersion(version)
	if (e.effectiveVersion() >= clparams.GloasVersion) != (decodedVersion >= clparams.GloasVersion) {
		e.Deposits = nil
		e.Withdrawals = nil
		e.Consolidations = nil
		e.BuilderDeposits = nil
		e.BuilderExits = nil
	}
	e.version = decodedVersion
	e.ensureLists()
	schema := []any{e.Deposits, e.Withdrawals, e.Consolidations}
	if e.effectiveVersion() >= clparams.GloasVersion {
		schema = append(schema, e.BuilderDeposits, e.BuilderExits)
	}
	if strict {
		return ssz2.UnmarshalSSZStrict(buf, version, schema...)
	}
	return ssz2.UnmarshalSSZ(buf, version, schema...)
}

func (e *ExecutionRequests) Clone() clonable.Clonable {
	e.ensureLists()
	out := NewExecutionRequestsWithVersion(e.cfg, e.effectiveVersion())
	e.Deposits.Range(func(_ int, request *solid.DepositRequest, _ int) bool {
		if request == nil {
			return true
		}
		copied := *request
		out.Deposits.Append(&copied)
		return true
	})
	e.Withdrawals.Range(func(_ int, request *solid.WithdrawalRequest, _ int) bool {
		if request == nil {
			return true
		}
		copied := *request
		out.Withdrawals.Append(&copied)
		return true
	})
	e.Consolidations.Range(func(_ int, request *solid.ConsolidationRequest, _ int) bool {
		if request == nil {
			return true
		}
		copied := *request
		out.Consolidations.Append(&copied)
		return true
	})
	e.BuilderDeposits.Range(func(_ int, request *solid.BuilderDepositRequest, _ int) bool {
		if request == nil {
			return true
		}
		copied := *request
		out.BuilderDeposits.Append(&copied)
		return true
	})
	e.BuilderExits.Range(func(_ int, request *solid.BuilderExitRequest, _ int) bool {
		if request == nil {
			return true
		}
		copied := *request
		out.BuilderExits.Append(&copied)
		return true
	})
	return out
}

func (e *ExecutionRequests) HashSSZ() ([32]byte, error) {
	e.ensureLists()
	if e.effectiveVersion() < clparams.GloasVersion {
		return merkle_tree.HashTreeRoot(e.Deposits, e.Withdrawals, e.Consolidations)
	}
	deposits, err := e.Deposits.HashSSZProgressive(nil)
	if err != nil {
		return [32]byte{}, err
	}
	withdrawals, err := e.Withdrawals.HashSSZProgressive(nil)
	if err != nil {
		return [32]byte{}, err
	}
	consolidations, err := e.Consolidations.HashSSZProgressive(nil)
	if err != nil {
		return [32]byte{}, err
	}
	builderDeposits, err := e.BuilderDeposits.HashSSZProgressive(nil)
	if err != nil {
		return [32]byte{}, err
	}
	builderExits, err := e.BuilderExits.HashSSZProgressive(nil)
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ProgressiveContainerRootAll(deposits[:], withdrawals[:], consolidations[:], builderDeposits[:], builderExits[:])
}

func (e *ExecutionRequests) Static() bool {
	return false
}

func (e *ExecutionRequests) validateForConfig(cfg *clparams.BeaconChainConfig) error {
	if e.Deposits == nil {
		return fmt.Errorf("nil deposit requests")
	}
	if e.Withdrawals == nil {
		return fmt.Errorf("nil withdrawal requests")
	}
	if e.Consolidations == nil {
		return fmt.Errorf("nil consolidation requests")
	}
	if e.BuilderDeposits == nil {
		return fmt.Errorf("nil builder deposit requests")
	}
	if e.BuilderExits == nil {
		return fmt.Errorf("nil builder exit requests")
	}
	if err := e.Withdrawals.ValidateBounds(int(cfg.MaxWithdrawalRequestsPerPayload)); err != nil {
		return fmt.Errorf("withdrawals: %w", err)
	}
	if err := e.Consolidations.ValidateBounds(int(cfg.MaxConsolidationRequestsPerPayload)); err != nil {
		return fmt.Errorf("consolidations: %w", err)
	}
	if err := e.BuilderDeposits.ValidateBounds(int(cfg.MaxBuilderDepositRequestsPerPayload)); err != nil {
		return fmt.Errorf("builder deposits: %w", err)
	}
	if err := e.BuilderExits.ValidateBounds(int(cfg.MaxBuilderExitRequestsPerPayload)); err != nil {
		return fmt.Errorf("builder exits: %w", err)
	}
	if err := solid.RangeErr(e.Deposits, rejectNilRequest("deposit", func(request *solid.DepositRequest) bool { return request == nil })); err != nil {
		return err
	}
	if err := solid.RangeErr(e.Withdrawals, rejectNilRequest("withdrawal", func(request *solid.WithdrawalRequest) bool { return request == nil })); err != nil {
		return err
	}
	if err := solid.RangeErr(e.Consolidations, rejectNilRequest("consolidation", func(request *solid.ConsolidationRequest) bool { return request == nil })); err != nil {
		return err
	}
	if err := solid.RangeErr(e.BuilderDeposits, rejectNilRequest("builder deposit", func(request *solid.BuilderDepositRequest) bool { return request == nil })); err != nil {
		return err
	}
	return solid.RangeErr(e.BuilderExits, rejectNilRequest("builder exit", func(request *solid.BuilderExitRequest) bool { return request == nil }))
}

func (e *ExecutionRequests) validateForPersistence(cfg *clparams.BeaconChainConfig) error {
	if err := e.validateForConfig(cfg); err != nil {
		return err
	}
	if err := e.Deposits.ValidateProgressiveDecodeBounds(int(cfg.MaxDepositRequestsPerPayload)); err != nil {
		return fmt.Errorf("deposits exceed decoder resource limit: %w", err)
	}
	return nil
}

func rejectNilRequest[T solid.EncodableHashableSSZ](name string, isNil func(T) bool) func(int, T, int) error {
	return func(i int, request T, _ int) error {
		if isNil(request) {
			return fmt.Errorf("nil %s request at index %d", name, i)
		}
		return nil
	}
}

func (e *ExecutionRequests) UnmarshalJSON(b []byte) error {
	e.ensureLists()
	newDeposits := solid.NewStaticListSSZ[*solid.DepositRequest](int(e.cfg.MaxDepositRequestsPerPayload), solid.SizeDepositRequest)
	newWithdrawals := solid.NewStaticListSSZ[*solid.WithdrawalRequest](int(e.cfg.MaxWithdrawalRequestsPerPayload), solid.SizeWithdrawalRequest)
	newConsolidations := solid.NewStaticListSSZ[*solid.ConsolidationRequest](int(e.cfg.MaxConsolidationRequestsPerPayload), solid.SizeConsolidationRequest)
	newBuilderDeposits := solid.NewStaticListSSZ[*solid.BuilderDepositRequest](int(e.cfg.MaxBuilderDepositRequestsPerPayload), solid.SizeBuilderDepositRequest)
	newBuilderExits := solid.NewStaticListSSZ[*solid.BuilderExitRequest](int(e.cfg.MaxBuilderExitRequestsPerPayload), solid.SizeBuilderExitRequest)
	if e.effectiveVersion() >= clparams.GloasVersion {
		newDeposits = solid.NewStaticProgressiveListSSZ[*solid.DepositRequest](int(e.cfg.MaxDepositRequestsPerPayload), solid.SizeDepositRequest)
		newWithdrawals = solid.NewStaticProgressiveListSSZ[*solid.WithdrawalRequest](int(e.cfg.MaxWithdrawalRequestsPerPayload), solid.SizeWithdrawalRequest)
		newConsolidations = solid.NewStaticProgressiveListSSZ[*solid.ConsolidationRequest](int(e.cfg.MaxConsolidationRequestsPerPayload), solid.SizeConsolidationRequest)
		newBuilderDeposits = solid.NewStaticProgressiveListSSZ[*solid.BuilderDepositRequest](int(e.cfg.MaxBuilderDepositRequestsPerPayload), solid.SizeBuilderDepositRequest)
		newBuilderExits = solid.NewStaticProgressiveListSSZ[*solid.BuilderExitRequest](int(e.cfg.MaxBuilderExitRequestsPerPayload), solid.SizeBuilderExitRequest)
	}
	c := struct {
		Deposits        *solid.ListSSZ[*solid.DepositRequest]        `json:"deposits"`
		Withdrawals     *solid.ListSSZ[*solid.WithdrawalRequest]     `json:"withdrawals"`
		Consolidations  *solid.ListSSZ[*solid.ConsolidationRequest]  `json:"consolidations"`
		BuilderDeposits *solid.ListSSZ[*solid.BuilderDepositRequest] `json:"builder_deposits"`
		BuilderExits    *solid.ListSSZ[*solid.BuilderExitRequest]    `json:"builder_exits"`
	}{
		Deposits:        newDeposits,
		Withdrawals:     newWithdrawals,
		Consolidations:  newConsolidations,
		BuilderDeposits: newBuilderDeposits,
		BuilderExits:    newBuilderExits,
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return err
	}
	c.Deposits = coalesceExecutionRequestList(c.Deposits, newDeposits)
	c.Withdrawals = coalesceExecutionRequestList(c.Withdrawals, newWithdrawals)
	c.Consolidations = coalesceExecutionRequestList(c.Consolidations, newConsolidations)
	c.BuilderDeposits = coalesceExecutionRequestList(c.BuilderDeposits, newBuilderDeposits)
	c.BuilderExits = coalesceExecutionRequestList(c.BuilderExits, newBuilderExits)
	if err := solid.RangeErr(c.Deposits, func(i int, request *solid.DepositRequest, _ int) error {
		if request == nil {
			return fmt.Errorf("deposit request %d is null", i)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := solid.RangeErr(c.Withdrawals, func(i int, request *solid.WithdrawalRequest, _ int) error {
		if request == nil {
			return fmt.Errorf("withdrawal request %d is null", i)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := solid.RangeErr(c.Consolidations, func(i int, request *solid.ConsolidationRequest, _ int) error {
		if request == nil {
			return fmt.Errorf("consolidation request %d is null", i)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := solid.RangeErr(c.BuilderDeposits, func(i int, request *solid.BuilderDepositRequest, _ int) error {
		if request == nil {
			return fmt.Errorf("builder deposit request %d is null", i)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := solid.RangeErr(c.BuilderExits, func(i int, request *solid.BuilderExitRequest, _ int) error {
		if request == nil {
			return fmt.Errorf("builder exit request %d is null", i)
		}
		return nil
	}); err != nil {
		return err
	}

	e.Deposits = c.Deposits
	e.Withdrawals = c.Withdrawals
	e.Consolidations = c.Consolidations
	e.BuilderDeposits = c.BuilderDeposits
	e.BuilderExits = c.BuilderExits
	e.ensureLists()
	if e.effectiveVersion() < clparams.GloasVersion && (e.BuilderDeposits.Len() > 0 || e.BuilderExits.Len() > 0) {
		return fmt.Errorf("builder execution requests before gloas")
	}
	return nil
}

func coalesceExecutionRequestList[T solid.EncodableHashableSSZ](list, empty *solid.ListSSZ[T]) *solid.ListSSZ[T] {
	if list == nil {
		return empty
	}
	return list
}

func (e *ExecutionRequests) MarshalJSON() ([]byte, error) {
	e.ensureLists()
	if e.effectiveVersion() < clparams.GloasVersion {
		return json.Marshal(struct {
			Deposits       *solid.ListSSZ[*solid.DepositRequest]       `json:"deposits"`
			Withdrawals    *solid.ListSSZ[*solid.WithdrawalRequest]    `json:"withdrawals"`
			Consolidations *solid.ListSSZ[*solid.ConsolidationRequest] `json:"consolidations"`
		}{
			Deposits:       e.Deposits,
			Withdrawals:    e.Withdrawals,
			Consolidations: e.Consolidations,
		})
	}
	return json.Marshal(struct {
		Deposits        *solid.ListSSZ[*solid.DepositRequest]        `json:"deposits"`
		Withdrawals     *solid.ListSSZ[*solid.WithdrawalRequest]     `json:"withdrawals"`
		Consolidations  *solid.ListSSZ[*solid.ConsolidationRequest]  `json:"consolidations"`
		BuilderDeposits *solid.ListSSZ[*solid.BuilderDepositRequest] `json:"builder_deposits"`
		BuilderExits    *solid.ListSSZ[*solid.BuilderExitRequest]    `json:"builder_exits"`
	}{
		Deposits:        e.Deposits,
		Withdrawals:     e.Withdrawals,
		Consolidations:  e.Consolidations,
		BuilderDeposits: e.BuilderDeposits,
		BuilderExits:    e.BuilderExits,
	})
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

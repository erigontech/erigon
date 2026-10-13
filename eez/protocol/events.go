// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/types"
)

var (
	// ErrUnexpectedEvent reports a log whose first topic is not the parsed event's.
	ErrUnexpectedEvent = errors.New("unexpected event")
	// ErrMalformedLog reports a log of the parsed event that its ABI does not describe.
	ErrMalformedLog = errors.New("malformed log")
)

var (
	bytes32Type = mustType("bytes32", nil)
	uint64Type  = mustType("uint64", nil)
	uint256Type = mustType("uint256", nil)
	addressType = mustType("address", nil)
	bytesType   = mustType("bytes", nil)

	batchPostedEvent = abi.NewEvent("BatchPosted", "BatchPosted", false, abi.Arguments{
		{Name: "sharedPublicInput", Type: bytes32Type},
		{Name: "rollupIds", Type: mustType("uint64[]", nil)},
	})
	l2ExecutionPerformedEvent = abi.NewEvent("L2ExecutionPerformed", "L2ExecutionPerformed", false, abi.Arguments{
		{Name: "rollupId", Type: uint64Type, Indexed: true},
		{Name: "newRoot", Type: bytes32Type},
		{Name: "etherBalance", Type: uint256Type},
	})
	executionConsumedEvent = abi.NewEvent("ExecutionConsumed", "ExecutionConsumed", false, abi.Arguments{
		{Name: "crossChainCallHash", Type: bytes32Type, Indexed: true},
		{Name: "rollupId", Type: uint64Type, Indexed: true},
		{Name: "entryQueueIndex", Type: uint256Type, Indexed: true},
	})
	l2TxSkippedEvent = abi.NewEvent("L2TxSkipped", "L2TxSkipped", false, abi.Arguments{
		{Name: "entryIndex", Type: uint256Type, Indexed: true},
		{Name: "revertData", Type: bytesType},
	})
	rollupCreatedEvent = abi.NewEvent("RollupCreated", "RollupCreated", false, abi.Arguments{
		{Name: "rollupId", Type: uint64Type, Indexed: true},
		{Name: "rollupContract", Type: addressType, Indexed: true},
		{Name: "initialRoot", Type: bytes32Type},
	})
	l2CrossChainCallExecutedEvent = abi.NewEvent("CrossChainCallExecuted", "CrossChainCallExecuted", false, abi.Arguments{
		{Name: "crossChainCallHash", Type: bytes32Type, Indexed: true},
		{Name: "proxy", Type: addressType, Indexed: true},
		{Name: "sourceAddress", Type: addressType},
		{Name: "callData", Type: bytesType},
		{Name: "value", Type: uint256Type},
		{Name: "callGas", Type: uint64Type},
	})
)

// Event topics.
var (
	BatchPostedTopic          = batchPostedEvent.ID
	L2ExecutionPerformedTopic = l2ExecutionPerformedEvent.ID
	ExecutionConsumedTopic    = executionConsumedEvent.ID
	L2TxSkippedTopic          = l2TxSkippedEvent.ID
	RollupCreatedTopic        = rollupCreatedEvent.ID
	// L2CrossChainCallExecutedTopic is EEZL2's six-argument
	// CrossChainCallExecuted; the registry emits a five-argument one.
	L2CrossChainCallExecutedTopic = l2CrossChainCallExecutedEvent.ID
)

// BatchPosted is emitted by postAndVerifyBatch once the batch verified.
type BatchPosted struct {
	SharedPublicInput common.Hash `abi:"sharedPublicInput"`
	RollupIDs         []uint64    `abi:"rollupIds"`
}

// L2ExecutionPerformed reports a rollup's root and ether balance after an
// entry for it executed.
type L2ExecutionPerformed struct {
	RollupID     uint64
	NewRoot      common.Hash
	EtherBalance *big.Int
}

// ExecutionConsumed reports a deferred or transient entry consumed by a call.
type ExecutionConsumed struct {
	CrossChainCallHash common.Hash
	RollupID           uint64
	EntryQueueIndex    *big.Int
}

// L2TxSkipped reports an immediate entry that reverted and was skipped.
type L2TxSkipped struct {
	EntryIndex *big.Int
	RevertData []byte
}

// RollupCreated reports a rollup registration.
type RollupCreated struct {
	RollupID       uint64
	RollupContract common.Address
	InitialRoot    common.Hash
}

// L2CrossChainCallExecuted reports a call leaving the L2 through a proxy.
type L2CrossChainCallExecuted struct {
	CrossChainCallHash common.Hash
	Proxy              common.Address
	SourceAddress      common.Address
	CallData           []byte
	Value              *big.Int
	CallGas            uint64
}

// ParseBatchPosted parses a BatchPosted log.
func ParseBatchPosted(l *types.Log) (*BatchPosted, error) {
	var e BatchPosted
	if err := unpackLog(&batchPostedEvent, l, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// ParseL2ExecutionPerformed parses an L2ExecutionPerformed log.
func ParseL2ExecutionPerformed(l *types.Log) (*L2ExecutionPerformed, error) {
	var data struct {
		NewRoot      common.Hash `abi:"newRoot"`
		EtherBalance *big.Int    `abi:"etherBalance"`
	}
	if err := unpackLog(&l2ExecutionPerformedEvent, l, &data); err != nil {
		return nil, err
	}
	rollupID, err := topicUint64(&l2ExecutionPerformedEvent, l.Topics[1])
	if err != nil {
		return nil, err
	}
	return &L2ExecutionPerformed{RollupID: rollupID, NewRoot: data.NewRoot, EtherBalance: data.EtherBalance}, nil
}

// ParseExecutionConsumed parses the registry's ExecutionConsumed log.
func ParseExecutionConsumed(l *types.Log) (*ExecutionConsumed, error) {
	if err := unpackLog(&executionConsumedEvent, l, nil); err != nil {
		return nil, err
	}
	rollupID, err := topicUint64(&executionConsumedEvent, l.Topics[2])
	if err != nil {
		return nil, err
	}
	return &ExecutionConsumed{
		CrossChainCallHash: l.Topics[1],
		RollupID:           rollupID,
		EntryQueueIndex:    new(big.Int).SetBytes(l.Topics[3][:]),
	}, nil
}

// ParseL2TxSkipped parses an L2TxSkipped log.
func ParseL2TxSkipped(l *types.Log) (*L2TxSkipped, error) {
	var data struct {
		RevertData []byte `abi:"revertData"`
	}
	if err := unpackLog(&l2TxSkippedEvent, l, &data); err != nil {
		return nil, err
	}
	return &L2TxSkipped{EntryIndex: new(big.Int).SetBytes(l.Topics[1][:]), RevertData: data.RevertData}, nil
}

// ParseRollupCreated parses a RollupCreated log.
func ParseRollupCreated(l *types.Log) (*RollupCreated, error) {
	var data struct {
		InitialRoot common.Hash `abi:"initialRoot"`
	}
	if err := unpackLog(&rollupCreatedEvent, l, &data); err != nil {
		return nil, err
	}
	rollupID, err := topicUint64(&rollupCreatedEvent, l.Topics[1])
	if err != nil {
		return nil, err
	}
	rollupContract, err := topicAddress(&rollupCreatedEvent, l.Topics[2])
	if err != nil {
		return nil, err
	}
	return &RollupCreated{RollupID: rollupID, RollupContract: rollupContract, InitialRoot: data.InitialRoot}, nil
}

// ParseL2CrossChainCallExecuted parses EEZL2's CrossChainCallExecuted log.
func ParseL2CrossChainCallExecuted(l *types.Log) (*L2CrossChainCallExecuted, error) {
	var data struct {
		SourceAddress common.Address `abi:"sourceAddress"`
		CallData      []byte         `abi:"callData"`
		Value         *big.Int       `abi:"value"`
		CallGas       uint64         `abi:"callGas"`
	}
	if err := unpackLog(&l2CrossChainCallExecutedEvent, l, &data); err != nil {
		return nil, err
	}
	proxy, err := topicAddress(&l2CrossChainCallExecutedEvent, l.Topics[2])
	if err != nil {
		return nil, err
	}
	return &L2CrossChainCallExecuted{
		CrossChainCallHash: l.Topics[1],
		Proxy:              proxy,
		SourceAddress:      data.SourceAddress,
		CallData:           data.CallData,
		Value:              data.Value,
		CallGas:            data.CallGas,
	}, nil
}

// unpackLog checks that l is an ev log with one topic per indexed argument
// and decodes its data into out. Solidity emits data with abi.encode, so
// anything but the canonical encoding is malformed.
func unpackLog(ev *abi.Event, l *types.Log, out any) error {
	if len(l.Topics) == 0 || l.Topics[0] != ev.ID {
		return fmt.Errorf("%s: %w", ev.Name, ErrUnexpectedEvent)
	}
	args := ev.Inputs.NonIndexed()
	if want := 1 + len(ev.Inputs) - len(args); len(l.Topics) != want {
		return fmt.Errorf("%s: %d topics, want %d: %w", ev.Name, len(l.Topics), want, ErrMalformedLog)
	}
	if len(args) == 0 {
		if len(l.Data) != 0 {
			return fmt.Errorf("%s: unexpected data: %w", ev.Name, ErrMalformedLog)
		}
		return nil
	}
	// Decoded byte strings alias the buffer they are decoded from.
	data := bytes.Clone(l.Data)
	values, err := args.Unpack(data)
	if err != nil {
		return fmt.Errorf("%s: %w: %w", ev.Name, ErrMalformedLog, err)
	}
	if enc, err := args.Pack(values...); err != nil || !bytes.Equal(enc, data) {
		return fmt.Errorf("%s: non-canonical data: %w", ev.Name, ErrMalformedLog)
	}
	if err := args.Copy(out, values); err != nil {
		return fmt.Errorf("%s: %w: %w", ev.Name, ErrMalformedLog, err)
	}
	return nil
}

func topicUint64(ev *abi.Event, topic common.Hash) (uint64, error) {
	if !isZero(topic[:24]) {
		return 0, fmt.Errorf("%s: uint64 topic %x: %w", ev.Name, topic, ErrMalformedLog)
	}
	return binary.BigEndian.Uint64(topic[24:]), nil
}

func topicAddress(ev *abi.Event, topic common.Hash) (common.Address, error) {
	if !isZero(topic[:12]) {
		return common.Address{}, fmt.Errorf("%s: address topic %x: %w", ev.Name, topic, ErrMalformedLog)
	}
	return common.BytesToAddress(topic[12:]), nil
}

func isZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

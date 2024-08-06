// Copyright 2024 The Erigon Authors
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

package heimdall

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/rlp"
)

// EventRecord represents state record
type EventRecord struct {
	ID       uint64            `json:"id" yaml:"id"`
	Contract libcommon.Address `json:"contract" yaml:"contract"`
	Data     hexutility.Bytes  `json:"data" yaml:"data"`
	TxHash   libcommon.Hash    `json:"tx_hash" yaml:"tx_hash"`
	LogIndex uint64            `json:"log_index" yaml:"log_index"`
	ChainID  string            `json:"bor_chain_id" yaml:"bor_chain_id"`
}

type EventRecordWithTime struct {
	EventRecord
	Time time.Time `json:"record_time" yaml:"record_time"`
}

var ErrEventRecordNotFound = errors.New("event record not found")

// String returns the string representation of a state record
func (e *EventRecordWithTime) String() string {
	return fmt.Sprintf(
		"id %v, contract %v, data: %v, txHash: %v, logIndex: %v, chainId: %v, time %s",
		e.ID,
		e.Contract.String(),
		e.Data.String(),
		e.TxHash.Hex(),
		e.LogIndex,
		e.ChainID,
		e.Time.Format(time.RFC3339),
	)
}

func (e *EventRecordWithTime) BuildEventRecord() *EventRecord {
	return &EventRecord{
		ID:       e.ID,
		Contract: e.Contract,
		Data:     e.Data,
		TxHash:   e.TxHash,
		LogIndex: e.LogIndex,
		ChainID:  e.ChainID,
	}
}

func (e *EventRecordWithTime) Pack(stateContract abi.ABI) (rlp.RawValue, error) {
	eventRecordWithoutTime := e.BuildEventRecord()
	recordBytes, err := rlp.EncodeToBytes(eventRecordWithoutTime)
	if err != nil {
		return nil, err
	}

	data, err := stateContract.Pack("commitState", big.NewInt(e.Time.Unix()), recordBytes)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func UnpackEventRecordWithTime(stateContract abi.ABI, encodedEvent rlp.RawValue) (*EventRecordWithTime, error) {
	commitStateInputs := stateContract.Methods["commitState"].Inputs
	methodId := stateContract.Methods["commitState"].ID

	if bytes.Equal(methodId, encodedEvent[0:4]) {
		t := time.Unix((&big.Int{}).SetBytes(encodedEvent[4:36]).Int64(), 0)
		args, _ := commitStateInputs.Unpack(encodedEvent[4:])

		if len(args) == 2 {
			var eventRecord EventRecord
			if err := rlp.DecodeBytes(args[1].([]byte), &eventRecord); err != nil {
				return nil, err
			}

			return &EventRecordWithTime{EventRecord: eventRecord, Time: t}, nil
		}
	}

	return nil, errors.New("no valid record")
}

type StateSyncEventsResponse struct {
	Height string                 `json:"height"`
	Result []*EventRecordWithTime `json:"result"`
}

type StateSyncEventResponse struct {
	Height string              `json:"height"`
	Result EventRecordWithTime `json:"result"`
}

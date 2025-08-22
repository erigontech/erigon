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

package bridge

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/polygon/bor/borabi"
)

// EventRecord represents state record
type EventRecord struct {
	ID       uint64         `json:"id" yaml:"id"`
	Contract common.Address `json:"contract" yaml:"contract"`
	Data     hexutil.Bytes  `json:"data" yaml:"data"`
	TxHash   common.Hash    `json:"tx_hash" yaml:"tx_hash"`
	LogIndex uint64         `json:"log_index" yaml:"log_index"`
	ChainID  string         `json:"bor_chain_id" yaml:"bor_chain_id"`
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

func (e *EventRecordWithTime) MarshallIdBytes() []byte {
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], e.ID)
	return id[:]
}

func (e *EventRecordWithTime) MarshallTimeBytes() []byte {
	var t [8]byte
	binary.BigEndian.PutUint64(t[:], uint64(e.Time.Unix()))
	return t[:]
}

func (e *EventRecordWithTime) MarshallBytes() ([]byte, error) {
	eventRecordWithoutTime := e.BuildEventRecord()
	rlpBytes, err := rlp.EncodeToBytes(eventRecordWithoutTime)
	if err != nil {
		return nil, err
	}

	stateContract := borabi.StateReceiverContractABI()
	packedBytes, err := stateContract.Pack("commitState", big.NewInt(e.Time.Unix()), rlpBytes)
	if err != nil {
		return nil, err
	}

	return packedBytes, nil
}

func (e *EventRecordWithTime) UnmarshallBytes(v []byte) error {
	stateContract := borabi.StateReceiverContractABI()
	commitStateInputs := stateContract.Methods["commitState"].Inputs
	methodId := stateContract.Methods["commitState"].ID

	if !bytes.Equal(methodId, v[0:4]) {
		return errors.New("no valid record - method mismatch")
	}

	t := time.Unix((&big.Int{}).SetBytes(v[4:36]).Int64(), 0)
	args, err := commitStateInputs.Unpack(v[4:])
	if err != nil {
		return err
	}

	if len(args) != 2 {
		return errors.New("no valid record - args count mismatch")
	}

	var eventRecord EventRecord
	if err := rlp.DecodeBytes(args[1].([]byte), &eventRecord); err != nil {
		return err
	}

	*e = EventRecordWithTime{EventRecord: eventRecord, Time: t}
	return nil
}

type StateSyncEventsResponseV1 struct {
	Height string                 `json:"height"`
	Result []*EventRecordWithTime `json:"result"`
}

type StateSyncEventsResponse struct {
	Height string                 `json:"height"`
	Result []*EventRecordWithTime `json:"result"`
}

type StateSyncEventsResponseV2 struct {
	EventRecords []struct {
		ID       string         `json:"id" yaml:"id"`
		Contract common.Address `json:"contract" yaml:"contract"`
		Data     string         `json:"data" yaml:"data"`
		TxHash   common.Hash    `json:"tx_hash" yaml:"tx_hash"`
		LogIndex string         `json:"log_index" yaml:"log_index"`
		ChainID  string         `json:"bor_chain_id" yaml:"bor_chain_id"`
		Time     time.Time      `json:"record_time" yaml:"record_time"`
	} `json:"event_records"`
}

func (v *StateSyncEventsResponseV2) GetEventRecords() ([]*EventRecordWithTime, error) {
	records := make([]*EventRecordWithTime, 0, len(v.EventRecords))

	for i := range v.EventRecords {
		r := &EventRecordWithTime{
			EventRecord: EventRecord{
				Contract: v.EventRecords[i].Contract,
				TxHash:   v.EventRecords[i].TxHash,
				ChainID:  v.EventRecords[i].ChainID,
			},
		}

		id, err := strconv.Atoi(v.EventRecords[i].ID)
		if err != nil {
			return nil, err
		}

		logIndex, err := strconv.Atoi(v.EventRecords[i].LogIndex)
		if err != nil {
			return nil, err
		}

		decoded, err := base64.StdEncoding.DecodeString(v.EventRecords[i].Data)
		if err != nil {
			return nil, err
		}

		r.ID = uint64(id)
		r.LogIndex = uint64(logIndex)
		r.Data = decoded
		r.Time = v.EventRecords[i].Time

		records = append(records, r)
	}

	return records, nil
}

type StateSyncEventResponse struct {
	Height string              `json:"height"`
	Result EventRecordWithTime `json:"result"`
}

var methodId []byte = borabi.StateReceiverContractABI().Methods["commitState"].ID

func EventTime(encodedEvent rlp.RawValue) time.Time {
	if bytes.Equal(methodId, encodedEvent[0:4]) {
		return time.Unix((&big.Int{}).SetBytes(encodedEvent[4:36]).Int64(), 0)
	}

	return time.Time{}
}

var commitStateInputs = borabi.StateReceiverContractABI().Methods["commitState"].Inputs

func EventId(encodedEvent rlp.RawValue) uint64 {
	if bytes.Equal(methodId, encodedEvent[0:4]) {
		args, _ := commitStateInputs.Unpack(encodedEvent[4:])

		if len(args) == 2 {
			var eventRecord EventRecord
			if err := rlp.DecodeBytes(args[1].([]byte), &eventRecord); err == nil {
				return eventRecord.ID
			}
		}
	}
	return 0
}

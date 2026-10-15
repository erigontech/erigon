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

package rpc

import (
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
)

// Batch is a decoded postAndVerifyBatch call.
type Batch struct {
	RollupIDs           []hexutil.Uint64 `json:"rollupIds"`
	Entries             []Entry          `json:"entries"`
	ImmediateEntryCount *hexutil.Big     `json:"immediateEntryCount"`
	StaticEntryCount    int              `json:"staticEntryCount"`
	BlockNumber         hexutil.Uint64   `json:"blockNumber"`
	BlobCount           int              `json:"blobCount"`
	// Payload is the DA payload of a batch that carries it in callData.
	Payload *Payload `json:"payload,omitempty"`
	// PayloadError says why callData holds no payload this follower reads.
	PayloadError string `json:"payloadError,omitempty"`
}

// Entry is an execution entry of a batch.
type Entry struct {
	RollupUpdates       []RollupUpdate `json:"rollupUpdates"`
	ProxyEntryHash      common.Hash    `json:"proxyEntryHash"`
	L2ToL1Calls         int            `json:"l2ToL1Calls"`
	ExpectedL1ToL2Calls int            `json:"expectedL1ToL2Calls"`
	DestinationRollupID hexutil.Uint64 `json:"destinationRollupId"`
	Success             bool           `json:"success"`
}

// RollupUpdate is a state commitment an entry moves.
type RollupUpdate struct {
	RollupID    hexutil.Uint64 `json:"rollupId"`
	EtherDelta  *hexutil.Big   `json:"etherDelta"`
	CurrentRoot common.Hash    `json:"currentRoot"`
	NewRoot     common.Hash    `json:"newRoot"`
}

// Payload is a DA payload: the span of L2 blocks and the action brackets.
type Payload struct {
	RollupID hexutil.Uint64 `json:"rollupId"`
	Blocks   []Block        `json:"blocks"`
	Actions  []Action       `json:"actions"`
}

// Block is a block of a span; Transactions are the hashes of its pure-L2
// transactions.
type Block struct {
	Beneficiary  common.Address `json:"beneficiary"`
	ExtraData    hexutil.Bytes  `json:"extraData"`
	Transactions []common.Hash  `json:"transactions"`
}

// Action is a cross-chain call of a DA payload.
type Action struct {
	SourceRollupID hexutil.Uint64 `json:"sourceRollupId"`
	TargetRollupID hexutil.Uint64 `json:"targetRollupId"`
	SourceAddress  common.Address `json:"sourceAddress"`
	TargetAddress  common.Address `json:"targetAddress"`
	Value          *hexutil.Big   `json:"value"`
	Gas            hexutil.Uint64 `json:"gas"`
	Data           hexutil.Bytes  `json:"data"`
	Success        bool           `json:"success"`
	ReturnData     hexutil.Bytes  `json:"returnData"`
}

// DecodeBatch decodes a postAndVerifyBatch input and the DA payload its
// callData carries.
func DecodeBatch(input []byte) (*Batch, error) {
	b, err := protocol.DecodePostAndVerifyBatch(input)
	if err != nil {
		return nil, err
	}
	out := &Batch{
		Entries:             make([]Entry, len(b.Entries)),
		ImmediateEntryCount: (*hexutil.Big)(b.ImmediateEntryCount),
		StaticEntryCount:    len(b.StaticEntries),
		BlockNumber:         hexutil.Uint64(b.BlockNumber),
		BlobCount:           len(b.BlobIndices),
	}
	for _, r := range b.RollupIDsWithProofSystems {
		out.RollupIDs = append(out.RollupIDs, hexutil.Uint64(r.RollupID))
	}
	for i := range b.Entries {
		e := &b.Entries[i]
		entry := Entry{
			RollupUpdates:       make([]RollupUpdate, len(e.RollupUpdates)),
			ProxyEntryHash:      e.ProxyEntryHash,
			L2ToL1Calls:         len(e.L2ToL1Calls),
			ExpectedL1ToL2Calls: len(e.ExpectedL1ToL2Calls),
			DestinationRollupID: hexutil.Uint64(e.DestinationRollupID),
			Success:             e.Success,
		}
		for j, u := range e.RollupUpdates {
			entry.RollupUpdates[j] = RollupUpdate{RollupID: hexutil.Uint64(u.RollupID), EtherDelta: (*hexutil.Big)(u.EtherDelta), CurrentRoot: u.CurrentRoot, NewRoot: u.NewRoot}
		}
		out.Entries[i] = entry
	}
	if len(b.BlobIndices) > 0 {
		out.PayloadError = "DA payload in blobs"
		return out, nil
	}
	if out.Payload, err = decodePayload(b.CallData); err != nil {
		out.PayloadError = err.Error()
	}
	return out, nil
}

func decodePayload(callData []byte) (*Payload, error) {
	msgs, err := da.Decode(nil, callData)
	if err != nil {
		return nil, err
	}
	p, err := da.ParsePayload(msgs)
	if err != nil {
		return nil, err
	}
	span, err := da.DecodeSpan(p.Operations)
	if err != nil {
		return nil, err
	}
	out := &Payload{RollupID: hexutil.Uint64(p.RollupID), Blocks: make([]Block, len(span.TxCounts)), Actions: make([]Action, len(p.Actions))}
	txs := span.Transactions
	for i, n := range span.TxCounts {
		blk := Block{Beneficiary: span.Beneficiaries[i], ExtraData: span.ExtraData[i], Transactions: make([]common.Hash, n)}
		for j, tx := range txs[:n] {
			// An EIP-2718 transaction hashes to the hash of its encoding.
			blk.Transactions[j] = crypto.Keccak256Hash(tx)
		}
		txs = txs[n:]
		out.Blocks[i] = blk
	}
	for i := range p.Actions {
		a := &p.Actions[i]
		out.Actions[i] = Action{
			SourceRollupID: hexutil.Uint64(a.SourceRollupID),
			TargetRollupID: hexutil.Uint64(a.TargetRollupID),
			SourceAddress:  a.SourceAddress,
			TargetAddress:  a.TargetAddress,
			Value:          (*hexutil.Big)(new(big.Int).SetBytes(a.Value.Bytes())),
			Gas:            hexutil.Uint64(a.Gas),
			Data:           a.Data,
			Success:        a.Success,
			ReturnData:     a.ReturnData,
		}
	}
	return out, nil
}

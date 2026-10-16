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
	"errors"
	"fmt"
	"math"

	"github.com/holiman/uint256"
)

// SyncPair is a Sync-block system transaction and the user transaction that
// consumes its effect right after it; UserTx is nil when the system
// transaction consumes its own entry.
type SyncPair struct {
	SystemTx []byte
	UserTx   []byte
}

// OutboundSettlement is an outbound entry of a batch and the L2 user
// transaction whose call consumes it.
type OutboundSettlement struct {
	Entry  *ExecutionEntry
	UserTx []byte
}

// SyncTxContext is the public input every role rebuilds system transactions
// from.
type SyncTxContext struct {
	// ChainID is the L2 chain id.
	ChainID uint64
	// RollupID is the rollup the Sync block belongs to.
	RollupID uint64
}

var errNonceOverflow = errors.New("system nonce overflow")

// BuildSyncPairs lowers a batch's cross-chain entries to Sync-block pairs:
// each outbound entry as loadExecutionTable of its EEZL2 entry followed by its
// user transaction, then each inbound sidecar for ctx.RollupID as
// executeIncomingCrossChainCall carrying the call's value. System nonces count
// up from nonce in that order. Every load resets the execution table, so one
// load per user transaction keeps a failing withdrawal from desynchronizing
// the next. Inbound sidecars of other rollups are validated and skipped.
func BuildSyncPairs(outbound []OutboundSettlement, inbound []ExecutionEntry, ctx SyncTxContext, nonce uint64) ([]SyncPair, error) {
	calls := make([]*IncomingCall, len(inbound))
	for i := range inbound {
		if err := CheckEntryShape(&inbound[i]); err != nil {
			return nil, fmt.Errorf("inbound entry %d: %w", i, err)
		}
		c, err := IncomingCallFromSidecar(&inbound[i])
		if err != nil {
			return nil, fmt.Errorf("inbound entry %d: %w", i, err)
		}
		calls[i] = c
	}
	pairs := make([]SyncPair, 0, len(outbound)+len(inbound))
	emit := func(value uint256.Int, input, userTx []byte) error {
		if nonce == math.MaxUint64 {
			return errNonceOverflow
		}
		tx := SystemTx{ChainID: ctx.ChainID, Nonce: nonce, To: EEZL2Address, Value: value, Input: input}
		enc, err := tx.MarshalBinary()
		if err != nil {
			return err
		}
		pairs = append(pairs, SyncPair{SystemTx: enc, UserTx: userTx})
		nonce++
		return nil
	}
	for i := range outbound {
		o := &outbound[i]
		if err := CheckEntryShape(o.Entry); err != nil {
			return nil, fmt.Errorf("outbound entry %d: %w", i, err)
		}
		if len(o.Entry.L2ToL1Calls) != 1 || len(o.UserTx) == 0 {
			return nil, fmt.Errorf("outbound entry %d: %w: needs one call and its user transaction", i, ErrEntryShape)
		}
		c := &o.Entry.L2ToL1Calls[0]
		l2, err := BuildOutgoingEntry(&OutgoingCall{
			SourceAddress: c.SourceAddress,
			RollupID:      ctx.RollupID,
			TargetAddress: c.TargetAddress,
			Value:         c.Value,
			Data:          c.Data,
			ReturnData:    o.Entry.ReturnData,
		})
		if err != nil {
			return nil, fmt.Errorf("outbound entry %d: %w", i, err)
		}
		input, err := EncodeLoadExecutionTable(&L2ExecutionTable{Entries: []L2ExecutionEntry{*l2}})
		if err != nil {
			return nil, fmt.Errorf("outbound entry %d: %w", i, err)
		}
		if err := emit(uint256.Int{}, input, o.UserTx); err != nil {
			return nil, fmt.Errorf("outbound entry %d: %w", i, err)
		}
	}
	for i, c := range calls {
		if c.RollupID != ctx.RollupID {
			continue
		}
		l2, err := BuildIncomingEntry(c)
		if err != nil {
			return nil, fmt.Errorf("inbound entry %d: %w", i, err)
		}
		input, err := EncodeExecuteIncomingCrossChainCall(&L2ExecutionTable{Entries: []L2ExecutionEntry{*l2}})
		if err != nil {
			return nil, fmt.Errorf("inbound entry %d: %w", i, err)
		}
		value, err := toUint256(c.Value)
		if err != nil {
			return nil, fmt.Errorf("inbound entry %d: %w", i, err)
		}
		if err := emit(value, input, nil); err != nil {
			return nil, fmt.Errorf("inbound entry %d: %w", i, err)
		}
	}
	return pairs, nil
}

// SyncBlockTxs returns the transactions of pairs in block order: each system
// transaction followed by its user transaction, if any.
func SyncBlockTxs(pairs []SyncPair) [][]byte {
	txs := make([][]byte, 0, 2*len(pairs))
	for _, p := range pairs {
		txs = append(txs, p.SystemTx)
		if p.UserTx != nil {
			txs = append(txs, p.UserTx)
		}
	}
	return txs
}

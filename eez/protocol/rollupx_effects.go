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
	"errors"
	"fmt"
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/types"
)

var (
	// ErrUnsupportedSemantics reports a cross-chain transaction beyond the
	// flat one-call profile this follower reconstructs.
	ErrUnsupportedSemantics = errors.New("cross-chain transaction outside the flat profile")
	// ErrSemantics reports cross-chain transactions rollupX rejects.
	ErrSemantics = errors.New("invalid cross-chain transactions")
	// ErrBinding reports settlement entries that differ from the ones the
	// cross-chain transactions describe.
	ErrBinding = errors.New("settlement does not match the DA payload")
)

// RollupXOutbound is a signed L2 transaction of a rollupX Sync block and the
// source entries its L2-to-L1 calls consume.
type RollupXOutbound struct {
	Sources []L2ExecutionEntry
	UserTx  []byte
}

// RollupXDelivery is an L1 call the Sync block delivers, and the value the
// system transaction mints for it.
type RollupXDelivery struct {
	Entry L2ExecutionEntry
	Value uint256.Int
}

// RollupXEffects is what the cross-chain transactions of a rollupX batch do
// on its rollup, and the L1 entries settlement must carry for them.
type RollupXEffects struct {
	rollupID uint64
	// Outbound holds the transactions that start on the rollup, in order.
	Outbound []RollupXOutbound
	// Inbound holds the deliveries of the applied L1 calls, in order.
	Inbound []RollupXDelivery
	// l1 holds, in transaction order, the L1 entry of each transaction
	// before settlement fills in its update and rolling hash.
	l1 []rollupXL1Entry
}

type rollupXL1Entry struct {
	entry    ExecutionEntry
	outbound bool
	// callHash, success and returnData are the call an outbound host makes
	// on L1 and its outcome.
	callHash   common.Hash
	success    bool
	returnData []byte
}

// NewRollupXEffects reconstructs the effects of txs on rollupID, an L2 of
// chain id chainID. Transactions starting on the rollup come first, each
// with its signed transaction; then those starting on L1.
func NewRollupXEffects(txs []da.SemanticTx, rollupID, chainID uint64) (*RollupXEffects, error) {
	if rollupID == MainnetRollupID {
		return nil, fmt.Errorf("%w: rollup id 0", ErrSemantics)
	}
	e := &RollupXEffects{rollupID: rollupID}
	inbound := false
	for i := range txs {
		tx := &txs[i]
		if len(tx.Actions) == 0 {
			return nil, fmt.Errorf("%w: transaction %d makes no call", ErrSemantics, i)
		}
		if len(tx.Actions) != 1 || tx.Actions[0].Static || tx.Actions[0].RevertSpan != 0 {
			return nil, fmt.Errorf("%w: transaction %d", ErrUnsupportedSemantics, i)
		}
		a := &tx.Actions[0]
		var err error
		switch tx.Origin {
		case rollupID:
			if inbound {
				return nil, fmt.Errorf("%w: transaction %d starts on the rollup after one from L1", ErrSemantics, i)
			}
			if err := CheckOriginTx(tx.TxData, chainID); err != nil {
				return nil, fmt.Errorf("%w: transaction %d: %w", ErrSemantics, i, err)
			}
			err = e.addOutbound(a, tx.TxData)
		case MainnetRollupID:
			inbound = true
			err = e.addInbound(a)
		default:
			return nil, fmt.Errorf("%w: transaction %d starts on rollup %d", ErrSemantics, i, tx.Origin)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: transaction %d: %w", ErrSemantics, i, err)
		}
	}
	return e, nil
}

func callHash(a *da.SemanticAction) (common.Hash, error) {
	return CrossChainCallHash(&CallHashInput{
		SourceAddress:  a.SourceAddress,
		SourceRollupID: a.SourceRollupID,
		TargetAddress:  a.TargetAddress,
		TargetRollupID: a.TargetRollupID,
		Value:          a.Value.ToBig(),
		Data:           a.Data,
	})
}

// addOutbound adds a call from the rollup to L1: its source entry on the
// rollup and the L1 host that makes it.
func (e *RollupXEffects) addOutbound(a *da.SemanticAction, userTx []byte) error {
	if a.SourceRollupID != e.rollupID || a.TargetRollupID != MainnetRollupID {
		return fmt.Errorf("call from rollup %d to %d", a.SourceRollupID, a.TargetRollupID)
	}
	h, err := callHash(a)
	if err != nil {
		return err
	}
	seed := L2EntrySeed(h)
	e.Outbound = append(e.Outbound, RollupXOutbound{
		Sources: []L2ExecutionEntry{{
			ProxyEntryHash:        h,
			IncomingCalls:         []CrossChainCall{},
			ExpectedOutgoingCalls: []ExpectedCall{},
			RollingHash:           seed.Sum(),
			Success:               a.Success,
			ReturnData:            a.ReturnData,
		}},
		UserTx: userTx,
	})
	e.l1 = append(e.l1, rollupXL1Entry{
		entry: ExecutionEntry{
			L2ToL1Calls: []CrossChainCall{{
				SourceAddress:  a.SourceAddress,
				SourceRollupID: a.SourceRollupID,
				TargetAddress:  a.TargetAddress,
				Value:          a.Value.ToBig(),
				Data:           a.Data,
			}},
			ExpectedL1ToL2Calls: []ExpectedCall{},
			DestinationRollupID: e.rollupID,
			Success:             true,
			ReturnData:          []byte{},
		},
		outbound:   true,
		callHash:   h,
		success:    a.Success,
		returnData: a.ReturnData,
	})
	return nil
}

// addInbound adds a call from L1 to the rollup: its L1 entry and, when it
// succeeded, its delivery. A failed call is a lookup with no delivery.
func (e *RollupXEffects) addInbound(a *da.SemanticAction) error {
	if a.SourceRollupID != MainnetRollupID || a.TargetRollupID != e.rollupID {
		return fmt.Errorf("call from rollup %d to %d", a.SourceRollupID, a.TargetRollupID)
	}
	h, err := callHash(a)
	if err != nil {
		return err
	}
	e.l1 = append(e.l1, rollupXL1Entry{entry: ExecutionEntry{
		ProxyEntryHash:      h,
		L2ToL1Calls:         []CrossChainCall{},
		ExpectedL1ToL2Calls: []ExpectedCall{},
		DestinationRollupID: e.rollupID,
		Success:             a.Success,
		ReturnData:          a.ReturnData,
	}})
	if !a.Success {
		return nil
	}
	l2, err := BuildIncomingEntry(&IncomingCall{
		SourceAddress:  a.SourceAddress,
		SourceRollupID: a.SourceRollupID,
		TargetAddress:  a.TargetAddress,
		RollupID:       e.rollupID,
		Value:          a.Value.ToBig(),
		Data:           a.Data,
		ReturnData:     a.ReturnData,
	})
	if err != nil {
		return err
	}
	e.Inbound = append(e.Inbound, RollupXDelivery{Entry: *l2, Value: a.Value})
	return nil
}

// Bind checks that b carries exactly the L1 entries of the effects: a
// root-only anchor, then one outbound host per signed transaction before
// the immediate count, then the inbound entries. Each entry takes its one
// update of the rollup from b and must then encode identically.
func (e *RollupXEffects) Bind(b *ProofSystemBatch) error {
	if b.ImmediateStaticEntryCount != nil && b.ImmediateStaticEntryCount.Sign() != 0 {
		return fmt.Errorf("%w: immediate static entries", ErrBinding)
	}
	if !b.ImmediateEntryCount.IsUint64() || b.ImmediateEntryCount.Uint64() == 0 || b.ImmediateEntryCount.Uint64() > uint64(len(b.Entries)) {
		return fmt.Errorf("%w: immediate entry count %v of %d entries", ErrBinding, b.ImmediateEntryCount, len(b.Entries))
	}
	immediate := int(b.ImmediateEntryCount.Uint64())
	if !IsRootOnly(&b.Entries[0], e.rollupID) {
		return fmt.Errorf("%w: entry 0 is no anchor", ErrBinding)
	}
	if len(b.StaticEntries) != 0 {
		return fmt.Errorf("%w: %d static entries", ErrBinding, len(b.StaticEntries))
	}
	out, in := b.Entries[1:immediate], b.Entries[immediate:]
	for i := range e.l1 {
		m := e.l1[i]
		queue := &in
		if m.outbound {
			queue = &out
		}
		if len(*queue) == 0 {
			return fmt.Errorf("%w: transaction %d has no entry", ErrBinding, i)
		}
		supplied := &(*queue)[0]
		*queue = (*queue)[1:]
		if len(supplied.RollupUpdates) != 1 || supplied.RollupUpdates[0].RollupID != e.rollupID {
			return fmt.Errorf("%w: transaction %d: entry does not update rollup %d alone", ErrBinding, i, e.rollupID)
		}
		m.entry.RollupUpdates = supplied.RollupUpdates
		u := supplied.RollupUpdates[0]
		seedUpdates := []RollupUpdate{{RollupID: e.rollupID, CurrentRoot: u.CurrentRoot}}
		if m.outbound {
			r := L1EntrySeed(seedUpdates, common.Hash{})
			r.CallBegin(m.callHash)
			r.CallEnd(m.success, m.returnData)
			m.entry.RollingHash = r.Sum()
		} else {
			r := L1EntrySeed(seedUpdates, m.entry.ProxyEntryHash)
			m.entry.RollingHash = r.Sum()
		}
		want, err := EncodeExecutionEntry(&m.entry)
		if err != nil {
			return err
		}
		got, err := EncodeExecutionEntry(supplied)
		if err != nil {
			return fmt.Errorf("%w: transaction %d: %w", ErrBinding, i, err)
		}
		if !bytes.Equal(want, got) {
			return fmt.Errorf("%w: transaction %d: entry differs", ErrBinding, i)
		}
	}
	if len(out) != 0 || len(in) != 0 {
		return fmt.Errorf("%w: %d outbound and %d inbound entries left over", ErrBinding, len(out), len(in))
	}
	return nil
}

// SyncSuffix returns the transactions a rollupX Sync block carries after its
// pure L2 transactions: for each outbound transaction of out, a
// loadExecutionTable of its sources and the transaction; then for each
// delivery of in an executeIncomingCrossChainCall minting its value. System
// nonces count up from nonce.
func (e *RollupXEffects) SyncSuffix(ctx SyncTxContext, out, in []int, nonce uint64) ([][]byte, error) {
	return e.suffix(out, in, func(_ int, value uint256.Int, input []byte) ([]byte, error) {
		if nonce == math.MaxUint64 {
			return nil, errNonceOverflow
		}
		tx := SystemTx{ChainID: ctx.ChainID, Nonce: nonce, To: ctx.EEZL2, Value: value, Input: input}
		nonce++
		return tx.MarshalBinary()
	})
}

// NativeSuffix is SyncSuffix under rollupX v0.1: type-0x45 transactions,
// each identified by its group's index among the batch's outbound groups
// then deliveries, under the L1 parent parentHash of the batch. Their gas
// limits are the cap; building the block lowers them to what their
// positions leave.
func (e *RollupXEffects) NativeSuffix(ctx SyncTxContext, out, in []int, parentHash common.Hash) ([][]byte, error) {
	return e.suffix(out, in, func(group int, value uint256.Int, input []byte) ([]byte, error) {
		tx := NativeTx{
			ChainID:    ctx.ChainID,
			SourceHash: SourceHash(ctx.ChainID, parentHash, uint64(group)),
			GasLimit:   NativeTxGasCap,
			To:         ctx.EEZL2,
			Value:      value,
			Input:      input,
		}
		return tx.MarshalBinary()
	})
}

// suffix builds the Sync suffix of out and in, encoding each system
// transaction of the group at its index with system.
func (e *RollupXEffects) suffix(out, in []int, system func(group int, value uint256.Int, input []byte) ([]byte, error)) ([][]byte, error) {
	var txs [][]byte
	for _, i := range out {
		o := &e.Outbound[i]
		input, err := EncodeLoadExecutionTable(&L2ExecutionTable{Entries: o.Sources, StaticEntries: []L2StaticExecutionEntry{}})
		if err != nil {
			return nil, err
		}
		value, err := committedValue(o.Sources)
		if err != nil {
			return nil, err
		}
		tx, err := system(i, value, input)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx, o.UserTx)
	}
	for _, i := range in {
		d := &e.Inbound[i]
		input, err := EncodeExecuteIncomingCrossChainCall(&L2ExecutionTable{Entries: []L2ExecutionEntry{d.Entry}, StaticEntries: []L2StaticExecutionEntry{}})
		if err != nil {
			return nil, err
		}
		tx, err := system(len(e.Outbound)+i, d.Value, input)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// committedValue is the value a table's incoming calls carry, its own and
// those of its expected outgoing calls.
func committedValue(entries []L2ExecutionEntry) (uint256.Int, error) {
	var sum uint256.Int
	add := func(calls []CrossChainCall) error {
		for i := range calls {
			v, err := toUint256(calls[i].Value)
			if err != nil {
				return err
			}
			if _, overflow := sum.AddOverflow(&sum, &v); overflow {
				return fmt.Errorf("%w: table value overflows", ErrEntryShape)
			}
		}
		return nil
	}
	for i := range entries {
		if err := add(entries[i].IncomingCalls); err != nil {
			return sum, err
		}
		for j := range entries[i].ExpectedOutgoingCalls {
			if err := add(entries[i].ExpectedOutgoingCalls[j].Calls); err != nil {
				return sum, err
			}
		}
	}
	return sum, nil
}

// CheckOriginTx checks the signed transaction a cross-chain transaction
// starts with on an L2 of chain id chainID: one canonical EIP-2718
// envelope, no blobs, the chain's id when it names one, and a recoverable
// sender.
func CheckOriginTx(enc []byte, chainID uint64) error {
	tx, err := types.UnmarshalTransactionFromBinary(enc, false)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := tx.MarshalBinary(&buf); err != nil {
		return err
	}
	switch {
	case !bytes.Equal(buf.Bytes(), enc):
		return errors.New("non-canonical transaction")
	case tx.Type() == types.BlobTxType || tx.Type() == SystemTxType || tx.Type() == NativeTxType || tx.Type() == HistoryTxType:
		return fmt.Errorf("transaction of type %d", tx.Type())
	}
	id := uint256.NewInt(chainID)
	if c := tx.GetChainID(); c != nil && !c.IsZero() && !c.Eq(id) {
		return fmt.Errorf("transaction of chain %v", c)
	}
	if _, err := tx.Sender(*types.LatestSignerForChainID(id)); err != nil {
		return err
	}
	return nil
}

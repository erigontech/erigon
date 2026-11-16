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

// Package composer settles the sequencer's blocks on L1.
package composer

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Batcher encodes the batches of a rollup.
type Batcher struct {
	RollupID uint64
	// ProofSystem attests the batches.
	ProofSystem common.Address
	// Signer recovers the senders of the blocks' transactions.
	Signer *types.Signer
}

// Effects are the cross-chain effects a Sync block carries, in the block's
// order: outbound entries, each with its call's return data, then inbound
// sidecars.
type Effects struct {
	Outbound []protocol.ExecutionEntry
	Inbound  []protocol.ExecutionEntry
}

// AnchorBatch returns the anchor-only batch settling blocks, the local chain
// from the cursor's child to the terminal block, an empty Sync block or the
// boundary of a historical chunk: Batch with no effects.
func (b *Batcher) AnchorBatch(cursor common.Hash, blocks []*types.Block) (*protocol.ProofSystemBatch, error) {
	if len(blocks) == 0 {
		return nil, errors.New("batch of no blocks")
	}
	return b.Batch(cursor, blocks, Effects{}, []common.Hash{blocks[len(blocks)-1].Hash()}, false)
}

// Batch returns the batch settling blocks, the local chain from the
// cursor's child to the terminal Sync block, which carries fx. Its first
// entry moves the rollup from the cursor to candidates[0], the empty Sync
// block; each effect's entry moves it on to the next candidate, the
// terminal's sibling ending with that effect, so the last is the terminal.
// Its DA carries the blocks as built, less the terminal's system
// transactions, which derivation rebuilds from the effects' actions. A live
// batch binds the L1 block it lands in. Its proof is the prover's to add.
func (b *Batcher) Batch(cursor common.Hash, blocks []*types.Block, fx Effects, candidates []common.Hash, live bool) (*protocol.ProofSystemBatch, error) {
	if len(blocks) == 0 {
		return nil, errors.New("batch of no blocks")
	}
	if p := blocks[0].ParentHash(); p != cursor {
		return nil, fmt.Errorf("block %d is on %x, not on the cursor %x", blocks[0].NumberU64(), p, cursor)
	}
	effects := len(fx.Outbound) + len(fx.Inbound)
	terminal := blocks[len(blocks)-1]
	switch {
	case len(candidates) != 1+effects:
		return nil, fmt.Errorf("%d candidates for %d effects", len(candidates), effects)
	case candidates[len(candidates)-1] != terminal.Hash():
		return nil, fmt.Errorf("last candidate %x is not the terminal block %x", candidates[len(candidates)-1], terminal.Hash())
	}
	span := &da.Span{}
	for i, block := range blocks {
		if i > 0 && block.ParentHash() != blocks[i-1].Hash() {
			return nil, fmt.Errorf("block %d is on %x, not on its parent %x", block.NumberU64(), block.ParentHash(), blocks[i-1].Hash())
		}
		var encoded [][]byte
		system := 0
		for j, tx := range block.Transactions() {
			if err := b.ordinary(tx); err != nil {
				if i == len(blocks)-1 && tx.Type() == protocol.SystemTxType {
					system++
					continue
				}
				return nil, fmt.Errorf("block %d transaction %d: %w", block.NumberU64(), j, err)
			}
			var buf bytes.Buffer
			if err := tx.MarshalBinary(&buf); err != nil {
				return nil, err
			}
			encoded = append(encoded, buf.Bytes())
		}
		if i == len(blocks)-1 && (len(encoded) != len(fx.Outbound) || system != effects) {
			return nil, fmt.Errorf("terminal block %d carries %d user transactions and %d system ones for %d outbound and %d inbound effects",
				block.NumberU64(), len(encoded), system, len(fx.Outbound), len(fx.Inbound))
		}
		span.TxCounts = append(span.TxCounts, uint32(len(encoded)))
		span.Beneficiaries = append(span.Beneficiaries, block.Coinbase())
		span.ExtraData = append(span.ExtraData, block.Extra())
		span.Transactions = append(span.Transactions, encoded...)
	}

	entries := make([]protocol.ExecutionEntry, 0, 1+effects)
	actions := make([]da.Action, 0, effects)
	entries = append(entries, protocol.ExecutionEntry{ReturnData: []byte{}})
	for i := range fx.Outbound {
		e := fx.Outbound[i]
		if len(e.L2ToL1Calls) != 1 || e.ProxyEntryHash != (common.Hash{}) {
			return nil, fmt.Errorf("outbound effect %d: %w", i, protocol.ErrEntryShape)
		}
		entries = append(entries, e)
	}
	for i := range fx.Inbound {
		sc := &fx.Inbound[i]
		if len(sc.L2ToL1Calls) != 1 {
			return nil, fmt.Errorf("inbound effect %d: %w", i, protocol.ErrEntryShape)
		}
		entries = append(entries, protocol.ExecutionEntry{ProxyEntryHash: sc.ProxyEntryHash, ReturnData: bytes.Clone(sc.ReturnData), Success: true})
	}
	published := append(slices.Clone(fx.Outbound), fx.Inbound...)
	for i := range published {
		a, err := protocol.ActionFromEntry(&published[i], b.RollupID)
		if err != nil {
			return nil, fmt.Errorf("effect %d: %w", i, err)
		}
		actions = append(actions, *a)
	}
	current := cursor
	for k := range entries {
		e := &entries[k]
		delta := new(big.Int)
		switch {
		case k == 0:
		case k <= len(fx.Outbound):
			delta.Neg(e.L2ToL1Calls[0].Value)
		default:
			delta.Set(fx.Inbound[k-1-len(fx.Outbound)].L2ToL1Calls[0].Value)
		}
		e.RollupUpdates = []protocol.RollupUpdate{{RollupID: b.RollupID, EtherDelta: delta, CurrentRoot: current, NewRoot: candidates[k]}}
		current = candidates[k]
		e.DestinationRollupID, e.Success = b.RollupID, true
		if e.L2ToL1Calls == nil {
			e.L2ToL1Calls = []protocol.CrossChainCall{}
		}
		e.ExpectedL1ToL2Calls = []protocol.ExpectedCall{}
		rolling := protocol.L1EntrySeed(e.RollupUpdates, e.ProxyEntryHash)
		if len(e.L2ToL1Calls) == 1 {
			c := &e.L2ToL1Calls[0]
			h, err := protocol.CrossChainCallHash(&protocol.CallHashInput{
				SourceAddress: c.SourceAddress, SourceRollupID: c.SourceRollupID, TargetAddress: c.TargetAddress,
				TargetRollupID: protocol.MainnetRollupID, Value: c.Value, Data: c.Data,
			})
			if err != nil {
				return nil, fmt.Errorf("entry %d: %w", k, err)
			}
			rolling.CallBegin(h)
			rolling.CallEnd(true, e.ReturnData)
		}
		e.RollingHash = rolling.Sum()
		// An outbound entry's result lives in its rolling hash alone.
		if e.ProxyEntryHash == (common.Hash{}) {
			e.ReturnData = []byte{}
		}
	}
	var blockNumber uint64
	if live {
		blockNumber = protocol.LiveBlockNumber
	}
	return &protocol.ProofSystemBatch{
		ExpectedRootPerRollup:     []protocol.ExpectedRootPerRollup{},
		Entries:                   entries,
		StaticEntries:             []protocol.StaticExecutionEntry{},
		ImmediateEntryCount:       big.NewInt(int64(1 + len(fx.Outbound))),
		ImmediateStaticEntryCount: new(big.Int),
		ProofSystems:              []common.Address{b.ProofSystem},
		RollupIDsWithProofSystems: []protocol.RollupIDWithProofSystems{{RollupID: b.RollupID, ProofSystemIndexes: []uint64{0}}},
		BlobIndices:               []*big.Int{},
		CallData:                  da.Encode((&da.Payload{RollupID: b.RollupID, Actions: actions, Operations: span.Encode()}).Messages()...),
		Proofs:                    [][]byte{},
		BlockNumber:               blockNumber,
	}, nil
}

var systemAddress = accounts.InternAddress(protocol.SystemAddress)

// ordinary fails for a system transaction, by type or by sender.
func (b *Batcher) ordinary(tx types.Transaction) error {
	switch tx.Type() {
	case protocol.SystemTxType, protocol.NativeTxType, protocol.HistoryTxType:
		return fmt.Errorf("system transaction of type %#x", tx.Type())
	}
	sender, err := tx.Sender(*b.Signer)
	if err != nil {
		return err
	}
	if sender == systemAddress {
		return errors.New("system transaction by its sender")
	}
	return nil
}

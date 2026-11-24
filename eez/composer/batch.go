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

// AnchorBatch returns the anchor-only batch settling blocks, the local chain
// from the cursor's child to the terminal block, an empty Sync block or the
// boundary of a historical chunk. Its one entry moves the rollup from the
// cursor to the terminal; its DA carries the blocks as built. Its proof is
// the prover's to add.
//
// Derivation rebuilds the terminal as a Sync block from the DA and the
// batch's actions, of which an anchor-only batch has none, so the terminal
// must carry no transactions and the blocks no system transaction.
func (b *Batcher) AnchorBatch(cursor common.Hash, blocks []*types.Block) (*protocol.ProofSystemBatch, error) {
	if len(blocks) == 0 {
		return nil, errors.New("batch of no blocks")
	}
	if p := blocks[0].ParentHash(); p != cursor {
		return nil, fmt.Errorf("block %d is on %x, not on the cursor %x", blocks[0].NumberU64(), p, cursor)
	}
	span := &da.Span{}
	for i, block := range blocks {
		if i > 0 && block.ParentHash() != blocks[i-1].Hash() {
			return nil, fmt.Errorf("block %d is on %x, not on its parent %x", block.NumberU64(), block.ParentHash(), blocks[i-1].Hash())
		}
		txs := block.Transactions()
		if i == len(blocks)-1 && len(txs) > 0 {
			return nil, fmt.Errorf("terminal block %d carries %d transactions", block.NumberU64(), len(txs))
		}
		encoded := make([][]byte, len(txs))
		for j, tx := range txs {
			if err := b.ordinary(tx); err != nil {
				return nil, fmt.Errorf("block %d transaction %d: %w", block.NumberU64(), j, err)
			}
			var buf bytes.Buffer
			if err := tx.MarshalBinary(&buf); err != nil {
				return nil, err
			}
			encoded[j] = buf.Bytes()
		}
		span.TxCounts = append(span.TxCounts, uint32(len(txs)))
		span.Beneficiaries = append(span.Beneficiaries, block.Coinbase())
		span.ExtraData = append(span.ExtraData, block.Extra())
		span.Transactions = append(span.Transactions, encoded...)
	}
	updates := []protocol.RollupUpdate{{
		RollupID:    b.RollupID,
		EtherDelta:  new(big.Int),
		CurrentRoot: cursor,
		NewRoot:     blocks[len(blocks)-1].Hash(),
	}}
	seed := protocol.L1EntrySeed(updates, common.Hash{})
	return &protocol.ProofSystemBatch{
		ExpectedRootPerRollup: []protocol.ExpectedRootPerRollup{},
		Entries: []protocol.ExecutionEntry{{
			RollupUpdates:       updates,
			L2ToL1Calls:         []protocol.CrossChainCall{},
			ExpectedL1ToL2Calls: []protocol.ExpectedCall{},
			RollingHash:         seed.Sum(),
			DestinationRollupID: b.RollupID,
			Success:             true,
			ReturnData:          []byte{},
		}},
		StaticEntries:             []protocol.StaticExecutionEntry{},
		ImmediateEntryCount:       big.NewInt(1),
		ImmediateStaticEntryCount: new(big.Int),
		ProofSystems:              []common.Address{b.ProofSystem},
		RollupIDsWithProofSystems: []protocol.RollupIDWithProofSystems{{RollupID: b.RollupID, ProofSystemIndexes: []uint64{0}}},
		BlobIndices:               []*big.Int{},
		CallData:                  da.Encode((&da.Payload{RollupID: b.RollupID, Operations: span.Encode()}).Messages()...),
		Proofs:                    [][]byte{},
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

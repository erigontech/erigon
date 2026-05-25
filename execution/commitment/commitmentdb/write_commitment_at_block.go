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

package commitmentdb

import "github.com/erigontech/erigon/db/kv"

// CommitmentStateWriter is the subset of db/state/execctx.*SharedDomains
// needed to write a commitment state entry. Defined as a narrow
// interface so commitmentdb does not import db/state/execctx (which
// would create a dependency cycle). *SharedDomains satisfies it
// structurally.
type CommitmentStateWriter interface {
	DomainPut(domain kv.Domain, tx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error
}

// WriteCommitmentEntryAtBlock anchors a commitment state entry at the
// given (blockNum, txNum) coordinate using the supplied encoded
// patricia trie state.
//
// This is the standalone form of the writer used by
// encodeAndStoreCommitmentState during forward execution. It exists
// for callers that need to anchor a commitment outside the
// forward-execution loop:
//
//   - fork-from CLI, when the cut block is not on an existing snapshot
//     file boundary — the fork needs a commitment entry at the cut so
//     its post-cut producer has a valid anchor.
//   - storage.Provider.Unwind (administrative debug_setHead) — after
//     the temporal db rolls back to toBlock, the commitment domain
//     needs an entry at toBlock for subsequent reads to find. In
//     block-aligned-boundary mode every toBlock is a real boundary
//     by construction.
//
// trieState is the bytes produced by
// HexPatriciaHashed.EncodeCurrentState(nil) (or the concurrent
// variant's RootTrie().EncodeCurrentState(nil)). The caller is
// responsible for positioning the patricia trie at (blockNum, txNum)
// before encoding.
//
// The write goes through the standard SharedDomains.DomainPut path,
// identical to the path forward execution uses — no separate code
// path. The encoded value is laid out exactly as encodeCommitmentState
// produces it (8 B txNum, 8 B blockNum, 2 B trieStateLen, then
// trieState), so any reader of KeyCommitmentState (LatestCommitmentState,
// SeekCommitment, LatestBlockNumWithCommitment, DecodeTxBlockNums) sees
// the same shape.
func WriteCommitmentEntryAtBlock(
	domains CommitmentStateWriter,
	tx kv.TemporalTx,
	blockNum, txNum uint64,
	trieState []byte,
) error {
	cs := NewCommitmentState(txNum, blockNum, trieState)
	encoded, err := cs.Encode()
	if err != nil {
		return err
	}
	return domains.DomainPut(kv.CommitmentDomain, tx, KeyCommitmentState, encoded, txNum, nil)
}

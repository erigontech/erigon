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

package stagedsync

import (
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func FreezeHexCommitment(tx kv.TemporalTx, agg *dbstate.Aggregator) (uint64, error) {
	if !slices.Contains(agg.CommitmentDomains(), kv.CommitmentBinDomain) {
		return 0, errors.New("freezing hex commitment requires a hex+bin datadir")
	}
	state, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV3State, kv.GetLatestOptions{})
	if err != nil {
		return 0, err
	}
	if len(state) < 18 {
		return 0, errors.New("hex commitment state is missing or truncated")
	}
	blockNum, txNum, _, err := commitment.DecodeCommitmentV3State(state)
	if err != nil {
		return 0, fmt.Errorf("decode hex commitment state: %w", err)
	}
	if finalized := rawdb.ReadForkchoiceFinalizedNum(tx); finalized < blockNum {
		return 0, fmt.Errorf("hex commitment at block %d is above finalized block %d: a reorg below it could not unwind a frozen domain", blockNum, finalized)
	}
	binaryState, _, err := tx.GetLatest(kv.CommitmentBinDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	if err != nil {
		return 0, err
	}
	if len(binaryState) < 18 {
		return 0, errors.New("binary commitment state is missing or truncated")
	}
	binaryTxNum, binaryBlockNum := commitmentdb.DecodeTxBlockNums(binaryState)
	if binaryTxNum != txNum || binaryBlockNum != blockNum {
		return 0, errors.New("binary commitment is not aligned with hex commitment")
	}
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return 0, err
	}
	config, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return 0, err
	}
	if config == nil {
		return 0, errors.New("chain configuration is missing")
	}
	header := rawdb.ReadHeaderByNumber(tx, blockNum)
	if header == nil {
		return 0, fmt.Errorf("header for commitment block %d is missing", blockNum)
	}
	if !config.IsBinaryTrie(header.Time) {
		return 0, errors.New("hex commitment is still canonical")
	}
	if err := agg.FreezeDomain(kv.CommitmentDomain, txNum); err != nil {
		return 0, err
	}
	return txNum, nil
}

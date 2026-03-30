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

package rpchelper

import (
	"context"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
)

// simulateStateReader is a StateReader for eth_simulateV1 whose Clone() method mirrors
// commitmentdb.CommitmentReplayStateReader from main: the commitment reader (temp DB) is
// re-cloned with the new tx so warmup goroutines get their own fresh read-only transaction,
// while the plain state reader (outer DB) is kept unchanged so non-modified accounts are
// always read from real on-chain state, not the empty temp DB.
type simulateStateReader struct {
	commitmentReader commitmentdb.StateReader
	plainStateReader commitmentdb.StateReader
}

func NewSimulateStateReader(commitmentReader, plainStateReader commitmentdb.StateReader) commitmentdb.StateReader {
	return simulateStateReader{commitmentReader: commitmentReader, plainStateReader: plainStateReader}
}

func (r simulateStateReader) WithHistory() bool { return false }

func (r simulateStateReader) CheckDataAvailable(_ kv.Domain, _ kv.Step) error { return nil }

func (r simulateStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	if d == kv.CommitmentDomain {
		return r.commitmentReader.Read(d, plainKey, stepSize)
	}
	return r.plainStateReader.Read(d, plainKey, stepSize)
}

func (r simulateStateReader) Clone(tx kv.TemporalTx) commitmentdb.StateReader {
	// Propagate new tx only to the commitment reader (temp DB) for warmup goroutines;
	// keep the plain state reader on the original outer-DB tx.
	return simulateStateReader{
		commitmentReader: r.commitmentReader.Clone(tx),
		plainStateReader: r.plainStateReader,
	}
}

type CommitmentReplay struct {
	dirs        datadir.Dirs
	txNumReader rawdbv3.TxNumsReader
	logger      log.Logger
}

func NewCommitmentReplay(dirs datadir.Dirs, txNumReader rawdbv3.TxNumsReader, logger log.Logger) *CommitmentReplay {
	return &CommitmentReplay{
		dirs:        dirs,
		txNumReader: txNumReader,
		logger:      logger,
	}
}

// ComputeCustomCommitmentFromStateHistory calculates the commitment root resulting from:
// - replaying the state history up to baseBlockNum
// - applying a custom delta computation function
func (r *CommitmentReplay) ComputeCustomCommitmentFromStateHistory(
	ctx context.Context,
	tx kv.TemporalTx,
	baseBlockNum uint64,
	deltaComputation func(ctx context.Context, ttx kv.TemporalTx, tsd *execctx.SharedDomains) ([]byte, error),
) ([]byte, error) {
	// Prepare a temporary data storage for commitment replay computation
	db := mdbx.New(dbcfg.TemporaryDB, r.logger).
		InMem(nil, r.dirs.Tmp).MapSize(2 * datasize.TB).GrowthStep(1 * datasize.MB).MustOpen()
	defer db.Close()

	erigonDBSettings, err := dbstate.ResolveErigonDBSettings(r.dirs, r.logger, false)
	if err != nil {
		return nil, err
	}
	agg, err := dbstate.New(r.dirs).Logger(r.logger).WithErigonDBSettings(erigonDBSettings).Open(ctx, db)
	if err != nil {
		return nil, err
	}
	defer agg.Close()

	tdb, err := temporal.New(db, agg)
	if err != nil {
		return nil, err
	}
	defer tdb.Close()

	ttx, err := tdb.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer ttx.Rollback()

	tsd, err := execctx.NewSharedDomains(ctx, ttx, r.logger)
	if err != nil {
		return nil, err
	}
	defer tsd.Close()
	tsd.GetCommitmentContext().SetDeferBranchUpdates(false)

	// We must compute genesis commitment from scratch because there's no history for block 0
	genesis, err := rawdb.ReadGenesis(tx)
	if err != nil {
		return nil, err
	}
	genesisHeader, _ := genesiswrite.GenesisWithoutStateToBlock(genesis)
	_, _, err = genesiswrite.ComputeGenesisCommitment(ctx, genesis, ttx, tsd, genesisHeader)
	if err != nil {
		return nil, err
	}
	genesisRoot, err := tsd.GetCommitmentCtx().Trie().RootHash()
	if err != nil {
		return nil, err
	}
	r.logger.Debug("Genesis", "root", common.Bytes2Hex(genesisRoot))

	if baseBlockNum > 0 {
		// We can obtain the historical commitment at baseBlockNum by touching all state keys from history, then compute
		minTxNum, err := r.txNumReader.Min(ctx, tx, 1)
		if err != nil {
			return nil, err
		}
		maxTxNum, err := r.txNumReader.Max(ctx, tx, baseBlockNum)
		if err != nil {
			return nil, err
		}
		tsd.GetCommitmentCtx().SetStateReader(commitmentdb.NewCommitmentReplayStateReader(ttx, tx, tsd, maxTxNum+1))
		r.logger.Debug("Touch historical keys", "fromTxNum", minTxNum, "toTxNum", maxTxNum+1)
		_, _, err = tsd.TouchChangedKeysFromHistory(tx, minTxNum, maxTxNum+1)
		if err != nil {
			return nil, err
		}
		historicalStateRoot, err := tsd.ComputeCommitment(ctx, ttx, true, baseBlockNum, maxTxNum, "commitment-from-history", nil)
		if err != nil {
			return nil, err
		}
		r.logger.Debug("Historical state", "historicalStateRoot", common.Bytes2Hex(historicalStateRoot))
	}

	// Apply custom delta computation to produce the final state root
	stateRoot, err := deltaComputation(ctx, ttx, tsd)
	if err != nil {
		return nil, err
	}
	r.logger.Debug("Simulated block", "root", common.Bytes2Hex(stateRoot))

	return stateRoot, nil
}

// Copyright 2025 The Erigon Authors
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

package backtester

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

var _ commitmentdb.StateReader = (*backtestStateReader)(nil)

// backtestStateReader implements StateReader for commitment backtesting. What we need for that is to:
//   - read commitment data as-of the beginning of the block
//   - read account/storage/code data as-of the end of the block
type backtestStateReader struct {
	commitmentReader commitmentdb.StateReader
	plainStateReader commitmentdb.StateReader
	commitmentAsOf   uint64
	plainStateAsOf   uint64

	// fields used only by NewRebuildStateReader for Clone
	sd *execctx.SharedDomains
}

// newBacktestStateReader creates a StateReader for verifying commitment against already-persisted data.
// Both commitment and plain state are read from history (DB/files) at the given txNum boundaries.
// Use this when all prior commitment data already exists on disk - e.g. backtesting a known block range
// where commitment was previously computed and flushed.
//
//   - commitment domain: HistoryStateReader as-of commitmentAsOf (reads persisted commitment)
//   - acc/storage/code:  HistoryStateReader as-of plainStateAsOf (reads persisted plain state)
func newBacktestStateReader(tx kv.TemporalTx, commitmentAsOf uint64, plainStateAsOf uint64) backtestStateReader {
	return backtestStateReader{
		commitmentReader: commitmentdb.NewHistoryStateReader(tx, commitmentAsOf),
		plainStateReader: commitmentdb.NewHistoryStateReader(tx, plainStateAsOf),
		commitmentAsOf:   commitmentAsOf,
		plainStateAsOf:   plainStateAsOf,
	}
}

// NewRebuildStateReader creates a StateReader for building commitment from scratch, block-by-block.
// Commitment is read from SharedDomains' in-memory batch (LatestStateReader) because we are generating
// it incrementally - prior commitment state lives in the MemBatch, not yet on disk.
// Plain state (acc/storage/code) is read from history since it already exists in DB/files.
//
//   - commitment domain: LatestStateReader via SharedDomains (reads in-memory MemBatch being built)
//   - acc/storage/code:  HistoryStateReader as-of plainStateAsOf (reads persisted plain state)
func NewRebuildStateReader(tx kv.TemporalTx, sd *execctx.SharedDomains, plainStateAsOf uint64) backtestStateReader {
	return backtestStateReader{
		commitmentReader: commitmentdb.NewLatestStateReader(tx, sd),
		plainStateReader: commitmentdb.NewHistoryStateReader(tx, plainStateAsOf),
		plainStateAsOf:   plainStateAsOf,
		sd:               sd,
	}
}

func (b backtestStateReader) WithHistory() bool {
	// we lie it is without history so we can exercise SharedDomain's in-memory DomainPut(kv.CommitmmentDomain)
	return false
}

func (b backtestStateReader) CheckDataAvailable(_ kv.Domain, _ kv.Step) error {
	return nil
}

func (b backtestStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	if d == kv.CommitmentDomain {
		return b.commitmentReader.Read(d, plainKey, stepSize)
	}
	return b.plainStateReader.Read(d, plainKey, stepSize)
}

func (b backtestStateReader) Clone(tx kv.TemporalTx) commitmentdb.StateReader {
	if b.sd != nil {
		return NewRebuildStateReader(tx, b.sd, b.plainStateAsOf)
	}
	return newBacktestStateReader(tx, b.commitmentAsOf, b.plainStateAsOf)
}

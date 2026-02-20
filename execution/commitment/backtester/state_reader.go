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
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

var _ commitmentdb.StateReader = (*backtestStateReader)(nil)

// backtestStateReader implements StateReader for commitment backtesting. What we need for that is to:
//   - read commitment data as-of the beginning of the block
//   - read account/storage/code data as-of the end of the block
type backtestStateReader struct {
	tx               kv.TemporalTx
	commitmentReader commitmentdb.StateReader
	plainStateReader commitmentdb.StateReader
	commitmentAsOf   uint64
	plainStateAsOf   uint64
}

func newBacktestStateReader(tx kv.TemporalTx, commitmentAsOf uint64, plainStateAsOf uint64) backtestStateReader {
	return backtestStateReader{
		tx:               tx,
		commitmentReader: commitmentdb.NewHistoryStateReader(tx, commitmentAsOf),
		plainStateReader: commitmentdb.NewHistoryStateReader(tx, plainStateAsOf),
		commitmentAsOf:   commitmentAsOf,
		plainStateAsOf:   plainStateAsOf,
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

func (b backtestStateReader) Clone(tx kv.TemporalTx, _ kv.TemporalGetter) commitmentdb.StateReader {
	return newBacktestStateReader(tx, b.commitmentAsOf, b.plainStateAsOf)
}

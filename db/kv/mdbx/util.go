// Copyright 2021 The Erigon Authors
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

package mdbx

import (
	"fmt"
	"os"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/mdbx-go/mdbx"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
)

func MustOpen(path string) kv.RwDB {
	return New(dbcfg.ChainDB, log.New()).Path(path).MustOpen()
}

func RecordSummaries(dbLabel kv.Label, latency mdbx.CommitLatency) error {
	_summaries, ok := kv.MDBXSummaries.Load(string(dbLabel))
	if !ok {
		return fmt.Errorf("MDBX summaries not initialized yet for db=%s", string(dbLabel))
	}
	// cast to *DBSummaries
	summaries, ok := _summaries.(*kv.DBSummaries)
	if !ok {
		return fmt.Errorf("type casting to *DBSummaries failed")
	}

	summaries.DbCommitPreparation.Observe(latency.Preparation.Seconds())
	summaries.DbCommitWrite.Observe(latency.Write.Seconds())
	summaries.DbCommitSync.Observe(latency.Sync.Seconds())
	summaries.DbCommitEnding.Observe(latency.Ending.Seconds())
	summaries.DbCommitTotal.Observe(latency.Whole.Seconds())
	return nil
}

func DefaultPageSize() datasize.ByteSize {
	osPageSize := os.Getpagesize()
	if osPageSize < 4096 { // reduce further may lead to errors (because some data is just big)
		osPageSize = 4096
	} else if osPageSize > mdbx.MaxPageSize {
		osPageSize = mdbx.MaxPageSize
	}
	osPageSize = osPageSize / 4096 * 4096 // ensure it's rounded
	return datasize.ByteSize(osPageSize)
}

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

package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
)

type afterFirstViewDB struct {
	kv.RwDB
	after func() error
}

func (db *afterFirstViewDB) View(ctx context.Context, f func(tx kv.Tx) error) error {
	if err := db.RwDB.View(ctx, f); err != nil {
		return err
	}
	after := db.after
	db.after = nil
	if after == nil {
		return nil
	}
	return after()
}

// The node commits the stop key and then writes the file. A reopen whose first chaindata
// read lands just before that commit must not drop the file the node writes after it.
func TestCommitmentStopFileKeptWhenNodeStopsDuringOpenFolder(t *testing.T) {
	rawDB, agg := pbinDualAggregator(t)
	db := &afterFirstViewDB{RwDB: rawDB, after: func() error {
		if err := rawDB.Update(t.Context(), func(tx kv.RwTx) error {
			return rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentDomain)
		}); err != nil {
			return err
		}
		return writeCommitmentStopFile(agg.Dirs(), map[string]bool{kv.CommitmentDomain.String(): true})
	}}

	require.NoError(t, agg.OpenFolder(db))
	require.FileExists(t, commitmentStopFilePath(agg.Dirs()))
}

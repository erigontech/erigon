// Copyright 2024 The Erigon Authors
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

// Package genesistest provides test-only helpers for committing a genesis
// block to a database. The production policy layer lives in genesiswrite;
// this package is the thin panicking/fatal wrapper previously exposed as
// genesiswrite.MustCommitGenesis, kept out of the production package so
// non-test code cannot import it.
package genesistest

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
)

// MustCommitGenesis commits g to db via genesiswrite.CommitGenesis and returns
// the resulting block. On error it calls tb.Fatal; tb must not be nil.
func MustCommitGenesis(tb testing.TB, g *types.Genesis, db kv.RwDB, dirs datadir.Dirs, logger log.Logger) *types.Block {
	tb.Helper()
	_, block, err := genesiswrite.CommitGenesis(context.Background(), db, genesiswrite.Options{
		Genesis: g,
		Dirs:    dirs,
		Logger:  logger,
	})
	if err != nil {
		tb.Fatal(err)
		return nil
	}
	return block
}

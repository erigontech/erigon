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

package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
)

// Pins that Commit enforces the CodeStore MDBX-tier byte cap on the write path.
// The only other Evict site is the forkchoice prune loop, which flows that
// never issue FCU (engine-x fixture imports, long side-chain stretches) don't
// reach — without eviction here the table grows unbounded with every deployed
// contract.
func TestCommit_EnforcesCodeStoreTableCap(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)

	const tableCap = 8 * 1024
	cs := cache.NewCodeStore(1<<20, tableCap)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback() // safety net; Rollback after a successful Commit is a no-op

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	sd.SetCodeStore(cs)
	sd.SetTxNum(1)

	// Write several distinct contract codes totalling well past the cap.
	code := make([]byte, 4*1024)
	for i := range 5 {
		addr := make([]byte, 20)
		addr[0] = byte(i + 1)
		code[0] = byte(i + 1)
		require.NoError(t, sd.DomainPut(kv.CodeDomain, rwTx, addr, append([]byte(nil), code...), 1, nil))
	}
	require.NoError(t, sd.Commit(ctx, rwTx))

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	c, err := roTx.Cursor(kv.TblCodeCache)
	require.NoError(t, err)
	defer c.Close()
	var total int
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		require.NoError(t, err)
		total += len(k) + len(v)
	}
	require.LessOrEqual(t, total, tableCap,
		"Commit must keep the CodeStore MDBX tier within its byte cap; unbounded growth on FCU-less flows was the bug")
}

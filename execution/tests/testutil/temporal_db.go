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

package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
)

func skipIfRequested(t testing.TB) {
	t.Helper()
	if os.Getenv("ERIGON_SKIP_EXECUTION_TESTS") != "" {
		t.Skip("skipped; unset ERIGON_SKIP_EXECUTION_TESTS to run")
	}
}

func TemporalDB(t testing.TB) kv.TemporalRwDB {
	return TemporalDBWithDirs(t, datadir.New(t.TempDir()))
}

func TemporalDBWithDirs(t testing.TB, dirs datadir.Dirs) kv.TemporalRwDB {
	// Skip when ERIGON_SKIP_EXECUTION_TESTS is set: MDBX correctness is
	// thoroughly exercised by the unit and integration tests in the main repo
	// (execution/state, execution/stagedsync, db/kv/mdbx, db/state, etc.) which
	// run on all platforms. The spec-test suites here
	// (ethereum/execution-spec-tests, legacy blockchain tests) verify EVM and
	// consensus correctness, which is platform-independent; running them on
	// Linux is sufficient. Unset ERIGON_SKIP_EXECUTION_TESTS to force locally.
	skipIfRequested(t)
	return temporaltest.NewTestDB(t, dirs)
}

func TemporalTxSD(t testing.TB, db kv.TemporalRwDB) (kv.TemporalRwTx, *execctx.SharedDomains) {
	tx, err := db.BeginTemporalRw(context.Background()) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	return tx, sd
}

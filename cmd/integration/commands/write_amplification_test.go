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

package commands

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

func corruptAccountsKV(t *testing.T) kv.TemporalRwDB {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(16))
	agg, _ := fillAccountState(t, db, 64, 5)
	require.NoError(t, agg.BuildFiles(db, 48, math.MaxUint64))

	kvFiles, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*accounts*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, kvFiles)
	// Windows cannot rename or truncate a mapped file, so overwrite the header in place: the open
	// aggregator parsed it already, only a fresh open of the path sees the oversized dictionary.
	f, err := os.OpenFile(kvFiles[0], os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(bytes.Repeat([]byte{0x7f}, 24), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return db
}

func TestCalculateWriteAmplificationReturnsDomainError(t *testing.T) {
	db := corruptAccountsKV(t)
	err := calculateWriteAmplification(t.Context(), db, []kv.Domain{kv.AccountsDomain}, log.New())
	require.ErrorContains(t, err, "decompressor")
}

func TestCalculateWriteAmplificationKeepsDomainErrorOnCancel(t *testing.T) {
	db := corruptAccountsKV(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := log.New()
	logger.SetHandler(log.FilterHandler(func(r *log.Record) bool {
		if r.Msg == "Processing domain" && len(r.Ctx) == 2 && r.Ctx[1] == kv.CommitmentDomain.String() {
			cancel()
		}
		return false
	}, log.DiscardHandler()))

	err := calculateWriteAmplification(ctx, db, []kv.Domain{kv.AccountsDomain, kv.CommitmentDomain}, logger)
	require.ErrorContains(t, err, "decompressor")
	require.NotErrorIs(t, err, context.Canceled)
}

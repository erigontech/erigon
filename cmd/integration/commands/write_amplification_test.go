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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/execfinality"
)

func TestCalculateWriteAmplificationReturnsDomainError(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(16))
	agg, _ := fillAccountState(t, db, 64, 5)
	require.NoError(t, agg.BuildFiles(db, 48, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)))

	kvFiles, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*accounts*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, kvFiles)
	// the open aggregator keeps the original mapped; only a fresh open of the path fails
	require.NoError(t, os.Rename(kvFiles[0], kvFiles[0]+".orig"))
	require.NoError(t, os.WriteFile(kvFiles[0], []byte("broken"), 0o644))

	err = calculateWriteAmplification(t.Context(), db, []kv.Domain{kv.AccountsDomain}, log.New())
	require.ErrorContains(t, err, "decompressor")
}

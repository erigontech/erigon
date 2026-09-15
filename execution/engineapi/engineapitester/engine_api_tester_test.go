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

package engineapitester_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

func TestBeforeNodeStartFailureClosesDatabase(t *testing.T) {
	genesis, key, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	var inspectedDB kv.TemporalRoDB
	require.PanicsWithValue(t, "inspection failed", func() {
		_, err := engineapitester.InitialiseEngineApiTester(t.Context(), engineapitester.EngineApiTesterInitArgs{
			Genesis:       genesis,
			CoinbaseKey:   key,
			DataDir:       t.TempDir(),
			Logger:        testlog.Logger(t, log.LvlError),
			NoEmptyBlock1: true,
			DisableTxPool: true,
			DisableSentry: true,
			BeforeNodeStart: func(db kv.TemporalRoDB) {
				inspectedDB = db
				t.Cleanup(db.Close)
				panic("inspection failed")
			},
		})
		require.NoError(t, err)
	})
	require.NotNil(t, inspectedDB)
	err = inspectedDB.View(t.Context(), func(kv.Tx) error { return nil })
	require.ErrorContains(t, err, "db closed")
}

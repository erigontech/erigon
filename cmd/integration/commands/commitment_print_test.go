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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/execfinality"
)

func TestCommitmentPrint(t *testing.T) {
	for _, tc := range []struct {
		name string
		v3   bool
	}{{"legacy", false}, {"v3", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(tc.v3))
			dirs := datadir.New(t.TempDir())
			db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(16))
			agg, roots := fillAccountState(t, db, 64, 5)
			require.NoError(t, agg.BuildFiles(db, 48, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)))

			acRo := agg.BeginFilesRo()
			defer acRo.Close()
			got, err := commitmentFileState(acRo, 47)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("block: 47 txn: 47 rootHash: %x", roots[47]), got)
			got, err = commitmentFileState(acRo, 63) // no file ends at 64: the lookup would find the 2-3 file
			require.NoError(t, err)
			require.Equal(t, "no state record", got)

			prevDatadir := datadirCli
			t.Cleanup(func() { datadirCli = prevDatadir })
			datadirCli = dirs.DataDir
			require.NoError(t, printCommitment(db, t.Context()))
		})
	}
}

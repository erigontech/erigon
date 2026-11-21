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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestCommitmentPrint(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "v3"}[v3], func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(v3))
			db, dirs, _ := dbWithState(t, true)
			prevDatadir := datadirCli
			t.Cleanup(func() { datadirCli = prevDatadir })
			datadirCli = dirs.DataDir
			require.NoError(t, printCommitment(db, t.Context()))
		})
	}
}

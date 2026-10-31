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

func TestCommitmentConvertReturnsErrors(t *testing.T) {
	enabled := statecfg.ExperimentalCommitmentV3
	t.Cleanup(func() {
		convertRestore, convertContinue, convertV3, statecfg.ExperimentalCommitmentV3 = false, false, false, enabled
	})
	root := RootCommand()
	root.SetArgs([]string{"--log.dir.disable", "commitment", "convert", "--datadir", t.TempDir(), "--chain", "mainnet", "--restore", "--continue"})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "--continue is mutually exclusive with --restore")

	convertRestore, convertContinue = false, false
	statecfg.ExperimentalCommitmentV3 = true
	root.SetArgs([]string{"--log.dir.disable", "commitment", "convert", "--datadir", t.TempDir(), "--chain", "mainnet", "--v3"})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "without COMMITMENT_V3=true")
}

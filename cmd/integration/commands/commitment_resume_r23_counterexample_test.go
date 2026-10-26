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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestPBTR23ReadResumeBaseRestoresSource(t *testing.T) {
	for _, variant := range []string{dbstate.TrieVariantHex, dbstate.TrieVariantHexBin} {
		t.Run(variant, func(t *testing.T) {
			source, output := pbtR15ResumeFixture(t, 1, false, false)
			settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
			require.NoError(t, err)
			settings.TrieVariant = &variant
			if variant == dbstate.TrieVariantHex {
				settings.TrieHash = nil
				statecfg.ConfigureCommitmentV3Records(false)
			}
			require.NoError(t, dbstate.WriteErigonDBSettings(source.Tester.Dirs, settings))
			configurePBTSourceVariant(settings)
			bin, dual, hash, v3 := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.BinCommitmentHash, statecfg.ExperimentalCommitmentV3
			_, err = readPBTResumeBase(t.Context(), source.Tester.Dirs, datadir.Open(output), true, settings, log.New())
			require.NoError(t, err)
			require.Equal(t, bin, statecfg.ExperimentalBinCommitment)
			require.Equal(t, dual, statecfg.ExperimentalHexBinCommitment)
			require.Equal(t, hash, statecfg.BinCommitmentHash)
			require.Equal(t, v3, statecfg.ExperimentalCommitmentV3, "reading the resume output must restore the source record format")
		})
	}
}

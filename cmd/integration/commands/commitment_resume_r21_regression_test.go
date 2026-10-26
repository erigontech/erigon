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
)

func TestPBTResumeEarlierOutputWithHexOnlySource(t *testing.T) {
	source, output := pbtR15ResumeFixture(t, 1, false, false)
	settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	variant := dbstate.TrieVariantHex
	settings.TrieVariant = &variant
	settings.TrieHash = nil
	require.NoError(t, dbstate.WriteErigonDBSettings(datadir.Open(source.Tester.Dirs.DataDir), settings))
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
}

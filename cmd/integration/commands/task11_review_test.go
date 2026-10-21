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
	"os/exec"
	"testing"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/stretchr/testify/require"
)

func TestPBinReviewConvertOracleRejectsSerialFallback(t *testing.T) {
	if os.Getenv("ERIGON_REVIEW_SERIAL_ORACLE") == "1" {
		require.Equal(t, 1, dbg.TipTrieWarmupers)
		TestConvertPBTWorkersMatchSequentialFiles(t)
		return
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestPBinReviewConvertOracleRejectsSerialFallback$", "-test.count=1")
	cmd.Env = append(os.Environ(), "ERIGON_REVIEW_SERIAL_ORACLE=1", "TIP_TRIE_WARMUPERS=1")
	output, err := cmd.CombinedOutput()
	require.NotContains(t, string(output), "panic:")
	if err == nil {
		t.Fatalf("convert byte oracle accepted serial fallback: TIP_TRIE_WARMUPERS=1, child exit=0")
	}
}

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

package state_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestPBinRangeWriterOraclesRejectWrongStateKey(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestPBinRangeWriter(BatchSizeDoesNotChangeRows|RecordsMatchSequentialTrie)$", "-test.count=1")
	cmd.Env = append(os.Environ(), "ERIGON_REVIEW_WRONG_STATE_KEY=1")
	output, err := cmd.CombinedOutput()
	require.Contains(t, string(output), "review persisted state key=42")
	require.NotContains(t, string(output), "panic:")
	require.Error(t, err, "both oracles accepted persisted state key 0x42 instead of 0x7374617465; child exit=0\n%s", output)
}

func pbinReviewRewritePersistedStateKey(t *testing.T, agg *state.Aggregator) {
	t.Helper()
	if os.Getenv("ERIGON_REVIEW_WRONG_STATE_KEY") != "1" {
		return
	}
	files := agg.BeginFilesRo()
	defer files.Close()
	replaced := 0
	for _, file := range files.Files(kv.CommitmentBinDomain) {
		if !strings.HasSuffix(file.Fullpath(), ".kv") {
			continue
		}
		decompressor, err := seg.NewDecompressor(file.Fullpath())
		require.NoError(t, err)
		reader := seg.NewReader(decompressor.MakeGetter(), seg.CompressNone)
		var words [][]byte
		changed := false
		for reader.HasNext() {
			key, _ := reader.Next(nil)
			value, _ := reader.Next(nil)
			if bytes.Equal(key, commitment.KeyCommitmentState) {
				key = commitment.KeyCommitmentV3State
				replaced++
				changed = true
			}
			words = append(words, bytes.Clone(key), bytes.Clone(value))
		}
		decompressor.Close()
		if !changed {
			continue
		}
		path := file.Fullpath() + ".review"
		compressor, err := seg.NewCompressor(t.Context(), "review", path, agg.Dirs().Tmp, seg.DefaultCfg, log.LvlTrace, log.New())
		require.NoError(t, err)
		writer := seg.NewWriter(compressor, seg.CompressNone)
		for _, word := range words {
			_, err = writer.Write(word)
			require.NoError(t, err)
		}
		require.NoError(t, compressor.Compress())
		compressor.Close()
		require.NoError(t, os.Rename(path, file.Fullpath()))
	}
	require.Equal(t, 1, replaced)
	fmt.Println("review persisted state key=42")
}

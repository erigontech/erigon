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

package cli

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestValidateCommitmentHistoryRetention(t *testing.T) {
	fullHistory := uint64(prune.FullMode.History.(prune.Distance))
	cases := []struct {
		name      string
		keep      bool
		blocks    uint64
		pruneMode prune.Mode
		wantErr   bool
	}{
		{name: "disabled", keep: false, blocks: 1_000, pruneMode: prune.FullMode},
		{name: "unbounded preserves existing behavior", keep: true, blocks: 0, pruneMode: prune.FullMode},
		{name: "bounded below state history", keep: true, blocks: 1_000, pruneMode: prune.FullMode},
		{name: "bounded equal to state history", keep: true, blocks: fullHistory, pruneMode: prune.FullMode},
		{name: "bounded above state history", keep: true, blocks: fullHistory + 1, pruneMode: prune.FullMode, wantErr: true},
		{name: "bounded with archive state history", keep: true, blocks: 1_000, pruneMode: prune.ArchiveMode},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ethconfig.Config{Prune: tc.pruneMode}
			cfg.KeepExecutionProofs = tc.keep
			cfg.KeepExecutionProofsBlocks = tc.blocks

			err := validateCommitmentHistoryRetention(cfg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

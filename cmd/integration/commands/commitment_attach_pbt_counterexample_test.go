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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/state"
)

func TestAttachPBTReviewResumeRejectsReplacedManifest(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip=%t", skip), func(t *testing.T) {
			selectPBTCommandSuite(t)
			source, _ := newPBTConversionSourceAtWithRows(t, 7, 0, 0, []byte{0x60, 0x01})
			published := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
			setExecutionProgress(t, source.Chaindata, 1)
			hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, skipVerify: skip, step: func(step string) error {
				if step == "marker" {
					return errors.New("interrupted attach")
				}
				return nil
			}}
			require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
			replacement, _ := newPBTConversionSourceAtWithRows(t, 7, 0, 1, []byte{0x60, 0x01})
			replacementPublished := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), replacement.DataDir, replacementPublished, true, "", log.New()))
			require.NoError(t, os.Rename(published, published+".old"))
			require.NoError(t, os.Rename(replacementPublished, published))
			_, root, end, fork, _, err := pbtAttachBlockEnd(t.Context(), source.Dirs, 1, 7)
			require.NoError(t, err)
			require.True(t, end)
			require.True(t, fork)
			manifest, err := state.ReadPBTManifest(filepath.Join(datadir.Open(published).Snap, "pbt-manifest.toml"))
			require.NoError(t, err)
			require.NotEqual(t, common.HexToHash(manifest.PBTRoot), root)
			var steps []string
			hooks.step = func(step string) error { steps = append(steps, step); return nil }
			err = attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks)
			require.Error(t, err, "resume accepted replacement manifest with mismatched post-fork root: err=%v steps=%v", err, steps)
			require.Empty(t, steps)
		})
	}
}

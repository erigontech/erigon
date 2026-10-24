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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

func TestConvertPBTR10RejectsMissingAccountsFiles(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBTWithStandaloneTestHook(t.Context(), source.DataDir, output, true, "", log.New(), func(dirs datadir.Dirs) error {
		files, err := pbtAttachFiles(dirs)
		if err != nil {
			return err
		}
		removed := 0
		for _, file := range files {
			if file.domain != kv.AccountsDomain {
				continue
			}
			if err := dir.RemoveFile(file.path); err != nil {
				return err
			}
			removed++
		}
		require.Positive(t, removed)
		return nil
	})
	require.Error(t, err, "C-e accepted output with every accounts file removed; conversion error=%v", err)
}

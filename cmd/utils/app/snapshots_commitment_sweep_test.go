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

package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

func TestDeleteStateSnapshotsCommitmentDomainsAreExact(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))
	hexFile := filepath.Join(dirs.SnapDomain, "v1.0-commitment.0-1024.kv")
	binFile := filepath.Join(dirs.SnapDomain, "v1.0-commitment-bin.0-1024.kv")
	require.NoError(t, os.WriteFile(hexFile, nil, 0o644))
	require.NoError(t, os.WriteFile(binFile, nil, 0o644))

	require.NoError(t, DeleteStateSnapshots(DeleteStateSnapshotsArgs{
		Dirs:                   dirs,
		StepRange:              "0-1024",
		DomainNames:            []string{kv.CommitmentDomain.String()},
		PromptUserBeforeDelete: false,
	}))
	confirmDoesntExist(t, hexFile)
	confirmExist(t, binFile)

	require.NoError(t, DeleteStateSnapshots(DeleteStateSnapshotsArgs{
		Dirs:                   dirs,
		StepRange:              "0-1024",
		DomainNames:            []string{kv.CommitmentBinDomain.String()},
		PromptUserBeforeDelete: false,
	}))
	confirmDoesntExist(t, binFile)
}

func TestDUClassifyFileCommitmentBinIsNotCommitmentHistory(t *testing.T) {
	require.Equal(t, duCatHistory, duClassifyFile("history", "v1.0-commitment-bin.0-1024.kv"))
	require.Equal(t, duCatInvIdx, duClassifyFile("idx", "v1.0-commitment-bin.0-1024.kv"))
}

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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

func commitmentInputs(versions ...version.Version) []*FilesItem {
	items := make([]*FilesItem, 0, len(versions))
	for _, v := range versions {
		items = append(items, &FilesItem{version: v})
	}
	return items
}

// A merge copies values through unchanged, so the output version must describe the inputs.
// Naming it from the live edge-records flag stamps v3.0 on bundled v2 rows.
func TestCommitmentMergeVersionFollowsInputsNotFlag(t *testing.T) {
	t.Parallel()

	edgeOn := &statecfg.DomainCfg{Name: kv.CommitmentDomain, EdgeRecordsInCommitment: true}
	v, err := commitmentMergeVersion(edgeOn, commitmentInputs(version.V2_2, version.V2_2))
	require.NoError(t, err)
	require.Equal(t, version.V2_2, v, "v2 inputs merged under the v3 flag must stay v2")
	require.False(t, statecfg.CommitmentEdgeRecords(v))

	edgeOff := &statecfg.DomainCfg{Name: kv.CommitmentDomain}
	v3 := statecfg.CommitmentKVWriteVersionFor(edgeOn, true)
	v, err = commitmentMergeVersion(edgeOff, commitmentInputs(v3, v3))
	require.NoError(t, err)
	require.Equal(t, v3, v, "v3 inputs merged under the legacy flag must stay v3")
	require.True(t, statecfg.CommitmentEdgeRecords(v))
}

func TestCommitmentMergeRejectsMixedFormats(t *testing.T) {
	t.Parallel()

	cfg := &statecfg.DomainCfg{Name: kv.CommitmentDomain, EdgeRecordsInCommitment: true}
	v3 := statecfg.CommitmentKVWriteVersionFor(cfg, true)
	_, err := commitmentMergeVersion(cfg, commitmentInputs(version.V2_2, v3))
	require.Error(t, err, "one .kv carries one encoding; a mixed range must not be merged")
	require.Contains(t, err.Error(), "mixed record formats")
}

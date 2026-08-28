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

package statecfg

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/version"
)

var commitmentV3 = version.Version{Major: 3, Minor: 0}

func TestCommitmentV3WriteVersionGate(t *testing.T) {
	t.Parallel()

	require.Equal(t, version.V2_2, commitmentKVWriteVersion(&DomainCfg{}), "edge-record gate off must keep v2.2")
	require.Equal(t, version.V2_1, commitmentKVWriteVersion(&DomainCfg{ReferencesInCommitmentBranches: true}), "reference-only gate must keep v2.1")
	require.Equal(t, commitmentV3, commitmentKVWriteVersion(&DomainCfg{EdgeRecordsInCommitment: true}), "edge-record gate on must stamp v3.0")
	require.Equal(t, commitmentV3, commitmentKVWriteVersion(&DomainCfg{
		EdgeRecordsInCommitment:        true,
		ReferencesInCommitmentBranches: true,
	}), "edge records must take precedence over the reference setting")
}

func TestCommitmentV3SchemaVersions(t *testing.T) {
	t.Parallel()

	cfg := Schema.CommitmentDomain
	require.True(t, cfg.EdgeRecordsInCommitment, "the schema enables edge records after task 9")
	require.Equal(t, commitmentV3, cfg.FileVersion.DataKV.Current)
	require.Equal(t, version.V1_0, cfg.FileVersion.DataKV.MinSupported)
	require.Equal(t, version.V2_0, cfg.FileVersion.AccessorBT.Current)
	require.Equal(t, version.V1_2, cfg.FileVersion.AccessorKVEI.Current)
	require.True(t, cfg.FileVersion.AccessorKVI.IsZero(), "the commitment hashmap accessor must be retired")
	require.Equal(t, version.Version{Major: 3, Minor: 0}, cfg.Hist.FileVersion.DataV.Current)
	require.Equal(t, version.V1_2, cfg.Hist.FileVersion.AccessorVI.Current)
	require.Equal(t, version.Version{Major: 3, Minor: 1}, cfg.Hist.IiCfg.FileVersion.DataEF.Current)
	require.Equal(t, version.V2_1, cfg.Hist.IiCfg.FileVersion.AccessorEFI.Current)
}

func TestCommitmentV3ReadVersionGate(t *testing.T) {
	t.Parallel()

	for _, fileVersion := range []version.Version{
		version.V1_0,
		version.V2_0,
		version.V2_1,
		version.V2_2,
	} {
		require.Falsef(t, CommitmentEdgeRecords(fileVersion), "version %s is below v3.0", fileVersion)
	}
	require.True(t, CommitmentEdgeRecords(commitmentV3))
	require.True(t, CommitmentEdgeRecords(version.Version{Major: 3, Minor: 1}))
}

func TestCommitmentV3ReadVersionGateIsPerFile(t *testing.T) {
	t.Parallel()

	files := []struct {
		name    string
		version version.Version
		want    bool
	}{
		{name: "legacy", version: version.V2_2, want: false},
		{name: "edge-record", version: commitmentV3, want: true},
		{name: "old-referenced", version: version.V2_1, want: false},
	}

	for _, file := range files {
		require.Equalf(t, file.want, CommitmentEdgeRecords(file.version), "file %s must use its own version", file.name)
	}
}

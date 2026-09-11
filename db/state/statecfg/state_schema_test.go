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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

func TestCommitmentReferencesDefault(t *testing.T) {
	assert.False(t, config3.DefaultReferencesInCommitmentBranches)
	assert.Equal(t, config3.DefaultReferencesInCommitmentBranches, Schema.CommitmentDomain.ReferencesInCommitmentBranches)
}

func TestEnableHistoricalCommitmentIncludesBinaryDomain(t *testing.T) {
	original := Schema
	t.Cleanup(func() { Schema = original })
	Schema.CommitmentDomain.Hist.HistoryDisabled = true
	Schema.CommitmentDomain.Hist.SnapshotsDisabled = true
	Schema.CommitmentBinDomain.Hist.HistoryDisabled = true
	Schema.CommitmentBinDomain.Hist.SnapshotsDisabled = true

	EnableHistoricalCommitment()

	for _, domain := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		cfg := Schema.GetDomainCfg(domain)
		require.False(t, cfg.Hist.HistoryDisabled, domain.String())
		require.False(t, cfg.Hist.SnapshotsDisabled, domain.String())
	}
}

// TestSchemaEntityEnabled pins which schema entities participate in writes and
// produce files. A missed literal in Schema silently changes this matrix.
func TestSchemaEntityEnabled(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     InvIdxCfg
		enabled bool
	}{
		{"accounts.hist.ii", Schema.AccountsDomain.Hist.IiCfg, true},
		{"storage.hist.ii", Schema.StorageDomain.Hist.IiCfg, true},
		{"code.hist.ii", Schema.CodeDomain.Hist.IiCfg, true},
		{"commitment.hist.ii", Schema.CommitmentDomain.Hist.IiCfg, true},
		{"receipt.hist.ii", Schema.ReceiptDomain.Hist.IiCfg, true},
		{"rcache.hist.ii", Schema.RCacheDomain.Hist.IiCfg, false},
		{"logAddrIdx", Schema.LogAddrIdx, true},
		{"logTopicIdx", Schema.LogTopicIdx, true},
		{"tracesFromIdx", Schema.TracesFromIdx, true},
		{"tracesToIdx", Schema.TracesToIdx, true},
	} {
		assert.Equal(t, tc.enabled, tc.cfg.Enabled, tc.name)
	}
}

type schemaCapture struct {
	domains      []kv.Domain
	indices      []kv.InvertedIdx
	dependencies [][2]kv.Domain
}

func (s *schemaCapture) RegisterDomain(cfg DomainCfg, _ *uint32, _ datadir.Dirs, _ log.Logger) error {
	s.domains = append(s.domains, cfg.Name)
	return nil
}

func (s *schemaCapture) RegisterII(cfg InvIdxCfg, _ *uint32, _ datadir.Dirs, _ log.Logger) error {
	s.indices = append(s.indices, cfg.Name)
	return nil
}

func (s *schemaCapture) AddDependencyBtwnDomains(dependency, dependent kv.Domain) {
	s.dependencies = append(s.dependencies, [2]kv.Domain{dependency, dependent})
}

func (s *schemaCapture) KeepRecentTxnsOfHistoriesWithDisabledSnapshots(uint64) {}

func TestConfigureHexRegistersSixDomains(t *testing.T) {
	original := ExperimentalBinCommitment
	t.Cleanup(func() { ExperimentalBinCommitment = original })
	ExperimentalBinCommitment = false

	capture := new(schemaCapture)
	require.NoError(t, Configure(Schema, capture, datadir.New(t.TempDir()), nil, log.New()))
	require.ElementsMatch(t, []kv.Domain{
		kv.AccountsDomain,
		kv.StorageDomain,
		kv.CodeDomain,
		kv.CommitmentDomain,
		kv.ReceiptDomain,
		kv.RCacheDomain,
	}, capture.domains)
	require.NotContains(t, capture.domains, kv.CommitmentBinDomain)
}

func TestConfigureHexBinRegistersCommitmentBinWithoutDependency(t *testing.T) {
	bin, hexBin := ExperimentalBinCommitment, ExperimentalHexBinCommitment
	t.Cleanup(func() { ExperimentalBinCommitment, ExperimentalHexBinCommitment = bin, hexBin })
	ExperimentalBinCommitment, ExperimentalHexBinCommitment = true, true

	capture := new(schemaCapture)
	require.NoError(t, Configure(Schema, capture, datadir.New(t.TempDir()), nil, log.New()))
	require.Contains(t, capture.domains, kv.CommitmentBinDomain)
	for _, dependency := range capture.dependencies {
		require.NotEqual(t, kv.CommitmentBinDomain, dependency[0])
		require.NotEqual(t, kv.CommitmentBinDomain, dependency[1])
	}
}

func TestCommitmentBinSchema(t *testing.T) {
	cfg := Schema.GetDomainCfg(kv.CommitmentBinDomain)
	require.Equal(t, kv.CommitmentBinDomain, cfg.Name)
	require.Equal(t, kv.TblCommitmentBinVals, cfg.ValuesTable)
	require.False(t, cfg.ReferencesInCommitmentBranches)
	require.True(t, cfg.Hist.SnapshotsDisabled)
	require.True(t, cfg.Hist.HistoryDisabled)
	require.Equal(t, kv.CommitmentBinHistoryIdx, cfg.Hist.HistoryIdx)
	require.Equal(t, kv.CommitmentBinDomain.String(), cfg.Hist.IiCfg.FilenameBase)
	require.True(t, cfg.Hist.IiCfg.Enabled)
	versioned, err := Schema.GetVersioned(kv.CommitmentBinDomain.String())
	require.NoError(t, err)
	require.Equal(t, cfg.Name, versioned.(DomainCfg).Name)
	require.Equal(t, cfg.ValuesTable, versioned.(DomainCfg).ValuesTable)
}

func TestConfigureBinOnlySkipsCommitmentBin(t *testing.T) {
	bin, hexBin := ExperimentalBinCommitment, ExperimentalHexBinCommitment
	t.Cleanup(func() { ExperimentalBinCommitment, ExperimentalHexBinCommitment = bin, hexBin })
	ExperimentalBinCommitment, ExperimentalHexBinCommitment = true, false

	capture := new(schemaCapture)
	require.NoError(t, Configure(Schema, capture, datadir.New(t.TempDir()), nil, log.New()))
	require.Contains(t, capture.domains, kv.CommitmentDomain)
	require.NotContains(t, capture.domains, kv.CommitmentBinDomain)
}

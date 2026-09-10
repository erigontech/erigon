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

package stagedsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

func TestUpdateExecDomainMetricsUsesCanonicalCommitmentDomain(t *testing.T) {
	metrics := kvmetrics.NewDomainMetrics()
	metrics.Domains[kv.CommitmentDomain] = &kvmetrics.DomainIOMetrics{DbReadCount: 1}
	metrics.Domains[kv.CommitmentBinDomain] = &kvmetrics.DomainIOMetrics{DbReadCount: 2}

	prev := updateExecDomainMetrics(metrics, nil, time.Second, false, kv.CommitmentDomain)
	require.Equal(t, int64(1), prev.Domains[kv.CommitmentDomain].DbReadCount)
	require.NotContains(t, prev.Domains, kv.CommitmentBinDomain)

	prev = updateExecDomainMetrics(metrics, prev, time.Second, false, kv.CommitmentBinDomain)
	require.Equal(t, int64(2), prev.Domains[kv.CommitmentBinDomain].DbReadCount)
}

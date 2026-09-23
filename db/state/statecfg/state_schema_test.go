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

	"github.com/erigontech/erigon/db/config3"
)

func TestCommitmentReferencesDefault(t *testing.T) {
	assert.False(t, config3.DefaultReferencesInCommitmentBranches)
	assert.Equal(t, config3.DefaultReferencesInCommitmentBranches, Schema.CommitmentDomain.ReferencesInCommitmentBranches)
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

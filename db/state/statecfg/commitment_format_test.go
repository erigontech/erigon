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
)

// The knob is what lets one binary build a v2 datadir and a v3 datadir; a hardcoded schema field
// silently pins every arm to the same format and the comparison reads as a tie.
func TestCommitmentSchemaFollowsEdgeRecordsKnob(t *testing.T) {
	require.Equal(t, ExperimentalCommitmentEdgeRecords, Schema.CommitmentDomain.EdgeRecordsInCommitment)
	require.Equal(t, commitmentKVEdgeRecordsVersion, commitmentKVWriteVersion(&DomainCfg{EdgeRecordsInCommitment: true}))
	require.False(t, CommitmentEdgeRecords(commitmentKVWriteVersion(&DomainCfg{EdgeRecordsInCommitment: false})))
}

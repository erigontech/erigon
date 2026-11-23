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

package sync_contribution_pool

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
)

func BenchmarkAddSyncCommitteeMessage(b *testing.B) {
	_, msgs, s := getTestCommitteesMessages(int(clparams.MainnetBeaconConfig.SyncCommitteeSize / clparams.MainnetBeaconConfig.SyncCommitteeSubnetCount))
	b.ReportAllocs()
	for b.Loop() {
		pool := NewSyncContributionPool(&clparams.MainnetBeaconConfig)
		for i := range msgs {
			if err := pool.AddSyncCommitteeMessage(s, 0, &msgs[i]); err != nil {
				b.Fatal(err)
			}
		}
	}
}

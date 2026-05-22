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

package snapshotsync

import (
	_ "embed"
	"testing"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

//go:embed testdata/mainnet_preverified.toml
var mainnetPreverifiedFixture []byte

// TestMain seeds the snapcfg registry with a minimal mainnet preverified fixture
// so MergeLimit-dependent tests (TestFindMergeRange, TestMergeSnapshots) see the
// historical 500K-wide segment layout. Production code populates the registry via
// LoadSnapshotsHashes during node startup; tests bypass that path.
func TestMain(m *testing.M) {
	snapcfg.SetToml(networkname.Mainnet, mainnetPreverifiedFixture, false)
	m.Run()
}

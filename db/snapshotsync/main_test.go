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

// TestMain seeds the snapcfg registry with a mainnet preverified fixture so
// tests that consult snapcfg.KnownCfg(mainnet) see real-world file layout.
//
// Historically these tests pulled this data implicitly through the bundled
// github.com/erigontech/erigon-snapshot Go module, which embedded the
// then-current mainnet.toml. That coupling was fragile: a refresh of the
// module could change the empirical thresholds the tests asserted against
// (lowest state-history step, tx-segment boundaries, maxStep) and break
// unrelated logic tests with no code change.
//
// After dropping the erigon-snapshot dependency, this PR freezes a copy of
// mainnet.toml as testdata at the version that was pinned on main when the
// dependency was removed (erigon-snapshot v1.3.1-0.20260402120223-7bb412bc89cd).
// Production code populates the registry at startup via LoadRemotePreverified.
func TestMain(m *testing.M) {
	snapcfg.SetToml(networkname.Mainnet, mainnetPreverifiedFixture, false)
	m.Run()
}

// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package executiontests

import (
	"testing"

	"github.com/erigontech/erigon/execution/tests/testutil"
)

func TestRLP(t *testing.T) {
	tm := new(testutil.TestMatcher)
	// Erigon's RLP layer is uint256.Int-only: every protocol field that goes
	// over RLP fits in 256 bits (EIP-7702 caps chainID at < 2^256, and the
	// stagnant EIP-2294 even proposes ~2^63 for chainID). The legacy
	// "bigint" vector encodes 2^256, which exceeds that ceiling.
	tm.SkipLoad("rlptest.json/bigint$")
	tm.Walk(t, rlpTestDir, func(t *testing.T, name string, test *testutil.RLPTest) {
		if err := tm.CheckFailure(t, test.Run()); err != nil {
			t.Error(err)
		}
	})
}

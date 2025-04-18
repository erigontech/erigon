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

package tests

import (
	"testing"

	"github.com/erigontech/erigon/params"
)

func TestTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	txt := new(testMatcher)

	// We don't allow more than uint64 in gas amount
	// This is a pseudo-consensus vulnerability, but not in practice
	// because of the gas limit
	txt.skipLoad("^ttGasLimit/TransactionWithGasLimitxPriceOverflow.json")

	txt.walk(t, transactionTestDir, func(t *testing.T, name string, test *TransactionTest) {
		t.Parallel()
		cfg := params.MainnetChainConfig
		if err := txt.checkFailure(t, test.Run(cfg.ChainID)); err != nil {
			t.Error(err)
		}
	})
}

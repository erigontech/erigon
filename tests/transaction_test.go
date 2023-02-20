// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

//go:build integration

package tests

import (
	"testing"

	"github.com/ledgerwatch/erigon/params"
)

func TestTransaction(t *testing.T) {
	//t.Parallel()

	txt := new(testMatcher)

	// We don't allow more than uint64 in gas amount
	// This is a pseudo-consensus vulnerability, but not in practice
	// because of the gas limit
	txt.skipLoad("^ttGasLimit/TransactionWithGasLimitxPriceOverflow.json")

	txt.walk(t, transactionTestDir, func(t *testing.T, name string, test *TransactionTest) {
		cfg := params.MainnetChainConfig
		if err := txt.checkFailure(t, test.Run(cfg.ChainID)); err != nil {
			t.Error(err)
		}
	})
}

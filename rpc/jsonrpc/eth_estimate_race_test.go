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

package jsonrpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// TestEstimateGasDeterminism verifies eth_estimateGas returns the same value
// for identical serial requests at a fixed head. A stale watcher goroutine in
// ReusableCaller.DoCallWithNewGas could cancel the shared EVM during the next
// probe of the binary search; the aborted frame reports err == nil, so the
// probe was misclassified as a success and the estimate intermittently
// converged below the true minimum gas limit. store(0) clears 17 non-zero
// slots, so the refund makes the (gasUsed, trueMinimum) window wide enough to
// observe the misconvergence.
func TestEstimateGasDeterminism(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, contractAddr, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	// store(0): overwrite all 17 non-zero slots with zero => large refund,
	// wide (gasUsed, trueMinimum) window.
	callData := hexutil.Bytes(contractInvocationData(0))
	args := &ethapi.CallArgs{
		From: &bankAddr,
		To:   &contractAddr,
		Data: &callData,
	}

	const iterations = 200
	estimates := make(map[hexutil.Uint64]int, 2)
	var order []hexutil.Uint64
	for range iterations {
		got, err := api.EstimateGas(context.Background(), args, nil, nil, nil)
		require.NoError(t, err)
		if _, seen := estimates[got]; !seen {
			order = append(order, got)
		}
		estimates[got]++
	}

	if len(estimates) > 1 {
		msg := fmt.Sprintf("eth_estimateGas returned %d distinct values over %d identical serial requests at a fixed head:", len(estimates), iterations)
		for _, v := range order {
			msg += fmt.Sprintf("\n  %d (x%d)", uint64(v), estimates[v])
		}
		t.Fatal(msg)
	}
}

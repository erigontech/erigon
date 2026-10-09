// Copyright 2024 The Erigon Authors
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

package rpc

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestWSReadBudget(t *testing.T) {
	t.Parallel()

	b := &wsReadBudget{limit: 100}
	require.True(t, b.acquire(60))
	require.True(t, b.acquire(40)) // exactly at the limit
	require.False(t, b.acquire(1)) // over the limit, not charged
	b.release(40)
	require.True(t, b.acquire(40)) // fits again once released

	// A nil budget and a non-positive limit are both unlimited.
	var nilBudget *wsReadBudget
	require.True(t, nilBudget.acquire(1<<40))
	nilBudget.release(1 << 40)
	require.True(t, (&wsReadBudget{limit: 0}).acquire(1<<40))
}

// A message larger than the shared budget must drop the connection mid-read
// instead of buffering the whole frame, while a message within the budget is
// still served. This pins the aggregate in-flight bound onto the WS read path.
func TestWebsocketReadBudgetDropsOversizedMessage(t *testing.T) {
	t.Parallel()
	logger := log.New()

	const budget = 1 << 20 // 1 MiB

	srv := newTestServer(logger)
	defer srv.Stop()
	srv.SetWSReadBudget(budget)

	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()
	wsURL := "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")

	// Within budget: served.
	within, err := DialWebsocket(context.Background(), wsURL, "", logger)
	require.NoError(t, err)
	defer within.Close()
	var result echoResult
	smallArg := strings.Repeat("x", budget/4)
	require.NoError(t, within.Call(&result, "test_echo", smallArg, 1))
	require.Equal(t, smallArg, result.String)

	// Over budget: the read is aborted and the connection dropped, so the call fails.
	over, err := DialWebsocket(context.Background(), wsURL, "", logger)
	require.NoError(t, err)
	defer over.Close()
	bigArg := strings.Repeat("x", 2*budget)
	require.Error(t, over.Call(&result, "test_echo", bigArg, 1))
}

// Every connection on a server reads through one shared budget, so a charge held by
// one constrains the others. Holding the budget directly stands in for a
// concurrently stalled read, keeping the assertion deterministic.
func TestWSReadChargedRespectsSharedBudget(t *testing.T) {
	t.Parallel()

	budget := &wsReadBudget{limit: 1 << 20}
	msg := make([]byte, 64<<10) // individually far within the budget

	// With the budget already fully held elsewhere, the read is refused rather than
	// allocating past the shared limit.
	require.True(t, budget.acquire(budget.limit))
	_, err := readCharged(bytes.NewReader(msg), budget)
	require.ErrorIs(t, err, errWSReadBudgetExceeded)

	// Releasing that charge lets the same read through, and the read frees its own
	// charge on the way out.
	budget.release(budget.limit)
	got, err := readCharged(bytes.NewReader(msg), budget)
	require.NoError(t, err)
	require.Len(t, got, len(msg))
	require.Zero(t, budget.inUse.Load())
}

// A message whose bytes exactly reach the budget is served, not rejected: the read
// buffer keeps a byte of slack so EOF is seen without a growth the budget can't cover.
func TestWSReadChargedExactBudget(t *testing.T) {
	t.Parallel()

	const n = wsReadBufMinCap // a size the buffer reaches exactly

	got, err := readCharged(bytes.NewReader(make([]byte, n)), &wsReadBudget{limit: n})
	require.NoError(t, err)
	require.Len(t, got, n)

	_, err = readCharged(bytes.NewReader(make([]byte, n+1)), &wsReadBudget{limit: n})
	require.ErrorIs(t, err, errWSReadBudgetExceeded)
}

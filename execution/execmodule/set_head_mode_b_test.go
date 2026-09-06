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

package execmodule

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state/execctx"
)

// TestWaitForQuiescence_AlreadyQuiet pins that the wait returns
// immediately when currentContext is already nil — the common case
// when an operator hits debug_setHead on an idle node.
func TestWaitForQuiescence_AlreadyQuiet(t *testing.T) {
	t.Parallel()
	e := &ExecModule{} // currentContext defaults to nil
	start := time.Now()
	require.NoError(t, e.waitForQuiescence(context.Background()))
	require.Less(t, time.Since(start), 50*time.Millisecond,
		"wait must return immediately when already quiescent (no poll-tick delay)")
}

// TestWaitForQuiescence_BecomesQuietMidWait pins that the wait
// terminates once another goroutine clears currentContext — the
// expected case when SetHead arrives while a stage is mid-flush.
func TestWaitForQuiescence_BecomesQuietMidWait(t *testing.T) {
	t.Parallel()
	e := &ExecModule{currentContext: &execctx.SharedDomains{}}

	go func() {
		time.Sleep(20 * time.Millisecond)
		e.lock.Lock()
		e.currentContext = nil
		e.lock.Unlock()
	}()

	start := time.Now()
	require.NoError(t, e.waitForQuiescence(context.Background()))
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond, "must have waited for the clear")
	require.Less(t, elapsed, 200*time.Millisecond,
		"must terminate promptly after currentContext clears, not wait out the full bound")
}

// TestWaitForQuiescence_TimesOut pins the bounded-wait contract: a
// wedged pipeline (currentContext never clears) surfaces as an error
// rather than hanging forever.
func TestWaitForQuiescence_TimesOut(t *testing.T) {
	t.Parallel()
	e := &ExecModule{currentContext: &execctx.SharedDomains{}}

	// 200 ms timeout via parent ctx; the inner modeBQuiescenceTimeout
	// is longer, but waitForQuiescence honors whichever deadline
	// fires first.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := e.waitForQuiescence(ctx)
	require.Error(t, err, "must return an error when currentContext never clears within the bound")
}

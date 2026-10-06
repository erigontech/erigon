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

package stages

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChainTipRejectionsReportsFirstThenPerInterval(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	r := newChainTipRejections(30*time.Second, func() time.Time { return now })

	// The first rejection is reported immediately.
	fields := r.record("process block failed", 100, errors.New("boom"))
	require.Equal(t, []any{"process block failed", "count=1 lastSlot=100 err=boom"}, fields)

	// Within the interval rejections are only aggregated.
	now = now.Add(10 * time.Second)
	require.Nil(t, r.record("process block failed", 101, errors.New("again")))
	require.Nil(t, r.record("parent not in fork graph", 102, nil))

	// Once the interval elapses the aggregate is flushed, sorted by reason, and reset.
	now = now.Add(25 * time.Second)
	fields = r.record("process block failed", 103, nil)
	require.Equal(t, []any{
		"parent not in fork graph", "count=1 lastSlot=102",
		"process block failed", "count=2 lastSlot=103 err=again",
	}, fields)
	now = now.Add(time.Second)
	require.Nil(t, r.record("process block failed", 104, nil))
}

func TestLogChainTipRejectionTolerantOfNilCfg(t *testing.T) {
	logChainTipRejection(nil, "process block failed", 1, nil)
	logChainTipRejection(&Cfg{}, "process block failed", 1, nil)
}

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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/phase1/forkchoice"
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
		"process block failed", "count=2 lastSlot=103",
	}, fields)
	now = now.Add(time.Second)
	require.Nil(t, r.record("process block failed", 104, nil))
}

func TestLogChainTipRejectionTolerantOfNilCfg(t *testing.T) {
	logChainTipRejection(nil, "process block failed", 1, nil)
	logChainTipRejection(&Cfg{}, "process block failed", 1, nil)
}

// A block that arrives early or ahead of its parent's envelope is imported moments later by
// gossip, so it does not count as a rejection.
func TestLogChainTipRejectionSkipsTransientErrors(t *testing.T) {
	cfg := &Cfg{chainTipRejections: newChainTipRejections(time.Hour, nil)}
	logChainTipRejection(cfg, "process block failed", 1, fmt.Errorf("on block: %w", forkchoice.ErrBlockTooEarly))
	logChainTipRejection(cfg, "process block failed", 2, fmt.Errorf("on block: %w", forkchoice.ErrParentEnvelopePending))
	require.Empty(t, cfg.chainTipRejections.reasons)

	logChainTipRejection(cfg, "process block failed", 3, errors.New("invalid block"))
	require.Len(t, cfg.chainTipRejections.reasons, 0, "the first real rejection is reported and flushed")
	require.False(t, cfg.chainTipRejections.lastLog.IsZero())
}

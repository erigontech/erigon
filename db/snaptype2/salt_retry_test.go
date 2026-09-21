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

package snaptype2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/snaptype"
)

// TestSaltRetryExhausted pins the bound that stops a transactions-index
// build spinning on a segment whose keys are byte-identical. Such keys
// collide under every salt (see recsplit's
// TestRecSplitIdenticalKeysCollideUnderEverySalt), so the retry has to
// end in a fault that names the segment rather than in a silent loop.
func TestSaltRetryExhausted(t *testing.T) {
	t.Parallel()
	sn := snaptype.FileInfo{From: 3664000, To: 3665000}

	for attempt := range recsplitSaltRetryLimit {
		require.NoError(t, saltRetryExhausted(attempt, sn, recsplit.ErrCollision),
			"attempt %d is within the budget and must keep retrying", attempt)
	}

	err := saltRetryExhausted(recsplitSaltRetryLimit, sn, recsplit.ErrCollision)
	require.Error(t, err, "the retry budget must end in a fault, not another salt")
	require.ErrorIs(t, err, recsplit.ErrCollision, "the cause must stay inspectable")
	require.Contains(t, err.Error(), "3664000", "the fault must name the segment range")
	require.Contains(t, err.Error(), "3665000")
	require.Contains(t, err.Error(), "duplicate transaction keys",
		"the fault must say why more salts cannot help")
}

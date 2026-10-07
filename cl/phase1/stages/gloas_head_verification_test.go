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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func headSequence(heads ...common.Hash) func() (common.Hash, error) {
	return func() (common.Hash, error) {
		h := heads[0]
		if len(heads) > 1 {
			heads = heads[1:]
		}
		return h, nil
	}
}

func TestAdvanceWhileHeadMovesRepeatsWhileEachStepMovesTheHead(t *testing.T) {
	steps := 0
	advanceWhileHeadMoves(context.Background(), headSequence(common.Hash{1}, common.Hash{2}, common.Hash{2}, common.Hash{3}, common.Hash{3}, common.Hash{3}), func(context.Context) bool {
		steps++
		return true
	})
	require.Equal(t, 3, steps)
}

func TestAdvanceWhileHeadMovesStopsWhenTheHeadStaysPut(t *testing.T) {
	steps := 0
	advanceWhileHeadMoves(context.Background(), headSequence(common.Hash{1}), func(context.Context) bool {
		steps++
		return true
	})
	require.Equal(t, 1, steps)
}

func TestAdvanceWhileHeadMovesStopsWhenAStepDoesNothing(t *testing.T) {
	steps := 0
	advanceWhileHeadMoves(context.Background(), headSequence(common.Hash{1}, common.Hash{2}), func(context.Context) bool {
		steps++
		return false
	})
	require.Equal(t, 1, steps)
}

func TestAdvanceWhileHeadMovesStopsOnCancelledContextOrHeadError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	steps := 0
	advanceWhileHeadMoves(ctx, headSequence(common.Hash{1}), func(context.Context) bool { steps++; return true })
	advanceWhileHeadMoves(context.Background(), func() (common.Hash, error) { return common.Hash{}, errors.New("no head") }, func(context.Context) bool { steps++; return true })
	require.Equal(t, 0, steps)
}

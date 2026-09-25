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

package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/stretchr/testify/require"
)

func TestRunRounds(t *testing.T) {
	c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
	require.NoError(t, err)
	got := Run(t, c, RunSpec{Name: "hph", Mode: commitment.ModeUpdate}, OpenHPH)
	require.Len(t, got.Rounds, 3)
	require.NotEqual(t, got.Rounds[0].Root, got.Rounds[1].Root)
	require.NotEqual(t, got.Rounds[1].Root, got.Rounds[2].Root)
}

type failEngine struct{ Engine }

func (e failEngine) Process(context.Context, *commitment.Updates, string, func(*commitment.CommitProgress), commitment.WarmupConfig) ([]byte, error) {
	return nil, errors.New("injected process failure")
}

func TestRunContinuesAfterFailure(t *testing.T) {
	c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
	require.NoError(t, err)
	opened := 0
	got := execute(t, c, RunSpec{Name: "hph", Mode: commitment.ModeUpdate}, func(ctx context.Context, spec RunSpec) (Engine, error) {
		engine, err := OpenHPH(ctx, spec)
		opened++
		if opened == 1 {
			return failEngine{engine}, err
		}
		return engine, err
	})
	require.Len(t, got.Rounds, 3)
	require.EqualError(t, got.Rounds[0].Err, "injected process failure")
	for _, round := range got.Rounds[1:] {
		require.NoError(t, round.Err)
		require.NotEmpty(t, round.Root)
	}
	require.True(t, got.Rounds[1].Rebuilt)
}

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
	"fmt"
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

func TestCompareRepairsHPHDrift(t *testing.T) {
	c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
	require.NoError(t, err)
	for _, tolerant := range []bool{false, true} {
		t.Run(fmt.Sprintf("tolerant=%t", tolerant), func(t *testing.T) {
			c.Assertions.TolerateHPHDrift = tolerant
			opened := 0
			recorder := &failureRecorder{TB: t}
			run := func() []observation {
				return Compare(recorder, c, []RunSpec{{Name: "fresh", Mode: commitment.ModeUpdate, Fresh: true}, {Name: "hph", Mode: commitment.ModeUpdate}}, func(ctx context.Context, spec RunSpec) (commitment.Trie, error) {
					engine, err := OpenHPH(ctx, spec)
					if spec.Name == "hph" {
						opened++
						if opened == 1 {
							return driftEngine{engine}, err
						}
					}
					return engine, err
				})
			}
			if !tolerant {
				require.PanicsWithValue(t, "test failure", func() { run() })
				require.Contains(t, recorder.failure, "round=0")
				require.Contains(t, recorder.failure, "HPH drift")
				require.Empty(t, recorder.logs)
				require.Equal(t, 1, opened)
				return
			}
			got := run()
			require.Empty(t, recorder.failure)
			require.Len(t, recorder.logs, 1)
			require.Len(t, got[1].Rounds, len(c.Rounds))
			require.Equal(t, []int{0}, got[1].HPHDrift)
			require.Equal(t, 2, opened)
			for i, round := range got[1].Rounds {
				require.Equal(t, got[0].Rounds[i].Root, round.Root)
			}
		})
	}
}

type failureRecorder struct {
	testing.TB
	failure string
	logs    []string
}

func (r *failureRecorder) Errorf(format string, args ...any) {
	r.failure += fmt.Sprintf(format, args...)
}

func (r *failureRecorder) FailNow() { panic("test failure") }

func (r *failureRecorder) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

type driftEngine struct{ commitment.Trie }

func (e driftEngine) Process(ctx context.Context, updates *commitment.Updates, prefix string, progress func(*commitment.CommitProgress), warmup commitment.WarmupConfig) ([]byte, error) {
	root, err := e.Trie.Process(ctx, updates, prefix, progress, warmup)
	root = append([]byte(nil), root...)
	root[0] ^= 1
	return root, err
}

type failEngine struct{ commitment.Trie }

func (e failEngine) Process(context.Context, *commitment.Updates, string, func(*commitment.CommitProgress), commitment.WarmupConfig) ([]byte, error) {
	return nil, errors.New("injected process failure")
}

func TestRunContinuesAfterFailure(t *testing.T) {
	c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
	require.NoError(t, err)
	opened := 0
	got := execute(t, c, RunSpec{Name: "hph", Mode: commitment.ModeUpdate}, func(ctx context.Context, spec RunSpec) (commitment.Trie, error) {
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

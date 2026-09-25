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
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/stretchr/testify/require"
)

type (
	Engine   = commitment.Trie
	openFunc func(context.Context, RunSpec) (Engine, error)
)

type RunSpec struct {
	Name    string
	Mode    commitment.Mode
	Workers int
	Reload  bool
	Context ContextSpec
	Memory  *Memory
}

type roundResult struct {
	State    []byte
	BlockNum uint64
	TxNum    uint64
	Root     []byte
	Records  map[string][]byte
	Counts   counts
	Deltas   []commitment.BranchDelta
	Err      error
	Rebuilt  bool
}

type observation struct {
	Name   string
	Rounds []roundResult
}

func OpenHPH(ctx context.Context, spec RunSpec) (Engine, error) {
	if spec.Mode == commitment.ModeParallel {
		return commitment.NewParallelPatriciaHashed(spec.Memory.Open, 20, commitment.DefaultTrieConfig()), nil
	}
	if spec.Mode != commitment.ModeUpdate {
		return nil, fmt.Errorf("unsupported HPH mode: %s", spec.Mode)
	}
	reader, _ := spec.Memory.Open(ctx)
	return commitment.NewHexPatriciaHashed(20, reader, commitment.DefaultTrieConfig()), nil
}

func execute(tb testing.TB, c commitmenttest.Case, spec RunSpec, open openFunc) observation {
	tb.Helper()
	got := observation{Name: spec.Name, Rounds: make([]roundResult, 0, len(c.Rounds))}
	state := make(commitmenttest.State)
	var engine Engine
	defer func() {
		if engine != nil {
			engine.Release()
		}
	}()
	for i, ops := range c.Rounds {
		state.Apply(ops)
		round := roundResult{Rebuilt: engine == nil && i != 0}
		if engine == nil {
			spec.Memory = NewMemory(spec.Context)
			engine, round.Err = open(context.Background(), spec)
			if round.Rebuilt {
				ops = state.Ops()
			}
		}
		spec.Memory.Apply(ops)
		if round.Err == nil {
			updates := updatesFor(tb, spec.Mode, ops)
			round.Root, round.Err = engine.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
			updates.Close()
			round.Root = bytes.Clone(round.Root)
		}
		if round.Err == nil && spec.Reload {
			engine = reload(tb, engine, spec, open, &round, uint64(i+1))
		}
		round.Counts, round.Records, round.Deltas = spec.Memory.Counts(), spec.Memory.Records(), spec.Memory.Deltas()
		got.Rounds = append(got.Rounds, round)
		if round.Err != nil && engine != nil {
			engine.Release()
			engine = nil
		}
	}
	return got
}

func reload(tb testing.TB, engine Engine, spec RunSpec, open openFunc, round *roundResult, number uint64) Engine {
	tb.Helper()
	codec, ok := engine.(commitment.TrieStateCodec)
	if !ok {
		round.Err = fmt.Errorf("engine %s has no state codec", spec.Name)
		return engine
	}
	round.State, round.Err = codec.EncodeState(number, number, nil)
	if round.Err != nil {
		return engine
	}
	engine.Release()
	engine, round.Err = open(context.Background(), spec)
	if round.Err != nil {
		return engine
	}
	codec = engine.(commitment.TrieStateCodec)
	round.BlockNum, round.TxNum, round.Err = codec.RestoreState(round.State)
	if round.Err != nil {
		return engine
	}
	updates := updatesFor(tb, spec.Mode, nil)
	root, err := engine.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
	updates.Close()
	round.Err = err
	if err == nil && (!bytes.Equal(root, round.Root) || round.BlockNum != number || round.TxNum != number) {
		round.Err = fmt.Errorf("restored root or metadata differs")
	}
	return engine
}

func check(tb testing.TB, c commitmenttest.Case, got observation) {
	tb.Helper()
	require.True(tb, c.Assertions.ProcessNoError, "case=%s must declare its Process no-error assertion", c.ID)
	require.Len(tb, got.Rounds, len(c.Rounds), "case=%s seed=%+v engine=%s", c.ID, c.Seed, got.Name)
	checkStateReads := len(c.Assertions.StateReadEngines) == 0 || slices.Contains(c.Assertions.StateReadEngines, got.Name)
	for i := range got.Rounds {
		round := &got.Rounds[i]
		label := fmt.Sprintf("case=%s seed=%+v engine=%s round=%d", c.ID, c.Seed, got.Name, i)
		require.NoError(tb, round.Err, label)
		if checkStateReads && c.Assertions.ZeroAccountReads {
			require.Zero(tb, round.Counts.AccountReads, label)
		}
		if checkStateReads && c.Assertions.ZeroStorageReads {
			require.Zero(tb, round.Counts.StorageReads, label)
		}
	}
}

func Run(tb testing.TB, c commitmenttest.Case, spec RunSpec, open openFunc) observation {
	tb.Helper()
	got := execute(tb, c, spec, open)
	check(tb, c, got)
	return got
}

func Compare(tb testing.TB, c commitmenttest.Case, runs []RunSpec, open openFunc) {
	tb.Helper()
	observations := make([]observation, 0, len(runs))
	for _, spec := range runs {
		observations = append(observations, execute(tb, c, spec, open))
	}
	for _, observation := range observations {
		check(tb, c, observation)
	}
	for i := 1; i < len(observations); i++ {
		for round := range c.Rounds {
			require.True(tb, bytes.Equal(observations[0].Rounds[round].Root, observations[i].Rounds[round].Root), "case=%s seed=%+v round=%d %s/%s", c.ID, c.Seed, round, runs[0].Name, runs[i].Name)
		}
	}
}

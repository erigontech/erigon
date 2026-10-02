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

type openFunc func(context.Context, RunSpec) (commitment.Trie, error)

type RunSpec struct {
	Name     string
	Mode     commitment.Mode
	Workers  int
	Reload   bool
	Fresh    bool
	Context  ContextSpec
	Memory   *Memory
	expected []roundResult
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
	Name     string
	Rounds   []roundResult
	HPHDrift []int
}

func OpenHPH(ctx context.Context, spec RunSpec) (commitment.Trie, error) {
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
	dir := tb.TempDir()
	var engine commitment.Trie
	defer func() {
		if engine != nil {
			engine.Release()
		}
	}()
	for i, ops := range c.Rounds {
		state.Apply(ops)
		label := fmt.Sprintf("case=%s seed=%+v engine=%s round=%d", c.ID, c.Seed, spec.Name, i)
		if spec.Fresh && engine != nil {
			engine.Release()
			engine = nil
		}
		round := roundResult{Rebuilt: engine == nil && i != 0}
		if engine == nil {
			spec.Memory = NewMemory(spec.Context)
			engine, round.Err = open(context.Background(), spec)
			if round.Rebuilt || spec.Fresh {
				ops = state.Ops()
			}
		}
		spec.Memory.Apply(ops)
		if round.Err == nil {
			round.Root, round.Err = process(engine, spec.Mode, dir, ops)
			round.Root = bytes.Clone(round.Root)
			if round.Err == nil && spec.Mode == commitment.ModeCollect {
				root, err := engine.RootHash()
				require.NoError(tb, err, label)
				require.Equal(tb, round.Root, root, label)
			}
		}
		if round.Err == nil && spec.expected != nil && !bytes.Equal(round.Root, spec.expected[i].Root) {
			if !c.Assertions.TolerateHPHDrift {
				require.Equal(tb, spec.expected[i].Root, round.Root, "%s: HPH drift: hph %x expected %x", label, round.Root, spec.expected[i].Root)
			}
			tb.Logf("%s: HexPatriciaHashed drifted from ground truth (hph %x); v3 matched a fresh rebuild; HexPatriciaHashed rebuilt from state", label, round.Root)
			got.HPHDrift = append(got.HPHDrift, i)
			engine.Release()
			spec.Memory = NewMemory(spec.Context)
			engine, round.Err = open(context.Background(), spec)
			require.NoError(tb, round.Err, label)
			spec.Memory.Apply(state.Ops())
			round.Root, round.Err = process(engine, spec.Mode, dir, state.Ops())
			require.NoError(tb, round.Err, label)
			require.Equal(tb, spec.expected[i].Root, round.Root, "%s: rebuilt HPH disagrees", label)
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

func reload(tb testing.TB, engine commitment.Trie, spec RunSpec, open openFunc, round *roundResult, number uint64) commitment.Trie {
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
	root, err := process(engine, spec.Mode, tb.TempDir(), nil)
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

func Compare(tb testing.TB, c commitmenttest.Case, runs []RunSpec, open openFunc) []observation {
	tb.Helper()
	observations := make([]observation, 0, len(runs))
	verifiedFresh := false
	for i, spec := range runs {
		if verifiedFresh && spec.Mode == commitment.ModeUpdate && !spec.Fresh {
			spec.expected = observations[0].Rounds
		}
		got := execute(tb, c, spec, open)
		check(tb, c, got)
		if i != 0 {
			for round := range c.Rounds {
				if spec.Fresh && !bytes.Equal(observations[0].Rounds[round].Root, got.Rounds[round].Root) {
					tb.Fatalf("case=%s seed=%+v round=%d: v3 root differs from a trie rebuilt from the same state\n v3 %x\n fresh %x\nhistory: %+v", c.ID, c.Seed, round, observations[0].Rounds[round].Root, got.Rounds[round].Root, c.Rounds[:round+1])
				}
				require.Equal(tb, observations[0].Rounds[round].Root, got.Rounds[round].Root, "case=%s seed=%+v round=%d %s/%s", c.ID, c.Seed, round, runs[0].Name, spec.Name)
			}
		}
		verifiedFresh = verifiedFresh || spec.Fresh
		observations = append(observations, got)
	}
	return observations
}

func process(engine commitment.Trie, mode commitment.Mode, dir string, ops []commitmenttest.Op) ([]byte, error) {
	updates := commitment.NewUpdates(mode, dir, commitment.KeyToHexNibbleHash)
	defer updates.Close()
	for _, op := range ops {
		if op.Read {
			updates.TouchPlainKey(string(op.Key), nil, func(*commitment.KeyUpdate, []byte) {})
		} else {
			updates.TouchPlainKeyDirect(string(op.Key), Update(op))
		}
	}
	return engine.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
}

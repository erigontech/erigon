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

package rulesconfig

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash/ethashcfg"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/node/nodecfg"
)

type fakeL2Config struct {
	name string
}

func (f fakeL2Config) Name() string { return f.name }

func (f fakeL2Config) ResolveRules(l2Version, blockNum, blockTime uint64, r *chain.Rules) {}

func TestRegisterL2EngineAndCreateRulesEngineBareBones(t *testing.T) {
	want := ethash.NewFaker()
	RegisterL2Engine("testl2", func(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine {
		return want
	})
	t.Cleanup(func() { unregisterL2Engine("testl2") })

	chainConfig := &chain.Config{L2: fakeL2Config{name: "testl2"}}
	got := CreateRulesEngineBareBones(context.Background(), chainConfig, log.New())

	assert.Same(t, want, got)
}

func TestRegisterL2EngineDuplicatePanics(t *testing.T) {
	RegisterL2Engine("duptest", func(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine {
		return ethash.NewFaker()
	})
	t.Cleanup(func() { unregisterL2Engine("duptest") })

	assert.Panics(t, func() {
		RegisterL2Engine("duptest", func(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine {
			return ethash.NewFaker()
		})
	})
}

func TestCreateRulesEngineUnknownL2Panics(t *testing.T) {
	chainConfig := &chain.Config{L2: fakeL2Config{name: "unregisteredl2"}}

	require.Panics(t, func() {
		CreateRulesEngineBareBones(context.Background(), chainConfig, log.New())
	})
}

func TestRegisterL2EngineRejectsBadInput(t *testing.T) {
	assert.Panics(t, func() {
		RegisterL2Engine("", func(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine {
			return ethash.NewFaker()
		})
	})
	assert.Panics(t, func() {
		RegisterL2Engine("nilfunc", nil)
	})
}

func TestCreateRulesEngineUnknownL2PanicNamesTheStack(t *testing.T) {
	chainConfig := &chain.Config{L2: fakeL2Config{name: "unregisteredl2"}}

	require.PanicsWithValue(t, "no L2 rules engine registered for: unregisteredl2", func() {
		CreateRulesEngineBareBones(context.Background(), chainConfig, log.New())
	})
}

func TestCreateRulesEngineL2NotMergeWrapped(t *testing.T) {
	want := ethash.NewFaker()
	RegisterL2Engine("ttdl2", func(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine {
		return want
	})
	t.Cleanup(func() { unregisterL2Engine("ttdl2") })

	chainConfig := &chain.Config{L2: fakeL2Config{name: "ttdl2"}, TerminalTotalDifficulty: uint256.NewInt(0)}
	got := CreateRulesEngine(context.Background(), &nodecfg.Config{}, chainConfig, &ethashcfg.Config{PowMode: ethashcfg.ModeFake},
		true, nil, false, log.New())

	assert.Same(t, want, got)
}

func init() {
	chain.RegisterL2Config("jsonl2", func(raw json.RawMessage) (chain.L2Config, error) {
		return fakeL2Config{name: "jsonl2"}, nil
	})
}

func TestCreateRulesEngineResolvesL2JSON(t *testing.T) {
	want := ethash.NewFaker()
	RegisterL2Engine("jsonl2", func(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine {
		return want
	})
	t.Cleanup(func() { unregisterL2Engine("jsonl2") })

	chainConfig := &chain.Config{L2JSON: json.RawMessage(`{"name":"jsonl2"}`), TerminalTotalDifficulty: uint256.NewInt(0)}
	got := CreateRulesEngine(context.Background(), &nodecfg.Config{}, chainConfig, &ethashcfg.Config{PowMode: ethashcfg.ModeFake},
		true, nil, false, log.New())

	assert.Same(t, want, got)
	assert.Equal(t, fakeL2Config{name: "jsonl2"}, chainConfig.L2)
}

func TestCreateRulesEngineUndecodableL2JSONPanics(t *testing.T) {
	chainConfig := &chain.Config{L2JSON: json.RawMessage(`{"name":"undecodablel2"}`)}

	require.PanicsWithValue(t, `no L2 config registered: chain config l2 name "undecodablel2"`, func() {
		CreateRulesEngineBareBones(context.Background(), chainConfig, log.New())
	})
}

func TestCreateRulesEngineWithoutL2(t *testing.T) {
	got := CreateRulesEngineBareBones(context.Background(), &chain.Config{}, log.New())
	assert.IsType(t, &ethash.FakeEthash{}, got)

	got = CreateRulesEngineBareBones(context.Background(), &chain.Config{TerminalTotalDifficulty: uint256.NewInt(0)}, log.New())
	assert.IsType(t, &merge.Merge{}, got)
}

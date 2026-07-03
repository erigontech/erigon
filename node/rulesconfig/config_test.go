// Copyright 2024 The Erigon Authors
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
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

	chainConfig := &chain.Config{L2: fakeL2Config{name: "testl2"}}
	got := CreateRulesEngineBareBones(context.Background(), chainConfig, log.New())

	assert.Same(t, want, got)
}

func TestRegisterL2EngineDuplicatePanics(t *testing.T) {
	RegisterL2Engine("duptest", func(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine {
		return ethash.NewFaker()
	})

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

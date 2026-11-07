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

package protocol

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

var errPostProcess = errors.New("post-process")

// postProcessor records the transaction positions it runs after and fails
// after the one at fail.
type postProcessor struct {
	rules.EngineReader
	ran  []int
	fail int
}

func (p *postProcessor) PostProcessTx(_ *chain.Config, _ *state.IntraBlockState, txs types.Transactions, i int) error {
	p.ran = append(p.ran, i)
	if i == p.fail {
		return errPostProcess
	}
	return nil
}

func TestPostProcessTx(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	txs := types.Transactions{&types.LegacyTx{}, &types.LegacyTx{}}

	p := &postProcessor{fail: 1}
	require.NoError(t, PostProcessTx(p, chain.TestChainOsakaConfig, ibs, txs, 0))
	require.ErrorIs(t, PostProcessTx(p, chain.TestChainOsakaConfig, ibs, txs, 1), errPostProcess)
	require.Equal(t, []int{0, 1}, p.ran)

	// Engines without the rule, and no engine, leave the state alone.
	require.NoError(t, PostProcessTx(ethash.NewFaker(), chain.TestChainOsakaConfig, ibs, txs, 0))
	require.NoError(t, PostProcessTx(nil, chain.TestChainOsakaConfig, ibs, txs, 0))
}

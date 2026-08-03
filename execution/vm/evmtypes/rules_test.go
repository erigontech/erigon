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

package evmtypes

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/execution/chain"
)

// fakeL2Config is a minimal chain.L2Config used to exercise the ResolveRules
// oracle hook: it stamps L2Version and flips IsCancun once l2Version crosses
// a threshold, standing in for an L2 stack gating an EVM fork on its own
// version ladder instead of L1 time/number.
type fakeL2Config struct{}

func (fakeL2Config) Name() string { return "fakel2" }

func (fakeL2Config) ResolveRules(l2Version, _, _ uint64, rules *chain.Rules) {
	rules.L2Version = l2Version
	rules.IsCancun = l2Version >= 10
}

func TestBlockContextRulesL2Oracle(t *testing.T) {
	c := chain.Config{L2: fakeL2Config{}}
	bc := BlockContext{L2Version: 20}

	rules := bc.Rules(&c)

	assert.Equal(t, uint64(20), rules.L2Version)
	assert.True(t, rules.IsCancun)
}

func TestBlockContextRulesNoL2(t *testing.T) {
	var c chain.Config
	bc := BlockContext{L2Version: 20}

	rules := bc.Rules(&c)

	assert.Equal(t, uint64(0), rules.L2Version)
	assert.False(t, rules.IsCancun)
}

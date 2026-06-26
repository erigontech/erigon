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

package commitment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

type countingWitnessTracer struct{ branches, exts, leaves int }

func (c *countingWitnessTracer) onBranch([]byte, int16, uint16, *[16]cellEncodeData, []byte) {
	c.branches++
}
func (c *countingWitnessTracer) onExtensionLeaf([]byte, int16, []byte, *cell) { c.exts++ }
func (c *countingWitnessTracer) onLeaf(int16, *cell, []byte)                  { c.leaves++ }

func Test_WitnessTracer_HooksFire(t *testing.T) {
	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)

	builder := NewUpdateBuilder()
	for i := 0; i < 12; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
	}
	plainKeys, updates := builder.Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()

	ct := &countingWitnessTracer{}
	hph.witnessTracer = ct
	_, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
	require.NoError(t, err)

	require.Positive(t, ct.branches, "onBranch must fire during fold")
	require.Positive(t, ct.leaves, "onLeaf must fire for account leaves")
}

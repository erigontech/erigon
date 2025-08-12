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

package tests

import (
	"context"
	"embed"
	_ "embed"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/kv"
)

//go:embed test_data/electra/blocks_0.ssz_snappy
var electra_blocks_0_ssz_snappy []byte

//go:embed test_data/electra/blocks_1.ssz_snappy
var electra_blocks_1_ssz_snappy []byte

//go:embed test_data/electra/pre.ssz_snappy
var electra_pre_state_ssz_snappy []byte

//go:embed test_data/electra/post.ssz_snappy
var electra_post_state_ssz_snappy []byte

//go:embed test_data/capella/blocks_0.ssz_snappy
var capella_blocks_0_ssz_snappy []byte

//go:embed test_data/capella/blocks_1.ssz_snappy
var capella_blocks_1_ssz_snappy []byte

//go:embed test_data/capella/pre.ssz_snappy
var capella_pre_state_ssz_snappy []byte

//go:embed test_data/capella/post.ssz_snappy
var capella_post_state_ssz_snappy []byte

//go:embed test_data/phase0/blocks_0.ssz_snappy
var phase0_blocks_0_ssz_snappy []byte

//go:embed test_data/phase0/blocks_1.ssz_snappy
var phase0_blocks_1_ssz_snappy []byte

//go:embed test_data/phase0/pre.ssz_snappy
var phase0_pre_state_ssz_snappy []byte

//go:embed test_data/phase0/post.ssz_snappy
var phase0_post_state_ssz_snappy []byte

// bellatrix is long

//go:embed test_data/bellatrix
var bellatrixFS embed.FS

type MockBlockReader struct {
	U map[uint64]*cltypes.SignedBeaconBlock
}

func NewMockBlockReader() *MockBlockReader {
	return &MockBlockReader{U: make(map[uint64]*cltypes.SignedBeaconBlock)}
}

func (m *MockBlockReader) ReadBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	return m.U[slot], nil
}

func (m *MockBlockReader) ReadBlindedBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBlindedBeaconBlock, error) {
	if m.U[slot] == nil {
		return nil, nil
	}
	return m.U[slot].Blinded()
}

func (m *MockBlockReader) ReadBlockByRoot(ctx context.Context, tx kv.Tx, blockRoot common.Hash) (*cltypes.SignedBeaconBlock, error) {
	// do a linear search
	for _, v := range m.U {
		r, err := v.Block.HashSSZ()
		if err != nil {
			return nil, err
		}

		if r == blockRoot {
			return v, nil
		}
	}
	return nil, nil
}
func (m *MockBlockReader) ReadHeaderByRoot(ctx context.Context, tx kv.Tx, blockRoot common.Hash) (*cltypes.SignedBeaconBlockHeader, error) {
	block, err := m.ReadBlockByRoot(ctx, tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.SignedBeaconBlockHeader(), nil
}

func (m *MockBlockReader) FrozenSlots() uint64 {
	panic("implement me")
}

func LoadChain(blocks []*cltypes.SignedBeaconBlock, s *state.CachingBeaconState, db kv.RwDB, t *testing.T) *MockBlockReader {
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	m := NewMockBlockReader()
	for _, block := range blocks {
		m.U[block.Block.Slot] = block
		require.NoError(t, beacon_indicies.WriteBeaconBlockAndIndicies(context.Background(), tx, block, true))
		require.NoError(t, beacon_indicies.WriteHighestFinalized(tx, block.Block.Slot+64))
	}

	require.NoError(t, tx.Commit())
	return m
}

func GetElectraRandom() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState) {
	block1 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.ElectraVersion)
	block2 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.ElectraVersion)

	// Lets do te
	if err := utils.DecodeSSZSnappy(block1, electra_blocks_0_ssz_snappy, int(clparams.ElectraVersion)); err != nil {
		panic(err)
	}
	if err := utils.DecodeSSZSnappy(block2, electra_blocks_1_ssz_snappy, int(clparams.ElectraVersion)); err != nil {
		panic(err)
	}

	preState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(preState, electra_pre_state_ssz_snappy, int(clparams.ElectraVersion)); err != nil {
		panic(err)

	}
	postState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(postState, electra_post_state_ssz_snappy, int(clparams.ElectraVersion)); err != nil {
		panic(err)
	}
	return []*cltypes.SignedBeaconBlock{block1, block2}, preState, postState
}

func GetCapellaRandom() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState) {
	block1 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.CapellaVersion)
	block2 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.CapellaVersion)

	// Lets do te
	if err := utils.DecodeSSZSnappy(block1, capella_blocks_0_ssz_snappy, int(clparams.CapellaVersion)); err != nil {
		panic(err)
	}
	if err := utils.DecodeSSZSnappy(block2, capella_blocks_1_ssz_snappy, int(clparams.CapellaVersion)); err != nil {
		panic(err)
	}

	preState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(preState, capella_pre_state_ssz_snappy, int(clparams.CapellaVersion)); err != nil {
		panic(err)

	}
	postState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(postState, capella_post_state_ssz_snappy, int(clparams.CapellaVersion)); err != nil {
		panic(err)
	}
	return []*cltypes.SignedBeaconBlock{block1, block2}, preState, postState
}

func GetPhase0Random() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState) {
	block1 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block2 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)

	// Lets do te
	if err := utils.DecodeSSZSnappy(block1, phase0_blocks_0_ssz_snappy, int(clparams.Phase0Version)); err != nil {
		panic(err)
	}
	if err := utils.DecodeSSZSnappy(block2, phase0_blocks_1_ssz_snappy, int(clparams.Phase0Version)); err != nil {
		panic(err)
	}

	preState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(preState, phase0_pre_state_ssz_snappy, int(clparams.Phase0Version)); err != nil {
		panic(err)
	}
	postState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(postState, phase0_post_state_ssz_snappy, int(clparams.Phase0Version)); err != nil {
		panic(err)
	}
	return []*cltypes.SignedBeaconBlock{block1, block2}, preState, postState
}

func GetBellatrixRandom() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState) {
	ret := make([]*cltypes.SignedBeaconBlock, 0, 96)
	// format for blocks is blocks_{i}.ssz_snappy where i is the index of the block, starting from 0 to 95 included.
	for i := 0; i < 96; i++ {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.BellatrixVersion)
		// Lets do te
		b, err := bellatrixFS.ReadFile("test_data/bellatrix/blocks_" + strconv.Itoa(i) + ".ssz_snappy")
		if err != nil {
			panic(err)
		}
		if err := utils.DecodeSSZSnappy(block, b, int(clparams.BellatrixVersion)); err != nil {
			panic(err)
		}
		ret = append(ret, block)
	}
	preState := state.New(&clparams.MainnetBeaconConfig)
	b, err := bellatrixFS.ReadFile("test_data/bellatrix/pre.ssz_snappy")
	if err != nil {
		panic(err)
	}
	if err := utils.DecodeSSZSnappy(preState, b, int(clparams.BellatrixVersion)); err != nil {
		panic(err)
	}
	postState := state.New(&clparams.MainnetBeaconConfig)
	b, err = bellatrixFS.ReadFile("test_data/bellatrix/post.ssz_snappy")
	if err != nil {
		panic(err)
	}
	if err := utils.DecodeSSZSnappy(postState, b, int(clparams.BellatrixVersion)); err != nil {
		panic(err)
	}
	return ret, preState, postState

}

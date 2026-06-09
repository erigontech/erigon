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

package forkchoice

import (
	"context"
	_ "embed"
	"math"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

//go:embed test_data/anchor_state.ssz_snappy
var diffAnchorEnc []byte

//go:embed test_data/block_0x3af8b5b42ca135c75b32abb32b3d71badb73695d3dc638bacfb6c8b7bcbee1a9.ssz_snappy
var diffBlock3aEnc []byte

//go:embed test_data/block_0xc2788d6005ee2b92c3df2eff0aeab0374d155fa8ca1f874df305fa376ce334cf.ssz_snappy
var diffBlockc2Enc []byte

//go:embed test_data/block_0xd4503d46e43df56de4e19acb0f93b3b52087e422aace49a7c3816cf59bafb0ad.ssz_snappy
var diffBlockd4Enc []byte

//go:embed test_data/attestation_0xfb924d35b2888d9cd70e6879c1609e6cad7ea3b028a501967747d96e49068cb6.ssz_snappy
var diffAttEnc []byte

// buildExAnteStore reconstructs the ex-ante fork-choice scenario with an
// attestation processed, so both f.latestMessages and the indexed weight store
// are populated.
func buildExAnteStore(tb testing.TB) *ForkChoiceStore {
	tb.Helper()
	ctx := context.Background()
	cfg := &clparams.MainnetBeaconConfig
	sd := synced_data.NewSyncedDataManager(cfg, true)
	b3a := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	bc2 := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	bd4 := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	require.NoError(tb, utils.DecodeSSZSnappy(b3a, diffBlock3aEnc, int(clparams.AltairVersion)))
	require.NoError(tb, utils.DecodeSSZSnappy(bc2, diffBlockc2Enc, int(clparams.AltairVersion)))
	require.NoError(tb, utils.DecodeSSZSnappy(bd4, diffBlockd4Enc, int(clparams.AltairVersion)))
	att := &solid.Attestation{}
	require.NoError(tb, utils.DecodeSSZSnappy(att, diffAttEnc, int(clparams.AltairVersion)))
	anchor := state2.New(cfg)
	require.NoError(tb, utils.DecodeSSZSnappy(anchor, diffAnchorEnc, int(clparams.AltairVersion)))
	em := beaconevents.NewEventEmitter()
	gs, err := initial_state.GetGenesisState(1)
	require.NoError(tb, err)
	clk := eth_clock.NewEthereumClock(gs.GenesisTime(), gs.GenesisValidatorsRoot(), cfg)
	bs := blob_storage.NewBlobStore(memdb.NewTestDB(tb, dbcfg.ChainDB), afero.NewMemMapFs(), math.MaxUint64, cfg, clk)
	store, err := NewForkChoiceStore(clk, anchor, nil, pool.NewOperationsPool(cfg),
		fork_graph.NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{}, em),
		em, sd, bs, public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(), false, nil)
	require.NoError(tb, err)
	store.OnTick(0)
	store.OnTick(12)
	require.NoError(tb, store.OnBlock(ctx, b3a, false, true, false))
	store.OnTick(36)
	require.NoError(tb, store.OnBlock(ctx, bc2, false, true, false))
	require.NoError(tb, store.OnBlock(ctx, bd4, false, true, false))
	store.SetSynced(true)
	s0, err := store.GetStateAtBlockRoot(store.ProposerBoostRoot(), true)
	require.NoError(tb, err)
	require.NoError(tb, sd.OnHeadState(s0))
	require.NoError(tb, store.OnAttestation(att, false, false))
	// Indexed-vote maintenance is gated to the GLOAS vote path, so the pre-GLOAS
	// fixtures above fill latestMessages but not the index. Mirror latestMessages
	// into the index directly so the differential check scores the index against
	// the full-scan store over the same votes.
	for i := 0; i < store.latestMessages.latestMessagesCount(); i++ {
		if msg, has := store.latestMessages.get(i); has && msg != (LatestMessage{}) {
			store.indexedWeightStore.IndexVote(uint64(i), msg)
		}
	}
	return store
}

// TestIndexedWeightStoreMatchesFullScan asserts the incremental indexedWeightStore
// returns the same attestation score and weight as the trusted full-scan weightStore
// for every node in the filtered block tree, given the same checkpoint state.
func TestIndexedWeightStoreMatchesFullScan(t *testing.T) {
	f := buildExAnteStore(t)

	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	// Mirror the production contract: getCheckpointState runs outside the lock,
	// the scoring pass (headWeightStore + queries) holds f.mu.
	f.mu.Lock()
	defer f.mu.Unlock()

	full := NewWeightStore(f)    // trusted O(V) full-scan oracle
	idx := f.headWeightStore(cs) // incremental index under test

	blocks := f.getFilteredBlockTree(justified.Root)
	require.NotEmpty(t, blocks)

	sawNonZero := false
	for root := range blocks {
		node := ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending}
		wantScore := full.GetAttestationScore(node)
		wantWeight := full.GetWeight(node)
		require.Equalf(t, wantScore, idx.GetAttestationScore(node), "attestation score mismatch at %x", root)
		require.Equalf(t, wantWeight, idx.GetWeight(node), "weight mismatch at %x", root)
		if wantWeight > 0 {
			sawNonZero = true
		}
	}
	require.True(t, sawNonZero, "differential check is vacuous: no node carried weight")
}

// BenchmarkHeadWeight_IndexedVsFullScan compares the indexed store against the
// full-scan store on the same scenario. The indexed store traverses only the
// votes in the subtree; the full-scan store iterates every validator per query.
func BenchmarkHeadWeight_IndexedVsFullScan(b *testing.B) {
	f := buildExAnteStore(b)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, _ := f.getCheckpointState(justified)
	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}

	b.Run("indexed", func(b *testing.B) {
		idx := f.headWeightStore(cs)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = idx.GetAttestationScore(node)
		}
	})
	b.Run("fullscan", func(b *testing.B) {
		full := NewWeightStore(f)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = full.GetAttestationScore(node)
		}
	})
}

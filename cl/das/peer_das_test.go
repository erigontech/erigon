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

package das

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/common"
)

func TestSyncColumnDataLaterStoresCompactFuluBlock(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	d := &peerdas{}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, d.SyncColumnDataLater(block))

	value, ok := d.blocksToCheckSync[common.Hash(blockRoot)]
	require.True(t, ok)
	queued, compact := value.block.(*deferredColumnSyncBlock)
	require.True(t, compact)
	require.Equal(t, block.Block.Slot, queued.slot)
	require.Equal(t, common.Hash(blockRoot), queued.root)
	require.Equal(t, block.Block.Body.BlobKzgCommitments.Len(), queued.commitments.Len())
}

func TestSyncColumnDataLaterBoundsQueue(t *testing.T) {
	const queueLimit = maxDeferredColumnSyncBlocks
	cfg := clparams.MainnetBeaconConfig
	d := &peerdas{}
	for slot := uint64(1); slot <= queueLimit+1; slot++ {
		block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
		block.Block.Slot = slot
		block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
		require.NoError(t, d.SyncColumnDataLater(block))
	}

	require.LessOrEqual(t, len(d.blocksToCheckSync), queueLimit)
}

func TestDownloadOnlyCustodyColumnsWithoutRPC(t *testing.T) {
	d := &peerdas{}
	require.Error(t, d.DownloadOnlyCustodyColumns(context.Background(), nil))
}

// initTestBeaconConfig installs cfg as the global config if no test has done so
// yet. InitGlobalStaticConfig panics on a second call, so tests in this package
// must agree on every global-only field; they may differ only in fork epochs,
// which each test reads from its own local config.
func initTestBeaconConfig(cfg *clparams.BeaconChainConfig) {
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(cfg, &clparams.CaplinConfig{})
	}
}

func TestIsExpectedColumnDownloadMiss(t *testing.T) {
	require.False(t, isExpectedColumnDownloadMiss(nil))
	require.True(t, isExpectedColumnDownloadMiss(&httpreqresp.PeerResponseError{
		Code: httpreqresp.ResponseCodeResourceUnavailable,
	}))
	require.True(t, isExpectedColumnDownloadMiss(fmt.Errorf("column miss: %w", &httpreqresp.PeerResponseError{
		Code: httpreqresp.ResponseCodeResourceUnavailable,
	})))
	require.False(t, isExpectedColumnDownloadMiss(&httpreqresp.PeerResponseError{
		Code:    httpreqresp.ResponseCodeServerError,
		Message: "broken",
	}))
	require.False(t, isExpectedColumnDownloadMiss(&httpreqresp.HTTPError{
		StatusCode: 400,
		Body:       "Read Code: EOF",
	}))
	require.False(t, isExpectedColumnDownloadMiss(errors.New("peer error code: 2 (server error). Error message: broken")))
}

func TestResolveColumnSidecarSlotAndRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	d := &peerdas{beaconConfig: &cfg}
	spe := cfg.SlotsPerEpoch

	t.Run("rejects Gloas schema carrying a pre-Gloas slot", func(t *testing.T) {
		// A peer selects the Gloas decode schema (no SignedBlockHeader) via the
		// response fork-digest, then claims slot 0, which maps to a pre-Gloas
		// fork. The pre-Gloas branch must not dereference the absent header.
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		sidecar.Slot = 0
		require.Nil(t, sidecar.SignedBlockHeader)
		_, _, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.False(t, ok)
	})

	t.Run("rejects pre-Gloas schema with nil signed block header", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		sidecar.SignedBlockHeader = nil
		_, _, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.False(t, ok)
	})

	t.Run("accepts a consistent Fulu sidecar", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		require.NotNil(t, sidecar.SignedBlockHeader)
		sidecar.SignedBlockHeader.Header.Slot = spe // epoch 1 => Fulu
		slot, blockRoot, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.True(t, ok)
		require.Equal(t, spe, slot)
		want, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, common.Hash(want), blockRoot)
	})

	t.Run("accepts a consistent Gloas sidecar", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		sidecar.Slot = 2 * spe // epoch 2 => Gloas
		sidecar.BeaconBlockRoot = common.HexToHash("0xabc")
		slot, blockRoot, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.True(t, ok)
		require.Equal(t, 2*spe, slot)
		require.Equal(t, common.HexToHash("0xabc"), blockRoot)
	})
}

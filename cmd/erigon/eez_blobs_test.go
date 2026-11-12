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

package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/node/eth"
	"github.com/erigontech/erigon/node/ethconfig"
)

// blobStore holds blobs by execution block hash and counts its reads.
type blobStore struct {
	blobs map[common.Hash][][]byte
	reads int
}

func (s *blobStore) ExecutionBlobs(_ context.Context, hash common.Hash, _ uint64) ([][]byte, bool, error) {
	s.reads++
	blobs, ok := s.blobs[hash]
	return blobs, ok, nil
}

// TestBlobSources: the follower of an all-in-one node reads blobs from
// --eez.l1.blobs, then from the L1's consensus layer, then from
// --eez.l1.beacon.
func TestBlobSources(t *testing.T) {
	blob := make([]byte, l1.BlobSize)
	blob[1] = 1
	hash, err := l1.VersionedHash(blob)
	require.NoError(t, err)
	h := &l1.Header{Number: 7, Hash: common.Hash{0x07}, Time: 70}
	ctx := context.Background()
	var beaconCalls atomic.Int32
	beacon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		beaconCalls.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(beacon.Close)

	require.Nil(t, blobSources(&eez.Config{}, nil))

	t.Run("consensus layer", func(t *testing.T) {
		calls := beaconCalls.Load()
		local := &blobStore{blobs: map[common.Hash][][]byte{h.Hash: {blob}}}
		got, err := blobSources(&eez.Config{L1Beacon: beacon.URL}, local).Blobs(ctx, h, []common.Hash{hash})
		require.NoError(t, err)
		require.Equal(t, [][]byte{blob}, got)
		require.Equal(t, 1, local.reads)
		require.Zero(t, beaconCalls.Load()-calls)
	})
	t.Run("consensus layer alone", func(t *testing.T) {
		_, err := blobSources(&eez.Config{}, &blobStore{}).Blobs(ctx, h, []common.Hash{hash})
		require.ErrorIs(t, err, l1.ErrBlobUnavailable)
		require.NotErrorIs(t, err, l1.ErrBlobLost)
	})
	t.Run("beacon API after the consensus layer", func(t *testing.T) {
		calls := beaconCalls.Load()
		local := &blobStore{}
		_, err := blobSources(&eez.Config{L1Beacon: beacon.URL}, local).Blobs(ctx, h, []common.Hash{hash})
		require.Error(t, err)
		require.Equal(t, 1, local.reads)
		require.NotZero(t, beaconCalls.Load()-calls)
	})
	t.Run("directory first", func(t *testing.T) {
		dir := t.TempDir()
		local := &blobStore{blobs: map[common.Hash][][]byte{h.Hash: {blob}}}
		got, err := blobSources(&eez.Config{L1Blobs: dir}, local).Blobs(ctx, h, []common.Hash{hash})
		require.NoError(t, err)
		require.Equal(t, [][]byte{blob}, got)
		stored, err := os.ReadFile(filepath.Join(dir, hash.Hex()+".blob"))
		require.NoError(t, err, "the directory keeps what the consensus layer served")
		require.Equal(t, blob, stored)

		got, err = blobSources(&eez.Config{L1Blobs: dir}, local).Blobs(ctx, h, []common.Hash{hash})
		require.NoError(t, err)
		require.Equal(t, [][]byte{blob}, got)
		require.Equal(t, 1, local.reads, "read from the directory")
	})
}

// TestAllInOneBlobWarning: an all-in-one node warns when its EEZ chain
// takes its blobs from L1 while the L1's consensus layer keeps no full
// blobs.
func TestAllInOneBlobWarning(t *testing.T) {
	eez.Register()
	for _, c := range []struct {
		name  string
		chain string
		set   func(*ethconfig.Config)
		warns bool
	}{
		{"rollupX", networks.Chiado699169.Name, func(*ethconfig.Config) {}, true},
		{"rollupX, blobs archive", networks.Chiado699169.Name, func(e *ethconfig.Config) { e.CaplinConfig.ArchiveBlobs = true }, false},
		{"rollupX, blob backfill", networks.Chiado699169.Name, func(e *ethconfig.Config) { e.CaplinConfig.ImmediateBlobsBackfilling = true }, false},
		{"rollupX, external consensus layer", networks.Chiado699169.Name, func(e *ethconfig.Config) { e.InternalCL = false }, false},
		{"rollup0", networks.Devnet.Name, func(*ethconfig.Config) {}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			l1Node, l1Eth := l1Configs(t, 1337)
			c.set(l1Eth)
			var out bytes.Buffer
			logger := log.New()
			logger.SetHandler(log.StreamHandler(&out, log.LogfmtFormat()))
			l1Only := true
			_, _, err := allInOneConfigs(t.Context(), l1Node, l1Eth, &eez.Config{Chain: c.chain, HTTPPort: 8645, SyncL1Only: &l1Only}, logger)
			require.NoError(t, err)
			if c.warns {
				require.Contains(t, out.String(), "lvl=warn")
				require.Contains(t, out.String(), "keeps full blobs only with --caplin.blobs-immediate-backfill")
			} else {
				require.NotContains(t, out.String(), "keeps full blobs only")
			}
		})
	}
}

// TestCaplinBlobsWithoutCaplin: a node without an embedded consensus layer
// gives its follower no local blob store, not one holding a nil reader.
func TestCaplinBlobsWithoutCaplin(t *testing.T) {
	require.True(t, caplinBlobs(&eth.Ethereum{}) == nil)
	require.True(t, blobSources(&eez.Config{}, caplinBlobs(&eth.Ethereum{})) == nil)
}

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

package network

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestRecordInvalidPeerChainResult(t *testing.T) {
	t.Run("preserves accepted prefix and prefers HTTP", func(t *testing.T) {
		downloader := &ForwardBeaconDownloader{
			highestSlotProcessed:  100,
			highestSlotUpdateTime: time.Unix(1, 0),
			httpFallbackURL:       "https://beacon.example",
		}

		handled := downloader.recordInvalidPeerChainResultLocked(120, fmt.Errorf("batch rejected: %w", ErrInvalidPeerChain), true)

		require.True(t, handled)
		require.Equal(t, uint64(120), downloader.highestSlotProcessed)
		require.True(t, downloader.highestSlotUpdateTime.After(time.Unix(1, 0)))
		require.True(t, downloader.httpPreferred.Load())
	})

	t.Run("does not roll progress back", func(t *testing.T) {
		downloader := &ForwardBeaconDownloader{highestSlotProcessed: 100}

		require.True(t, downloader.recordInvalidPeerChainResultLocked(99, ErrInvalidPeerChain, true))
		require.Equal(t, uint64(100), downloader.highestSlotProcessed)
		require.False(t, downloader.httpPreferred.Load())
	})

	t.Run("ignores unrelated error", func(t *testing.T) {
		downloader := &ForwardBeaconDownloader{
			highestSlotProcessed: 100,
			httpFallbackURL:      "https://beacon.example",
		}

		require.False(t, downloader.recordInvalidPeerChainResultLocked(120, errors.New("invalid block"), true))
		require.Equal(t, uint64(100), downloader.highestSlotProcessed)
		require.False(t, downloader.httpPreferred.Load())
	})

	t.Run("invalid HTTP chain falls back to P2P", func(t *testing.T) {
		downloader := &ForwardBeaconDownloader{
			highestSlotProcessed: 100,
			httpFallbackURL:      "https://beacon.example",
		}
		downloader.httpPreferred.Store(true)

		require.True(t, downloader.recordInvalidPeerChainResultLocked(100, ErrInvalidPeerChain, false))
		require.False(t, downloader.httpPreferred.Load())
	})
}

func TestFetchEnvelopesFromBeaconAPIUsesPluralEndpoint(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = 1_218
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(cfg),
	}
	envelope.Message.BeaconBlockRoot = root
	body, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/eth/v1/beacon/execution_payload_envelopes/1218", r.URL.Path)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, err := w.Write(body)
		require.NoError(t, err)
	}))
	defer server.Close()

	received := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, cfg,
	)

	require.Equal(t, 1, fetched)
	require.Contains(t, received, common.Hash(root))
}

func TestFetchEnvelopesFromBeaconAPIRejectsMismatchedRoot(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = 1_218
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(cfg),
	}
	envelope.Message.BeaconBlockRoot = common.Hash{1}
	body, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write(body)
		require.NoError(t, err)
	}))
	defer server.Close()

	received := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, cfg,
	)

	require.Zero(t, fetched)
	require.Empty(t, received)
}

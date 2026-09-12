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

package stages

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common"
)

func TestFetchBlocksFromSourcesPrefersHealthyP2POverLaggingHTTP(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	fresh := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 12}}
	stale := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 10}}
	p2pCalls := 0
	httpCalls := 0

	got, err := fetchBlocksFromSources(
		t.Context(), 10, 3, 11, "https://checkpoint.example", &cfg,
		func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			p2pCalls++
			return []*cltypes.SignedBeaconBlock{fresh}, "peer", nil
		},
		func(context.Context, string, uint64, uint64, *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
			httpCalls++
			return []*cltypes.SignedBeaconBlock{stale}, nil
		},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 1, p2pCalls)
	require.Zero(t, httpCalls)
	require.Equal(t, "peer", got.Peer)
	require.Equal(t, []*cltypes.SignedBeaconBlock{fresh}, got.Data)
}

func TestFetchBlocksFromSourcesUsesHTTPWhenP2PIsUnavailable(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	fallback := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 11}}
	httpCalls := 0

	got, err := fetchBlocksFromSources(
		t.Context(), 10, 3, 11, "https://checkpoint.example", &cfg,
		func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return nil, "", context.DeadlineExceeded
		},
		func(context.Context, string, uint64, uint64, *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
			httpCalls++
			return []*cltypes.SignedBeaconBlock{fallback}, nil
		},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 1, httpCalls)
	require.Equal(t, "http-fallback", got.Peer)
	require.Equal(t, []*cltypes.SignedBeaconBlock{fallback}, got.Data)
}

func TestFetchEnvelopeSourcesReservesTimeForP2P(t *testing.T) {
	root := [32]byte{1}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{}
	p2pCalled := false

	got := fetchEnvelopeSources(
		t.Context(), [][32]byte{root}, 10*time.Millisecond,
		func(ctx context.Context) map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope {
			<-ctx.Done()
			return nil
		},
		func(context.Context, [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
			p2pCalled = true
			return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): envelope}, nil
		},
	)

	require.True(t, p2pCalled)
	require.Same(t, envelope, got[common.Hash(root)])
}

func TestFetchEnvelopeSourcesSkipsHTTPWhenOnlyP2PReserveRemains(t *testing.T) {
	root := [32]byte{1}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{}
	httpCalled := false
	p2pCalled := false
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	got := fetchEnvelopeSources(
		ctx, [][32]byte{root}, time.Second,
		func(ctx context.Context) map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope {
			httpCalled = true
			<-ctx.Done()
			return nil
		},
		func(context.Context, [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
			p2pCalled = true
			return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): envelope}, nil
		},
	)

	require.False(t, httpCalled)
	require.True(t, p2pCalled)
	require.Same(t, envelope, got[common.Hash(root)])
}

func TestFetchBlocksFromSourcesRetriesP2PAfterHTTPBudget(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	fresh := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 12}}
	p2pCalls := 0
	ctx, cancel := context.WithTimeout(t.Context(), 1300*time.Millisecond)
	defer cancel()

	got, err := fetchBlocksFromSources(
		ctx, 10, 3, 11, "https://checkpoint.example", &cfg,
		func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			p2pCalls++
			if p2pCalls == 1 {
				return nil, "", context.DeadlineExceeded
			}
			return []*cltypes.SignedBeaconBlock{fresh}, "peer", nil
		},
		func(ctx context.Context, _ string, _, _ uint64, _ *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 2, p2pCalls)
	require.Equal(t, "peer", got.Peer)
}

func TestFetchBlocksFromSourcesRejectsHTTPWithoutForwardProgress(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	stale := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 10}}
	fresh := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 13}}
	p2pCalls := 0

	got, err := fetchBlocksFromSources(
		t.Context(), 10, 4, 11, "https://checkpoint.example", &cfg,
		func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			p2pCalls++
			if p2pCalls == 1 {
				return nil, "", errors.New("temporary P2P failure")
			}
			return []*cltypes.SignedBeaconBlock{fresh}, "peer", nil
		},
		func(context.Context, string, uint64, uint64, *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
			return []*cltypes.SignedBeaconBlock{stale}, nil
		},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 2, p2pCalls)
	require.Equal(t, "peer", got.Peer)
	require.Equal(t, []*cltypes.SignedBeaconBlock{fresh}, got.Data)
}

func TestFetchBlocksFromSourcesCapsHTTPFallbackBatch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	fresh := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 11}}
	requested := uint64(0)

	got, err := fetchBlocksFromSources(
		t.Context(), 10, 1_000_000, 11, "https://checkpoint.example", &cfg,
		func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return nil, "", errors.New("no P2P peers")
		},
		func(_ context.Context, _ string, _ uint64, count uint64, _ *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
			requested = count
			return []*cltypes.SignedBeaconBlock{fresh}, nil
		},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, uint64(maxChainTipHTTPBlockCount), requested)
	require.Equal(t, "http-fallback", got.Peer)
}

func TestFetchBlocksFromSourcesAdvancesHTTPFallbackPastEmptyPage(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	fresh := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 106}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var starts []uint64

	got, err := fetchBlocksFromSources(
		ctx, 98, 16, 101, "https://checkpoint.example", &cfg,
		func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return nil, "", peers.ErrNoPeers
		},
		func(_ context.Context, _ string, start, _ uint64, _ *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
			starts = append(starts, start)
			switch start {
			case 98:
				if len(starts) > 1 {
					cancel()
					return nil, context.Canceled
				}
				return nil, nil
			case 106:
				return []*cltypes.SignedBeaconBlock{fresh}, nil
			default:
				cancel()
				return nil, context.Canceled
			}
		},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, []uint64{98, 106}, starts)
	require.Equal(t, "http-fallback", got.Peer)
	require.Equal(t, []*cltypes.SignedBeaconBlock{fresh}, got.Data)
}

func TestFetchBlocksFromSourcesPersistsHTTPScanAcrossInvocations(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	scan := &chainTipHTTPBlockScan{}
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	var firstStarts []uint64

	_, err := fetchBlocksFromSources(
		firstCtx, 98, 16, 101, "https://checkpoint.example", &cfg,
		func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return nil, "", peers.ErrNoPeers
		},
		func(_ context.Context, _ string, start, _ uint64, _ *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
			firstStarts = append(firstStarts, start)
			if len(firstStarts) == 1 {
				return nil, nil
			}
			cancelFirst()
			return nil, context.Canceled
		},
		scan,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{98, 106}, firstStarts)

	fresh := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 106}}
	var resumedAt uint64
	got, err := fetchBlocksFromSources(
		t.Context(), 98, 16, 101, "https://checkpoint.example", &cfg,
		func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return nil, "", peers.ErrNoPeers
		},
		func(_ context.Context, _ string, start, _ uint64, _ *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
			resumedAt = start
			return []*cltypes.SignedBeaconBlock{fresh}, nil
		},
		scan,
	)

	require.NoError(t, err)
	require.Equal(t, uint64(106), resumedAt)
	require.Equal(t, "http-fallback", got.Peer)
}

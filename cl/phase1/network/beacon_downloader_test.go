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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

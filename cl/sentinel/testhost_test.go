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

package sentinel

import (
	"testing"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/p2p/discover"
)

// stubP2P satisfies p2p.P2PManager with only the host the connection handler needs.
type stubP2P struct{ host host.Host }

func (s stubP2P) Pubsub() *pubsub.PubSub                       { return nil }
func (s stubP2P) Host() host.Host                              { return s.host }
func (s stubP2P) BandwidthCounter() *metrics.BandwidthCounter  { return nil }
func (s stubP2P) UDPv5Listener() *discover.UDPv5               { return nil }
func (s stubP2P) UpdateENRAttSubnets(subnetIndex int, on bool) {}
func (s stubP2P) UpdateENRSyncNets(subnetIndex int, on bool)   {}

func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

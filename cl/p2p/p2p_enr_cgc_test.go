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

package p2p

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enr"
)

// setupENR once wrote the cgc entry as an empty byte slice, which is the canonical
// encoding of zero, so the node advertised custody of no groups. This drives the real
// setup path rather than the encoder, so reverting that line fails here.
func TestSetupENRAdvertisesTheCustodyRequirement(t *testing.T) {
	netCfg, beaconCfg, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	cfg := &P2PConfig{
		NetworkConfig: netCfg,
		BeaconConfig:  beaconCfg,
		IpAddr:        "127.0.0.1",
		Port:          0,
		TCPPort:       4001,
		TmpDir:        t.TempDir(),
	}
	listener, err := NewUDPv5Listener(context.Background(), cfg, discover.Config{PrivateKey: privKey}, log.Root())
	require.NoError(t, err)
	defer listener.LocalNode().Database().Close()
	defer listener.Close()

	p := &p2pManager{
		cfg:      cfg,
		udpv5:    listener,
		ethClock: eth_clock.NewEthereumClock(0, common.Hash{}, beaconCfg),
	}
	require.NoError(t, p.setupENR())

	var got []byte
	require.NoError(t, listener.LocalNode().Node().Load(enr.WithEntry(cfg.NetworkConfig.CgcKey, &got)))

	// fulu p2p-interface.md: big endian, no leading zero bytes. CUSTODY_REQUIREMENT is
	// 4 on every network, so one byte - not a zero-padded uint64.
	require.NotEmpty(t, got, "cgc advertised as empty, i.e. custody of no groups")
	require.Equal(t, []byte{byte(beaconCfg.CustodyRequirement)}, got)
}

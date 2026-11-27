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

//go:build unix

package sentinel

import (
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// The node's shutdown listener registers for SIGINT before the sentinel starts; the sentinel must leave it in place.
func TestNewKeepsTheSIGINTHandlers(t *testing.T) {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, syscall.SIGINT)
	defer signal.Stop(interrupts)
	ethClock := getEthClock(t)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	_, err := New(t.Context(), &SentinelConfig{
		P2PConfig: p2p.P2PConfig{NetworkConfig: networkConfig, BeaconConfig: beaconConfig, MaxPeerCount: 1},
	}, ethClock, nil, nil, nil, log.New(), &mock_services.ForkChoiceStorageMock{}, nil, newMockPeerDasStateReader(t), newTestP2PManager(t, ethClock))
	require.NoError(t, err)

	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGINT))
	select {
	case <-interrupts:
	case <-time.After(5 * time.Second):
		t.Fatal("SIGINT did not reach the handler registered before the sentinel")
	}
}

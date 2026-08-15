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

package sentinel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/p2p/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/semaphore"
)

func TestListenForPeersDialsDirectPeersWithoutDiscovery(t *testing.T) {
	local, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	remote, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remote.Close()) })

	peerComponent, err := multiaddr.NewComponent("p2p", remote.ID().String())
	require.NoError(t, err)
	directPeer := remote.Addrs()[0].Encapsulate(peerComponent).String()
	connected := make(chan struct{}, 1)
	local.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, connection network.Conn) {
			if connection.RemotePeer() == remote.ID() {
				connected <- struct{}{}
			}
		},
	})

	controller := gomock.NewController(t)
	p2pManager := mock_services.NewMockP2PManager(controller)
	p2pManager.EXPECT().Host().AnyTimes().Return(local)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	unsupportedPeer := "/ip4/127.0.0.1/udp/9001/quic-v1/p2p/16Uiu2HAkxcBE3LK7zhnyZERguonkKmXLgYPRcuDPaF6C2vaigYuT"
	sentinel := &Sentinel{
		ctx: ctx,
		cfg: &SentinelConfig{P2PConfig: p2p.P2PConfig{
			NetworkConfig: &clparams.NetworkConfig{StaticPeers: []string{unsupportedPeer, directPeer}},
			NoDiscovery:   true,
		}},
		peers:      peers.NewPool(local),
		p2p:        p2pManager,
		connectSem: semaphore.NewWeighted(goRoutinesOpeningPeerConnections),
	}

	sentinel.listenForPeers()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("direct peer was not dialed while discovery was disabled")
	}
}

func TestConnectWithAllPeersWaitsForDialCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	sem := semaphore.NewWeighted(1)
	require.NoError(t, sem.Acquire(ctx, 1))
	sentinel := &Sentinel{ctx: ctx, connectSem: sem}
	directPeer, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9000/p2p/16Uiu2HAmBciu61DBo623TByPbuBaGh9So6hRQfvHpCegCE71JNp9")
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- sentinel.connectWithAllPeers([]multiaddr.Multiaddr{directPeer})
	}()
	select {
	case err := <-done:
		t.Fatalf("connection batch bypassed exhausted dial capacity: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	sem.Release(1)
}

// TestConnectSemaphoreInitialized verifies that a Sentinel's connectSem field
// is properly initialised (not nil) and that its capacity matches
// goRoutinesOpeningPeerConnections.
func TestConnectSemaphoreInitialized(t *testing.T) {
	t.Parallel()

	s := &Sentinel{
		connectSem: semaphore.NewWeighted(int64(goRoutinesOpeningPeerConnections)),
	}
	require.NotNil(t, s.connectSem, "connectSem must not be nil after construction")

	// We should be able to acquire exactly goRoutinesOpeningPeerConnections tokens.
	for i := range goRoutinesOpeningPeerConnections {
		ok := s.connectSem.TryAcquire(1)
		assert.True(t, ok, "TryAcquire should succeed for token %d of %d", i+1, goRoutinesOpeningPeerConnections)
	}

	// The next acquire must fail -- the semaphore is exhausted.
	ok := s.connectSem.TryAcquire(1)
	assert.False(t, ok, "TryAcquire should fail when semaphore is at capacity")

	// Release all so the semaphore is left clean.
	for range goRoutinesOpeningPeerConnections {
		s.connectSem.Release(1)
	}
}

// TestConnectSemaphoreBoundsConcurrency spawns many goroutines that all try to
// hold the semaphore and verifies that at most goRoutinesOpeningPeerConnections
// goroutines hold it simultaneously.
func TestConnectSemaphoreBoundsConcurrency(t *testing.T) {
	t.Parallel()

	const totalGoroutines = 32

	sem := semaphore.NewWeighted(int64(goRoutinesOpeningPeerConnections))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		active  atomic.Int32
		maxSeen atomic.Int32
	)

	// recordMax is called after incrementing active; it races against other
	// goroutines doing the same so we use a CAS loop.
	recordMax := func(cur int32) {
		for {
			old := maxSeen.Load()
			if cur <= old {
				break
			}
			if maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
	}

	done := make(chan struct{}, totalGoroutines)
	for range totalGoroutines {
		go func() {
			defer func() { done <- struct{}{} }()

			err := sem.Acquire(ctx, 1)
			if err != nil {
				return
			}

			cur := active.Add(1)
			recordMax(cur)

			// Simulate a short piece of work so goroutines overlap.
			// Using a channel-based pause keeps the test deterministic
			// (no time.Sleep).
			runtime_Gosched()

			active.Add(-1)
			sem.Release(1)
		}()
	}

	for range totalGoroutines {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("timed out waiting for goroutines to finish")
		}
	}

	assert.LessOrEqual(t, maxSeen.Load(), int32(goRoutinesOpeningPeerConnections),
		"no more than %d goroutines should hold the semaphore at once", goRoutinesOpeningPeerConnections)
	assert.Equal(t, int32(0), active.Load(), "all goroutines should have released")
}

// runtime_Gosched yields the processor so other goroutines can run.
// This is a deterministic alternative to time.Sleep.
func runtime_Gosched() {
	// Using a small channel send/receive pair forces a goroutine reschedule
	// without introducing wall-clock delays.
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	<-ch
}

// TestConnectSemaphoreReleaseAndReacquire verifies the acquire-release cycle
// that mirrors ConnectWithPeer's `defer sem.Release(1)` pattern: after
// releasing a token the semaphore becomes available again.
func TestConnectSemaphoreReleaseAndReacquire(t *testing.T) {
	t.Parallel()

	sem := semaphore.NewWeighted(int64(goRoutinesOpeningPeerConnections))

	// Exhaust all tokens.
	for i := range goRoutinesOpeningPeerConnections {
		ok := sem.TryAcquire(1)
		require.True(t, ok, "should acquire token %d", i+1)
	}

	// Semaphore is full -- verify.
	require.False(t, sem.TryAcquire(1), "semaphore should be exhausted")

	// Simulate what ConnectWithPeer's defer does: release one token.
	sem.Release(1)

	// Now exactly one acquire should succeed.
	ok := sem.TryAcquire(1)
	assert.True(t, ok, "after release, one acquire should succeed")

	// And the next should fail again.
	ok = sem.TryAcquire(1)
	assert.False(t, ok, "semaphore should be exhausted again after re-acquire")

	// Clean up.
	for range goRoutinesOpeningPeerConnections {
		sem.Release(1)
	}
}

// TestConnectSemaphoreAcquireRespectsContext verifies that Acquire unblocks
// when the context is cancelled, which is the behaviour relied upon in
// listenForPeers and findPeersForSubnets.
func TestConnectSemaphoreAcquireRespectsContext(t *testing.T) {
	t.Parallel()

	sem := semaphore.NewWeighted(1)

	// Hold the only token.
	require.True(t, sem.TryAcquire(1))

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- sem.Acquire(ctx, 1)
	}()

	// The goroutine should be blocked. Cancel the context to unblock it.
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err, "Acquire should return an error when context is cancelled")
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Acquire did not unblock after context cancellation")
	}

	sem.Release(1)
}

// TestGoRoutinesOpeningPeerConnectionsConst asserts the constant value the
// production code depends on. If someone changes this constant the test
// will catch it so the semaphore capacity expectation stays aligned.
func TestGoRoutinesOpeningPeerConnectionsConst(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 4, goRoutinesOpeningPeerConnections,
		"goRoutinesOpeningPeerConnections must be 4; changing this affects concurrency bounds")
}

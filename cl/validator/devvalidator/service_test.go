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

package devvalidator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
)

const syncingBody = `{"code":503,"message":"beacon node is syncing"}`

// startFixture serves the genesis endpoint and the head validators handler.
// The returned channel closes on the first request to any other endpoint;
// Start sends none before the validator indices are resolved.
func startFixture(t *testing.T, validators func(http.ResponseWriter, *ValidatorKey)) (*Service, <-chan struct{}) {
	t.Helper()
	keys, err := LoadKeys("start", 1)
	require.NoError(t, err)

	slotLoopStarted := make(chan struct{})
	var once sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("GET /eth/v1/beacon/genesis", func(w http.ResponseWriter, _ *http.Request) {
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
			"genesis_time":            strconv.FormatInt(time.Now().Unix()-10, 10),
			"genesis_validators_root": common.Hash{}.Hex(),
		}}))
	})
	mux.HandleFunc("GET /eth/v1/beacon/states/head/validators", func(w http.ResponseWriter, _ *http.Request) {
		validators(w, keys[0])
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(slotLoopStarted) })
		http.NotFound(w, r)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return &Service{
		client:    NewBeaconClient(server.URL),
		keys:      keys,
		keysByPub: PubKeyToKey(keys),
		cfg:       &clparams.BeaconChainConfig{SecondsPerSlot: 1, SlotsPerEpoch: 8},
		logger:    log.New(),
	}, slotLoopStarted
}

func startService(ctx context.Context, service *Service) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		service.Start(ctx)
		close(done)
	}()
	return done
}

func waitStopped(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
}

func TestStartRetriesIndicesWhileBeaconNodeIsSyncing(t *testing.T) {
	var calls atomic.Int32
	service, slotLoopStarted := startFixture(t, func(w http.ResponseWriter, key *ValidatorKey) {
		if calls.Add(1) == 1 {
			http.Error(w, syncingBody, http.StatusServiceUnavailable)
			return
		}
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{
			"index":     "7",
			"validator": map[string]string{"pubkey": hexutil.Encode(key.PubKeyBytes[:])},
		}}}))
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := startService(ctx, service)
	select {
	case <-slotLoopStarted:
	case <-done:
		t.Fatal("Start returned before resolving the validator indices")
	case <-time.After(30 * time.Second):
		t.Fatal("slot loop not started after the beacon node stopped syncing")
	}
	cancel()
	waitStopped(t, done)

	require.GreaterOrEqual(t, calls.Load(), int32(2))
	require.Equal(t, uint64(7), service.keys[0].ValidatorIndex)
}

func TestStartReturnsOnCancelWhileBeaconNodeIsSyncing(t *testing.T) {
	var calls atomic.Int32
	requested := make(chan struct{})
	service, _ := startFixture(t, func(w http.ResponseWriter, _ *ValidatorKey) {
		if calls.Add(1) == 1 {
			close(requested)
		}
		http.Error(w, syncingBody, http.StatusServiceUnavailable)
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := startService(ctx, service)
	select {
	case <-requested:
	case <-done:
		t.Fatal("Start returned before requesting the validators")
	case <-time.After(30 * time.Second):
		t.Fatal("validators not requested")
	}
	// Cancel inside the backoff, once the client has read the 503.
	select {
	case <-done:
		t.Fatal("Start returned while the beacon node was syncing")
	case <-time.After(headStateRetryInterval / 10):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(headStateRetryInterval / 2):
		t.Fatal("Start did not return during the retry backoff")
	}
}

func TestStartDoesNotRetryValidatorsServerError(t *testing.T) {
	var calls atomic.Int32
	retried := make(chan struct{})
	service, slotLoopStarted := startFixture(t, func(w http.ResponseWriter, _ *ValidatorKey) {
		if calls.Add(1) == 2 {
			close(retried)
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := startService(ctx, service)
	select {
	case <-done:
	case <-retried:
		t.Fatal("Start retried a non-503 error")
	case <-slotLoopStarted:
		t.Fatal("slot loop started without validator indices")
	case <-time.After(30 * time.Second):
		t.Fatal("Start did not return after a non-503 error")
	}

	require.Equal(t, int32(1), calls.Load())
}

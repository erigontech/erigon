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

package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

func witnessTestNotifier(t *testing.T) (ctx context.Context, resc, closec chan any) {
	t.Helper()
	resc = make(chan any, 8) // buffered: LocalNotifier.Notify blocks on resc<-
	closec = make(chan any)
	ctx = rpc.ContextWithNotifier(context.Background(), rpc.NewLocalNotifier("debug", resc, closec))
	return ctx, resc, closec
}

func TestWitnessSubscriptionEncodingValidation(t *testing.T) {
	require.NoError(t, validateWitnessEncoding(nil))
	require.NoError(t, validateWitnessEncoding(&WitnessSubscriptionOpts{}))
	require.NoError(t, validateWitnessEncoding(&WitnessSubscriptionOpts{Encoding: "json"}))

	for _, enc := range []string{"rlp", "garbage"} {
		err := validateWitnessEncoding(&WitnessSubscriptionOpts{Encoding: enc})
		require.Error(t, err, "encoding %q must be rejected", enc)
		require.Contains(t, err.Error(), "unsupported witness encoding")
	}
}

func TestWitnessSubscriptionRejectsEncodingThroughEndpoint(t *testing.T) {
	api := &DebugAPIImpl{witnessCache: newWitnessResultCache(4)}
	_, err := api.ExecutionWitnesses(context.Background(), &WitnessSubscriptionOpts{Encoding: "rlp"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported witness encoding")
}

func TestWitnessSubscriptionNilCache(t *testing.T) {
	api := &DebugAPIImpl{}
	_, err := api.ExecutionWitnesses(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--witness.cache.blocks")
	require.Contains(t, err.Error(), "debug_executionWitness")
}

func TestWitnessSubscriptionNoNotifier(t *testing.T) {
	api := &DebugAPIImpl{witnessCache: newWitnessResultCache(4)}
	sub, err := api.ExecutionWitnesses(context.Background(), nil)
	require.ErrorIs(t, err, rpc.ErrNotificationsUnsupported)
	require.NotNil(t, sub)
}

func TestWitnessNotificationWireKeys(t *testing.T) {
	b, err := json.Marshal(WitnessNotification{
		BlockNumber: hexutil.Uint64(7),
		BlockHash:   hashN(0x33),
		Witness:     json.RawMessage(`{"state":[]}`),
	})
	require.NoError(t, err)

	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &m))
	for _, k := range []string{"blockNumber", "blockHash", "witness"} {
		_, ok := m[k]
		require.Truef(t, ok, "wire payload must carry key %q for non-Go subscribers", k)
	}
}

func TestWitnessSubscriptionDelivers(t *testing.T) {
	cache := newWitnessResultCache(4)
	api := &DebugAPIImpl{witnessCache: cache}

	ctx, resc, closec := witnessTestNotifier(t)
	defer close(closec)

	sub, err := api.ExecutionWitnesses(ctx, &WitnessSubscriptionOpts{Encoding: "json"})
	require.NoError(t, err)
	require.NotNil(t, sub)

	hash := hashN(0x11)
	enc := json.RawMessage(`{"state":["0xabcd"],"codes":[],"keys":[],"headers":[]}`)
	api.storeWitness(9, hash, enc)

	select {
	case v := <-resc:
		n, ok := v.(WitnessNotification)
		require.Truef(t, ok, "notification payload must be WitnessNotification, got %T", v)
		require.Equal(t, hexutil.Uint64(9), n.BlockNumber)
		require.Equal(t, hash, n.BlockHash)
		require.True(t, bytes.Equal(enc, n.Witness), "witness bytes must be delivered verbatim")
	case <-time.After(2 * time.Second):
		t.Fatal("subscription did not deliver the published witness")
	}
}

func TestWitnessSubscriptionExitsOnNotifierClosed(t *testing.T) {
	cache := newWitnessResultCache(4)
	api := &DebugAPIImpl{witnessCache: cache}

	ctx, _, closec := witnessTestNotifier(t)

	_, err := api.ExecutionWitnesses(ctx, nil)
	require.NoError(t, err)

	subCount := func() int {
		cache.feed.mu.Lock()
		defer cache.feed.mu.Unlock()
		return len(cache.feed.subs)
	}
	require.Equal(t, 1, subCount(), "subscribe must register the pump before returning")

	close(closec)

	require.Eventually(t, func() bool {
		return subCount() == 0
	}, 2*time.Second, 10*time.Millisecond, "pump must unsubscribe when the connection closes")
}

// TestWitnessSubscriptionWireDispatch drives the generic subscription machinery over an
// in-process server: debug_subscribe("executionWitnesses") must reach the real method and
// deliver a witness the builder store publishes.
func TestWitnessSubscriptionWireDispatch(t *testing.T) {
	cache := newWitnessResultCache(4)
	api := &DebugAPIImpl{witnessCache: cache}

	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("debug", api))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(client.Close)

	ch := make(chan WitnessNotification, 4)
	sub, err := client.Subscribe(context.Background(), "debug", ch, "executionWitnesses", &WitnessSubscriptionOpts{Encoding: "json"})
	require.NoError(t, err)
	t.Cleanup(sub.Unsubscribe)

	hash := hashN(0x22)
	enc := json.RawMessage(`{"state":["0x1234"],"codes":[],"keys":[],"headers":[]}`)
	api.storeWitness(42, hash, enc)

	select {
	case n := <-ch:
		require.Equal(t, hexutil.Uint64(42), n.BlockNumber)
		require.Equal(t, hash, n.BlockHash)
		require.True(t, bytes.Equal(enc, n.Witness), "witness bytes must survive the wire verbatim")
	case err := <-sub.Err():
		t.Fatalf("subscription errored: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("wire subscription did not deliver the published witness")
	}
}

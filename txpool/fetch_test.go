/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetch(t *testing.T) {
	logger := log.NewTest(t)

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	var genesisHash [32]byte
	var networkId uint64 = 1
	forks := []uint64{1, 5, 10}

	m := NewMockSentry(ctx)
	sentryClient := direct.NewSentryClientDirect(direct.ETH66, m)
	pool := &PoolMock{}

	fetch := NewFetch(ctx, []sentry.SentryClient{sentryClient}, genesisHash, networkId, forks, pool, logger)
	var wg sync.WaitGroup
	fetch.SetWaitGroup(&wg)
	m.StreamWg.Add(2)
	fetch.Start()
	m.StreamWg.Wait()
	// Send one transaction id
	wg.Add(1)
	data, _ := hex.DecodeString("e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328")
	errs := m.Send(&sentry.InboundMessage{
		Id:     sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		Data:   data,
		PeerId: PeerId,
	})
	for i, err := range errs {
		if err != nil {
			t.Errorf("sending new pool tx hashes 66 (%d): %v", i, err)
		}
	}
	wg.Wait()

}

func TestSendTxPropagate(t *testing.T) {
	logger := log.NewTest(t)
	//logger := log.New()

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	t.Run("few remote txs", func(t *testing.T) {
		m := NewMockSentry(ctx)
		send := NewSend(ctx, []sentry.SentryClient{direct.NewSentryClientDirect(direct.ETH66, m)}, nil, logger)
		send.BroadcastRemotePooledTxs(toHashes([32]byte{1}, [32]byte{42}))

		calls := m.SendMessageToRandomPeersCalls()
		assert.Equal(t, 1, len(calls))
		first := calls[0].SendMessageToRandomPeersRequest.Data
		require.Equal(t, sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, first.Id)
		require.Equal(t, 68, len(first.Data))
	})
	t.Run("much remote txs", func(t *testing.T) {
		m := NewMockSentry(ctx)
		send := NewSend(ctx, []sentry.SentryClient{direct.NewSentryClientDirect(direct.ETH66, m)}, nil, logger)
		list := make(Hashes, p2pTxPacketLimit*3)
		for i := 0; i < len(list); i += 32 {
			b := []byte(fmt.Sprintf("%x", i))
			copy(list[i:i+32], b)
		}
		send.BroadcastRemotePooledTxs(list)
		calls := m.SendMessageToRandomPeersCalls()
		require.Equal(t, 3, len(calls))
		for i := 0; i < 3; i++ {
			call := calls[i].SendMessageToRandomPeersRequest.Data
			require.Equal(t, sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, call.Id)
			require.Less(t, 0, len(call.Data))
		}
	})
	t.Run("few local txs", func(t *testing.T) {
		m := NewMockSentry(ctx)
		m.SendMessageToAllFunc = func(contextMoqParam context.Context, outboundMessageData *sentry.OutboundMessageData) (*sentry.SentPeers, error) {
			return &sentry.SentPeers{Peers: make([]*types.H512, 5)}, nil
		}
		send := NewSend(ctx, []sentry.SentryClient{direct.NewSentryClientDirect(direct.ETH66, m)}, nil, logger)
		send.BroadcastLocalPooledTxs(toHashes([32]byte{1}, [32]byte{42}))

		calls := m.SendMessageToAllCalls()
		assert.Equal(t, 1, len(calls))
		first := calls[0].OutboundMessageData
		require.Equal(t, sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, first.Id)
		require.Equal(t, 68, len(first.Data))
		t.Fail()
	})

}

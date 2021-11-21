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
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := NewMockSentry(ctx)
	sentryClient := direct.NewSentryClientDirect(direct.ETH66, m)
	pool := &PoolMock{}

	fetch := NewFetch(ctx, []direct.SentryClient{sentryClient}, pool, &remote.KVClientMock{}, nil, nil, *u256.N1)
	var wg sync.WaitGroup
	fetch.SetWaitGroup(&wg)
	m.StreamWg.Add(2)
	fetch.ConnectSentries()
	m.StreamWg.Wait()
	// Send one transaction id
	wg.Add(1)
	errs := m.Send(&sentry.InboundMessage{
		Id:     sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		Data:   decodeHex("e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328"),
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
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	t.Run("few remote byHash", func(t *testing.T) {
		m := NewMockSentry(ctx)
		send := NewSend(ctx, []direct.SentryClient{direct.NewSentryClientDirect(direct.ETH66, m)}, nil)
		send.BroadcastRemotePooledTxs(toHashes(1, 42))

		calls := m.SendMessageToRandomPeersCalls()
		require.Equal(t, 1, len(calls))
		first := calls[0].SendMessageToRandomPeersRequest.Data
		assert.Equal(t, sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, first.Id)
		assert.Equal(t, 68, len(first.Data))
	})
	t.Run("much remote byHash", func(t *testing.T) {
		m := NewMockSentry(ctx)
		send := NewSend(ctx, []direct.SentryClient{direct.NewSentryClientDirect(direct.ETH66, m)}, nil)
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
			require.True(t, len(call.Data) > 0)
		}
	})
	t.Run("few local byHash", func(t *testing.T) {
		m := NewMockSentry(ctx)
		m.SendMessageToAllFunc = func(contextMoqParam context.Context, outboundMessageData *sentry.OutboundMessageData) (*sentry.SentPeers, error) {
			return &sentry.SentPeers{Peers: make([]*types.H256, 5)}, nil
		}
		send := NewSend(ctx, []direct.SentryClient{direct.NewSentryClientDirect(direct.ETH66, m)}, nil)
		send.BroadcastLocalPooledTxs(toHashes(1, 42))

		calls := m.SendMessageToAllCalls()
		require.Equal(t, 1, len(calls))
		first := calls[0].OutboundMessageData
		assert.Equal(t, sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, first.Id)
		assert.Equal(t, 68, len(first.Data))
	})
	t.Run("sync with new peer", func(t *testing.T) {
		m := NewMockSentry(ctx)

		m.SendMessageToAllFunc = func(contextMoqParam context.Context, outboundMessageData *sentry.OutboundMessageData) (*sentry.SentPeers, error) {
			return &sentry.SentPeers{Peers: make([]*types.H256, 5)}, nil
		}
		send := NewSend(ctx, []direct.SentryClient{direct.NewSentryClientDirect(direct.ETH66, m)}, nil)
		expectPeers := toPeerIDs(1, 2, 42)
		send.PropagatePooledTxsToPeersList(expectPeers, toHashes(1, 42))

		calls := m.SendMessageByIdCalls()
		require.Equal(t, 3, len(calls))
		for i, call := range calls {
			req := call.SendMessageByIdRequest
			assert.Equal(t, expectPeers[i], PeerID(req.PeerId))
			assert.Equal(t, sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, req.Data.Id)
			assert.True(t, len(req.Data.Data) > 0)
		}
	})
}

func TestOnNewBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coreDB, db := memdb.NewTestDB(t), memdb.NewTestDB(t)

	i := 0
	stream := &remote.KV_StateChangesClientMock{
		RecvFunc: func() (*remote.StateChangeBatch, error) {
			if i > 0 {
				return nil, io.EOF
			}
			i++
			return &remote.StateChangeBatch{
				DatabaseViewID: 1,
				ChangeBatch: []*remote.StateChange{
					{Txs: [][]byte{decodeHex(txParseMainnetTests[0].payloadStr), decodeHex(txParseMainnetTests[1].payloadStr), decodeHex(txParseMainnetTests[2].payloadStr)}, BlockHeight: 1, BlockHash: gointerfaces.ConvertHashToH256([32]byte{})},
				},
			}, nil
		},
	}
	stateChanges := &remote.KVClientMock{
		StateChangesFunc: func(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error) {
			return stream, nil
		},
	}
	pool := &PoolMock{}
	fetch := NewFetch(ctx, nil, pool, stateChanges, coreDB, db, *u256.N1)
	err := fetch.handleStateChanges(ctx, stateChanges)
	assert.ErrorIs(t, io.EOF, err)
	assert.Equal(t, 1, len(pool.OnNewBlockCalls()))
	assert.Equal(t, 3, len(pool.OnNewBlockCalls()[0].MinedTxs.txs))
}

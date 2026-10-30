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

package txpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
)

type boundedBatchStream struct {
	grpc.ClientStream
	recv func() (*sentryproto.InboundMessage, error)
}

func (s *boundedBatchStream) Recv() (*sentryproto.InboundMessage, error) { return s.recv() }

func TestFetchBoundsQueuedBatch(t *testing.T) {
	for _, tc := range []struct {
		name            string
		bytes, messages int
		wantFlushes     int32
	}{
		{"bytes", 1024 * 1024, 5, 1},
		{"messages", 1, 257, 1},
		{"bytes_before_append", 3 * 1024 * 1024, 3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				pool := NewMockPool(ctrl)
				var flushes atomic.Int32
				pool.EXPECT().Started().DoAndReturn(func() bool { flushes.Add(1); return true }).AnyTimes()
				client := sentryproto.NewMockSentryClient(ctrl)
				var wg sync.WaitGroup
				fetch := NewFetch(t.Context(), nil, pool, nil, nil, u256.N1, log.New(), WithP2PFetcherWg(&wg))
				received := 0
				stream := &boundedBatchStream{recv: func() (*sentryproto.InboundMessage, error) {
					if received == tc.messages {
						require.Equal(t, tc.wantFlushes, flushes.Load(), "unexpected flush count before stream closure")
						return nil, nil
					}
					received++
					wg.Add(1)
					return &sentryproto.InboundMessage{Id: sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66, Data: make([]byte, tc.bytes)}, nil
				}}
				client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)
				require.NoError(t, fetch.receiveMessage(t.Context(), client))
				wg.Wait()
				require.Equal(t, tc.messages, received)
			})
		})
	}
}

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

package p2p

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	erigonlibtypes "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/testlog"
)

func TestFetcherFetchHeaders(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	mockInboundMessages := []*sentryproto.InboundMessage{
		{
			// should get filtered because it is from a different peer id
			PeerId: PeerIdFromUint64(2).H512(),
		},
		{
			// should get filtered because it is from a different request id
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId*2, 2),
		},
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId, 2),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           2,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 3, peerId)
		headersData := headers.Data
		require.NoError(t, err)
		require.Len(t, headersData, 2)
		require.Equal(t, uint64(1), headersData[0].Number.Uint64())
		require.Equal(t, uint64(2), headersData[1].Number.Uint64())
	})
}

func TestFetcherFetchHeadersWithChunking(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeaders := newMockBlockHeaders(1999)
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// 1024 headers in first response
			Data: blockHeadersPacket66Bytes(t, requestId1, mockHeaders[:1024]),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           1024,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// remaining 975 headers in second response
			Data: blockHeadersPacket66Bytes(t, requestId2, mockHeaders[1024:]),
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1025,
		wantRequestAmount:           975,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 2000, peerId)
		headersData := headers.Data
		require.NoError(t, err)
		require.Len(t, headersData, 1999)
		require.Equal(t, uint64(1), headersData[0].Number.Uint64())
		require.Equal(t, uint64(1999), headersData[len(headersData)-1].Number.Uint64())
	})
}

func TestFetcherFetchHeadersResponseTimeout(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// requestId2 takes too long and causes response timeout
			Data: nil,
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           10,
		// cause response timeout
		responseDelay: 600 * time.Millisecond,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// requestId2 takes too long and causes response timeout
			Data: nil,
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           10,
		// cause response timeout
		responseDelay: 600 * time.Millisecond,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 11, peerId)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, headers.Data)
	})
}

func TestFetcherFetchHeadersResponseTimeoutRetrySuccess(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeaders := newMockBlockHeaders(1999)
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// 1024 headers in first response
			Data: blockHeadersPacket66Bytes(t, requestId1, mockHeaders[:1024]),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           1024,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// requestId2 takes too long and causes response timeout
			Data: nil,
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1025,
		wantRequestAmount:           975,
		// cause response timeout
		responseDelay: 600 * time.Millisecond,
	}
	requestId3 := uint64(1236)
	mockInboundMessages3 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// remaining 975 headers in third response
			Data: blockHeadersPacket66Bytes(t, requestId3, mockHeaders[1024:]),
		},
	}
	mockRequestResponse3 := requestResponseMock{
		requestId:                   requestId3,
		mockResponseInboundMessages: mockInboundMessages3,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1025,
		wantRequestAmount:           975,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2, requestId3))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2, mockRequestResponse3)
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 2000, peerId)
		headersData := headers.Data
		require.NoError(t, err)
		require.Len(t, headersData, 1999)
		require.Equal(t, uint64(1), headersData[0].Number.Uint64())
		require.Equal(t, uint64(1999), headersData[len(headersData)-1].Number.Uint64())
	})
}

func TestFetcherErrInvalidFetchHeadersRange(t *testing.T) {
	t.Parallel()

	test := newFetcherTest(t, newMockRequestGenerator(1))
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 3, 1, PeerIdFromUint64(1))
		var errInvalidFetchHeadersRange *ErrInvalidFetchHeadersRange
		require.ErrorAs(t, err, &errInvalidFetchHeadersRange)
		require.Equal(t, uint64(3), errInvalidFetchHeadersRange.start)
		require.Equal(t, uint64(1), errInvalidFetchHeadersRange.end)
		require.Nil(t, headers.Data)
	})
}

func TestFetcherFetchHeadersErrIncompleteResponse(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1234)
	requestId2 := uint64(1235)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId1, 2),
		},
	}
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId2, 0),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           3,
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     3,
		wantRequestAmount:           1,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		var errIncompleteHeaders *ErrIncompleteHeaders
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 4, peerId)
		require.ErrorAs(t, err, &errIncompleteHeaders)
		require.Equal(t, uint64(3), errIncompleteHeaders.LowestMissingBlockNum())
		require.Nil(t, headers.Data)
	})
}

func TestFetcherFetchBodies(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	// setup 2 request to test "paging"-style fetch logic
	requestId1 := uint64(1234)
	requestId2 := uint64(1235)
	mockHeaders := []*types.Header{
		{Number: big.NewInt(1)},
		{Number: big.NewInt(2)},
	}
	mockHashes := []common.Hash{
		mockHeaders[0].Hash(),
		mockHeaders[1].Hash(),
	}
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data: newMockBlockBodiesPacketBytes(t, requestId1, &types.Body{
				Transactions: types.Transactions{
					types.NewEIP1559Transaction(
						*uint256.NewInt(1),
						1,
						common.BigToAddress(big.NewInt(123)),
						uint256.NewInt(55),
						0,
						uint256.NewInt(666),
						uint256.NewInt(777),
						uint256.NewInt(888),
						nil,
					),
				},
			}),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           mockHashes,
	}
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data: newMockBlockBodiesPacketBytes(t, requestId2, &types.Body{
				Transactions: types.Transactions{
					types.NewEIP1559Transaction(
						*uint256.NewInt(1),
						2,
						common.BigToAddress(big.NewInt(321)),
						uint256.NewInt(21),
						0,
						uint256.NewInt(987),
						uint256.NewInt(876),
						uint256.NewInt(765),
						nil,
					),
				},
			}),
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		// 2nd time only request the remaining hash since the first one has been received
		// in first batch
		wantRequestHashes: mockHashes[1:],
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		bodies, err := test.fetcher.FetchBodies(ctx, mockHeaders, peerId)
		require.NoError(t, err)
		require.Len(t, bodies.Data, 2)
	})
}

func TestFetcherFetchBodiesResponseTimeout(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1234)
	requestId2 := uint64(1235)
	mockHeaders := []*types.Header{{Number: big.NewInt(1)}}
	mockHashes := []common.Hash{mockHeaders[0].Hash()}
	mockInboundMessages := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data:   nil, // response timeout
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		responseDelay:               600 * time.Millisecond,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           mockHashes,
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		responseDelay:               600 * time.Millisecond,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           mockHashes,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		bodies, err := test.fetcher.FetchBodies(ctx, mockHeaders, peerId)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, bodies.Data)
	})
}

func TestFetcherFetchBodiesResponseTimeoutRetrySuccess(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1234)
	requestId2 := uint64(1235)
	mockHeaders := []*types.Header{{Number: big.NewInt(1)}}
	mockHashes := []common.Hash{mockHeaders[0].Hash()}
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data:   nil, // response timeout
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		responseDelay:               600 * time.Millisecond,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           mockHashes,
	}
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data: newMockBlockBodiesPacketBytes(t, requestId2, &types.Body{
				Transactions: types.Transactions{
					types.NewEIP1559Transaction(
						*uint256.NewInt(1),
						1,
						common.BigToAddress(big.NewInt(123)),
						uint256.NewInt(55),
						0,
						uint256.NewInt(666),
						uint256.NewInt(777),
						uint256.NewInt(888),
						nil,
					),
				},
			}),
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           mockHashes,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		bodies, err := test.fetcher.FetchBodies(ctx, mockHeaders, peerId)
		require.NoError(t, err)
		require.Len(t, bodies.Data, 1)
	})
}

func TestFetcherFetchBodiesErrMissingBodies(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	mockHeaders := []*types.Header{{Number: big.NewInt(1)}}
	mockHashes := []common.Hash{mockHeaders[0].Hash()}
	mockInboundMessages := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockBodiesPacketBytes(t, requestId),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           mockHashes,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	test.run(func(ctx context.Context, t *testing.T) {
		var errMissingBlocks *ErrMissingBodies
		bodies, err := test.fetcher.FetchBodies(ctx, mockHeaders, peerId)
		require.ErrorAs(t, err, &errMissingBlocks)
		lowest, exists := errMissingBlocks.LowestMissingBlockNum()
		require.Equal(t, uint64(1), lowest)
		require.True(t, exists)
		require.Nil(t, bodies.Data)
	})
}

func TestFetcherFetchBlockByHash(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeader := newMockBlockHeaders(1)[0]
	mockHash := mockHeader.Hash()
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, []*types.Header{mockHeader}),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       mockHash,
		wantRequestAmount:           1,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockBodiesPacketBytes(t, requestId2, &types.Body{}),
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           []common.Hash{mockHash},
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		response, err := test.fetcher.FetchBlockByHash(ctx, mockHash, peerId)
		require.NoError(t, err)
		require.Equal(t, mockHash, response.Data.Header().Hash())
	})
}

func TestFetcherFetchBlockByHashErrMissingHeaderHash(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeader := newMockBlockHeaders(1)[0]
	mockHash := mockHeader.Hash()
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, []*types.Header{}),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       mockHash,
		wantRequestAmount:           1,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1))
	test.mockSentryStreams(mockRequestResponse1)
	test.run(func(ctx context.Context, t *testing.T) {
		response, err := test.fetcher.FetchBlockByHash(ctx, mockHash, peerId)
		require.ErrorIs(t, err, &ErrMissingHeaderHash{})
		require.Nil(t, response.Data, response)
	})
}

func TestFetcherFetchBlockByHashErrTooManyHeaders(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeaders := newMockBlockHeaders(3)
	mockHeader := mockHeaders[0]
	mockHash := mockHeader.Hash()
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, mockHeaders),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       mockHash,
		wantRequestAmount:           1,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1))
	test.mockSentryStreams(mockRequestResponse1)
	test.run(func(ctx context.Context, t *testing.T) {
		response, err := test.fetcher.FetchBlockByHash(ctx, mockHash, peerId)
		require.ErrorIs(t, err, &ErrTooManyHeaders{})
		require.Nil(t, response.Data, response)
	})
}

func TestFetcherFetchBlockByHashErrUnexpectedHeaderHash(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeaders := newMockBlockHeaders(2)
	mockHeader := mockHeaders[0]
	mockHash := mockHeader.Hash()
	incorrectHeaderResponse := mockHeaders[1]
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, []*types.Header{incorrectHeaderResponse}),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       mockHash,
		wantRequestAmount:           1,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1))
	test.mockSentryStreams(mockRequestResponse1)
	test.run(func(ctx context.Context, t *testing.T) {
		response, err := test.fetcher.FetchBlockByHash(ctx, mockHash, peerId)
		require.ErrorIs(t, err, &ErrUnexpectedHeaderHash{})
		require.Nil(t, response.Data, response)
	})
}

func TestFetcherFetchBlockByHashErrTooManyBodies(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeader := newMockBlockHeaders(1)[0]
	mockHash := mockHeader.Hash()
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, []*types.Header{mockHeader}),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       mockHash,
		wantRequestAmount:           1,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockBodiesPacketBytes(t, requestId2, &types.Body{}, &types.Body{}),
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           []common.Hash{mockHash},
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		response, err := test.fetcher.FetchBlockByHash(ctx, mockHash, peerId)
		require.ErrorIs(t, err, &ErrTooManyBodies{})
		require.Nil(t, response.Data)
	})
}

func TestFetcherFetchBlockByHashErrMissingBodies(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeader := newMockBlockHeaders(1)[0]
	mockHash := mockHeader.Hash()
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, []*types.Header{mockHeader}),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       mockHash,
		wantRequestAmount:           1,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockBodiesPacketBytes(t, requestId2),
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           []common.Hash{mockHash},
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		response, err := test.fetcher.FetchBlockByHash(ctx, mockHash, peerId)
		require.ErrorIs(t, err, &ErrMissingBodies{})
		require.Nil(t, response.Data)
	})
}

func newFetcherTest(t *testing.T, requestIdGenerator RequestIdGenerator) *fetcherTest {
	ctx, cancel := context.WithCancel(context.Background())
	fetcherConfig := FetcherConfig{
		responseTimeout: 200 * time.Millisecond,
		retryBackOff:    time.Second,
		maxRetries:      1,
	}
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	statusDataFactory := sentry.StatusDataFactory(func(ctx context.Context) (*sentryproto.StatusData, error) {
		return &sentryproto.StatusData{}, nil
	})
	peerPenalizer := NewPeerPenalizer(sentryClient)
	messageListener := NewMessageListener(logger, sentryClient, statusDataFactory, peerPenalizer)
	messageSender := NewMessageSender(sentryClient)
	fetcher := newFetcher(logger, fetcherConfig, messageListener, messageSender, requestIdGenerator)
	return &fetcherTest{
		ctx:                  ctx,
		ctxCancel:            cancel,
		t:                    t,
		fetcher:              fetcher,
		logger:               logger,
		sentryClient:         sentryClient,
		messageListener:      messageListener,
		requestResponseMocks: map[uint64]requestResponseMock{},
	}
}

type fetcherTest struct {
	ctx                  context.Context
	ctxCancel            context.CancelFunc
	t                    *testing.T
	fetcher              *fetcher
	logger               log.Logger
	sentryClient         *direct.MockSentryClient
	messageListener      MessageListener
	requestResponseMocks map[uint64]requestResponseMock
	peerEvents           chan *delayedMessage[*sentryproto.PeerEvent]
}

func (ft *fetcherTest) run(f func(ctx context.Context, t *testing.T)) {
	var done atomic.Bool
	ft.t.Run("start", func(t *testing.T) {
		go func() {
			defer done.Store(true)
			err := ft.messageListener.Run(ft.ctx)
			require.ErrorIs(t, err, context.Canceled)
		}()
	})

	ft.t.Run("test", func(t *testing.T) {
		f(ft.ctx, t)
	})

	ft.t.Run("stop", func(t *testing.T) {
		ft.ctxCancel()
		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func (ft *fetcherTest) mockSentryStreams(mocks ...requestResponseMock) {
	// default mocks
	ft.sentryClient.
		EXPECT().
		HandShake(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	ft.sentryClient.
		EXPECT().
		SetStatus(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	ft.sentryClient.
		EXPECT().
		MarkDisconnected().
		AnyTimes()

	ft.mockSentryInboundMessagesStream(mocks...)
	ft.mockSentryPeerEventsStream()
}

func (ft *fetcherTest) mockSentryInboundMessagesStream(mocks ...requestResponseMock) {
	var numInboundMessages int
	for _, mock := range mocks {
		numInboundMessages += len(mock.mockResponseInboundMessages)
		ft.requestResponseMocks[mock.requestId] = mock
	}

	inboundMessageStreamChan := make(chan *delayedMessage[*sentryproto.InboundMessage], numInboundMessages)
	mockSentryInboundMessagesStream := &mockSentryMessagesStream[*sentryproto.InboundMessage]{
		ctx:    ft.ctx,
		stream: inboundMessageStreamChan,
	}

	ft.sentryClient.
		EXPECT().
		Messages(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockSentryInboundMessagesStream, nil).
		AnyTimes()
	ft.sentryClient.
		EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			var mock requestResponseMock
			var err error
			switch req.Data.Id {
			case sentryproto.MessageId_GET_BLOCK_HEADERS_66:
				mock, err = ft.mockSendMessageByIdForHeaders(req)
			case sentryproto.MessageId_GET_BLOCK_BODIES_66:
				mock, err = ft.mockSendMessageByIdForBodies(req)
			default:
				return nil, fmt.Errorf("unexpected message id request sent %d", req.Data.Id)
			}
			if err != nil {
				return nil, err
			}

			delete(ft.requestResponseMocks, mock.requestId)
			for _, inboundMessage := range mock.mockResponseInboundMessages {
				inboundMessageStreamChan <- &delayedMessage[*sentryproto.InboundMessage]{
					message:       inboundMessage,
					responseDelay: mock.responseDelay,
				}
			}

			return &sentryproto.SentPeers{
				Peers: []*erigonlibtypes.H512{req.PeerId},
			}, nil
		}).
		AnyTimes()
}

func (ft *fetcherTest) mockSendMessageByIdForHeaders(req *sentryproto.SendMessageByIdRequest) (requestResponseMock, error) {
	if sentryproto.MessageId_GET_BLOCK_HEADERS_66 != req.Data.Id {
		return requestResponseMock{}, fmt.Errorf("MessageId_GET_BLOCK_HEADERS_66 != req.Data.Id - %v", req.Data.Id)
	}

	var pkt eth.GetBlockHeadersPacket66
	if err := rlp.DecodeBytes(req.Data.Data, &pkt); err != nil {
		return requestResponseMock{}, err
	}

	mock, ok := ft.requestResponseMocks[pkt.RequestId]
	if !ok {
		return requestResponseMock{}, fmt.Errorf("unexpected request id %d", pkt.RequestId)
	}

	reqPeerId := PeerIdFromH512(req.PeerId)
	if !mock.wantRequestPeerId.Equal(reqPeerId) {
		return requestResponseMock{}, fmt.Errorf("wantRequestPeerId != reqPeerId - %v vs %v", mock.wantRequestPeerId, reqPeerId)
	}

	if mock.wantRequestOriginNumber != pkt.Origin.Number {
		return requestResponseMock{}, fmt.Errorf("wantRequestOriginNumber != pkt.Origin.Number - %v vs %v", mock.wantRequestOriginNumber, pkt.Origin.Number)
	}

	if mock.wantRequestOriginHash != pkt.Origin.Hash {
		return requestResponseMock{}, fmt.Errorf("wantRequestOriginHash != pkt.Origin.Hash - %v vs %v", mock.wantRequestOriginHash, pkt.Origin.Hash)
	}

	if mock.wantRequestAmount != pkt.Amount {
		return requestResponseMock{}, fmt.Errorf("wantRequestAmount != pkt.Amount - %v vs %v", mock.wantRequestAmount, pkt.Amount)
	}

	return mock, nil
}

func (ft *fetcherTest) mockSendMessageByIdForBodies(req *sentryproto.SendMessageByIdRequest) (requestResponseMock, error) {
	if sentryproto.MessageId_GET_BLOCK_BODIES_66 != req.Data.Id {
		return requestResponseMock{}, fmt.Errorf("MessageId_GET_BLOCK_BODIES_66 != req.Data.Id - %v", req.Data.Id)
	}

	var pkt eth.GetBlockBodiesPacket66
	if err := rlp.DecodeBytes(req.Data.Data, &pkt); err != nil {
		return requestResponseMock{}, err
	}

	mock, ok := ft.requestResponseMocks[pkt.RequestId]
	if !ok {
		return requestResponseMock{}, fmt.Errorf("unexpected request id %d", pkt.RequestId)
	}

	reqPeerId := PeerIdFromH512(req.PeerId)
	if !mock.wantRequestPeerId.Equal(reqPeerId) {
		return requestResponseMock{}, fmt.Errorf("wantRequestPeerId != reqPeerId - %v vs %v", mock.wantRequestPeerId, reqPeerId)
	}

	if len(mock.wantRequestHashes) != len(pkt.GetBlockBodiesPacket) {
		return requestResponseMock{}, fmt.Errorf("len(wantRequestHashes) != len(pkt.GetBlockBodiesPacket) - %v vs %v", len(mock.wantRequestHashes), len(pkt.GetBlockBodiesPacket))
	}

	for i, packet := range pkt.GetBlockBodiesPacket {
		if mock.wantRequestHashes[i].String() != packet.String() {
			return requestResponseMock{}, fmt.Errorf("wantRequestHash != packet - %s vs %s", mock.wantRequestHashes[i], packet)
		}
	}

	return mock, nil
}

func (ft *fetcherTest) mockSentryPeerEventsStream() {
	ft.peerEvents = make(chan *delayedMessage[*sentryproto.PeerEvent])
	ft.sentryClient.
		EXPECT().
		PeerEvents(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockSentryMessagesStream[*sentryproto.PeerEvent]{
			ctx:    ft.ctx,
			stream: ft.peerEvents,
		}, nil).
		AnyTimes()
}

type requestResponseMock struct {
	requestId                   uint64
	responseDelay               time.Duration
	mockResponseInboundMessages []*sentryproto.InboundMessage

	// Common
	wantRequestPeerId *PeerId

	// FetchHeaders only
	wantRequestOriginNumber uint64
	wantRequestOriginHash   common.Hash
	wantRequestAmount       uint64

	// FetchBodies only
	wantRequestHashes []common.Hash
}

func newMockRequestGenerator(requestIds ...uint64) RequestIdGenerator {
	var idx int
	idxPtr := &idx
	return func() uint64 {
		if *idxPtr >= len(requestIds) {
			panic("mock request generator does not have any request ids left")
		}

		res := requestIds[*idxPtr]
		*idxPtr++
		return res
	}
}

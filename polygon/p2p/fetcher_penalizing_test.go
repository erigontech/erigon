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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/core/types"
)

func TestPenalizingFetcherFetchHeadersShouldPenalizePeerWhenErrTooManyHeaders(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	mockInboundMessages := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// response should contain 2 headers instead we return 5
			Data: newMockBlockHeadersPacket66Bytes(t, requestId, 5),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           2,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errTooManyHeaders *ErrTooManyHeaders
		headers, err := test.penalizingFetcher.FetchHeaders(ctx, 1, 3, peerId)
		require.ErrorAs(t, err, &errTooManyHeaders)
		require.Equal(t, 2, errTooManyHeaders.requested)
		require.Equal(t, 5, errTooManyHeaders.received)
		require.Nil(t, headers.Data)
	})
}

func TestPenalizingFetcherFetchHeadersShouldPenalizePeerWhenErrNonSequentialHeaderNumbers(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	mockBlockHeaders := newMockBlockHeaders(5)
	disconnectedHeaders := make([]*types.Header, 3)
	disconnectedHeaders[0] = mockBlockHeaders[0]
	disconnectedHeaders[1] = mockBlockHeaders[2]
	disconnectedHeaders[2] = mockBlockHeaders[4]
	mockInboundMessages := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId, disconnectedHeaders),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           3,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errNonSequentialHeaderNumbers *ErrNonSequentialHeaderNumbers
		headers, err := test.penalizingFetcher.FetchHeaders(ctx, 1, 4, peerId)
		require.ErrorAs(t, err, &errNonSequentialHeaderNumbers)
		require.Equal(t, uint64(3), errNonSequentialHeaderNumbers.current)
		require.Equal(t, uint64(2), errNonSequentialHeaderNumbers.expected)
		require.Nil(t, headers.Data)
	})
}

func TestPenalizingFetcherFetchHeadersShouldPenalizePeerWhenErrNonSequentialHeaderHashes(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	disconnectedHeaders := newMockBlockHeaders(2)
	disconnectedHeaders[0] = &types.Header{
		Number:   big.NewInt(1),
		GasLimit: 1234, // change a random value in order to change the header Hash
	}
	mockInboundMessages := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId, disconnectedHeaders),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           2,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errNonSequentialHeaderHashes *ErrNonSequentialHeaderHashes
		headers, err := test.penalizingFetcher.FetchHeaders(ctx, 1, 3, peerId)
		require.ErrorAs(t, err, &errNonSequentialHeaderHashes)
		require.Equal(t, disconnectedHeaders[1].Hash(), errNonSequentialHeaderHashes.hash)
		require.Equal(t, disconnectedHeaders[1].ParentHash, errNonSequentialHeaderHashes.parentHash)
		require.Equal(t, disconnectedHeaders[0].Hash(), errNonSequentialHeaderHashes.prevHash)
		require.Nil(t, headers.Data)
	})
}

func TestPenalizingFetcherFetchHeadersShouldPenalizePeerWhenHeaderGtRequestedStart(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	mockBlockHeaders := newMockBlockHeaders(3)
	incorrectOriginHeaders := mockBlockHeaders[1:]
	mockInboundMessages := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// response headers should be 2 and start at 1 - instead we start at 2
			Data: blockHeadersPacket66Bytes(t, requestId, incorrectOriginHeaders),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           2,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errNonSequentialHeaderNumbers *ErrNonSequentialHeaderNumbers
		headers, err := test.penalizingFetcher.FetchHeaders(ctx, 1, 3, peerId)
		require.ErrorAs(t, err, &errNonSequentialHeaderNumbers)
		require.Equal(t, uint64(2), errNonSequentialHeaderNumbers.current)
		require.Equal(t, uint64(1), errNonSequentialHeaderNumbers.expected)
		require.Nil(t, headers.Data)
	})
}

func TestPenalizingFetcherFetchBodiesShouldPenalizePeerWhenErrTooManyBodies(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	headers := []*types.Header{{Number: big.NewInt(1)}}
	hashes := []common.Hash{headers[0].Hash()}
	mockInboundMessages := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_BODIES_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockBodiesPacketBytes(t, requestId, &types.Body{}, &types.Body{}),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestHashes:           hashes,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errTooManyBodies *ErrTooManyBodies
		bodies, err := test.penalizingFetcher.FetchBodies(ctx, headers, peerId)
		require.ErrorAs(t, err, &errTooManyBodies)
		require.Equal(t, 1, errTooManyBodies.requested)
		require.Equal(t, 2, errTooManyBodies.received)
		require.Nil(t, bodies.Data)
	})
}

func TestPenalizingFetcherFetchBodiesShouldPenalizePeerWhenErrMissingBodies(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	headers := []*types.Header{{Number: big.NewInt(1)}}
	hashes := []common.Hash{headers[0].Hash()}
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
		wantRequestHashes:           hashes,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errMissingBodies *ErrMissingBodies
		bodies, err := test.penalizingFetcher.FetchBodies(ctx, headers, peerId)
		require.ErrorAs(t, err, &errMissingBodies)
		lowest, ok := errMissingBodies.LowestMissingBlockNum()
		require.True(t, ok)
		require.Equal(t, uint64(1), lowest)
		require.Nil(t, bodies.Data)
	})
}

func TestPenalizingFetcherFetchBlocksBackwardsByHashShouldPenalizePeerWhenErrTooManyBodies(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1233)
	headers := newMockBlockHeaders(1)
	hash := headers[0].Hash()
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, headers),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       hash,
		wantRequestAmount:           1,
		wantReverse:                 true,
	}
	requestId2 := uint64(1234)
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
		wantRequestHashes:           []common.Hash{hash},
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errTooManyBodies *ErrTooManyBodies
		blocks, err := test.penalizingFetcher.FetchBlocksBackwardsByHash(ctx, hash, 1, peerId)
		require.ErrorAs(t, err, &errTooManyBodies)
		require.Equal(t, 1, errTooManyBodies.requested)
		require.Equal(t, 2, errTooManyBodies.received)
		require.Nil(t, blocks.Data)
	})
}

func TestPenalizingFetcherFetchBlocksBackwardsByHashShouldPenalizePeerWhenErrMissingBodies(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1233)
	headers := newMockBlockHeaders(1)
	hash := headers[0].Hash()
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, headers),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       hash,
		wantRequestAmount:           1,
		wantReverse:                 true,
	}
	requestId2 := uint64(1234)
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
		wantRequestHashes:           []common.Hash{hash},
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errMissingBodies *ErrMissingBodies
		blocks, err := test.penalizingFetcher.FetchBlocksBackwardsByHash(ctx, hash, 1, peerId)
		require.ErrorAs(t, err, &errMissingBodies)
		lowest, ok := errMissingBodies.LowestMissingBlockNum()
		require.True(t, ok)
		require.Equal(t, uint64(1), lowest)
		require.Nil(t, blocks.Data)
	})
}

func TestPenalizingFetcherFetchBlocksBackwardsByHashShouldPenalizePeerWhenErrUnexpectedHeaderHash(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1233)
	headers := newMockBlockHeaders(2)
	hash := headers[0].Hash()
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, headers[1:]),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       hash,
		wantRequestAmount:           1,
		wantReverse:                 true,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId1))
	test.mockSentryStreams(mockRequestResponse1)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errUnexpectedHeaderHash *ErrUnexpectedHeaderHash
		blocks, err := test.penalizingFetcher.FetchBlocksBackwardsByHash(ctx, hash, 1, peerId)
		require.ErrorAs(t, err, &errUnexpectedHeaderHash)
		require.Equal(t, hash, errUnexpectedHeaderHash.requested)
		require.Equal(t, headers[1].Hash(), errUnexpectedHeaderHash.received)
		require.Nil(t, blocks.Data)
	})
}

func TestPenalizingFetcherFetchBlocksBackwardsByHashShouldPenalizePeerWhenErrTooManyHeaders(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1233)
	headers := newMockBlockHeaders(2)
	hash := headers[0].Hash()
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, headers),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       hash,
		wantRequestAmount:           1,
		wantReverse:                 true,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId1))
	test.mockSentryStreams(mockRequestResponse1)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errTooManyHeaders *ErrTooManyHeaders
		blocks, err := test.penalizingFetcher.FetchBlocksBackwardsByHash(ctx, hash, 1, peerId)
		require.ErrorAs(t, err, &errTooManyHeaders)
		require.Equal(t, 1, errTooManyHeaders.requested)
		require.Equal(t, 2, errTooManyHeaders.received)
		require.Nil(t, blocks.Data)
	})
}

func TestPenalizingFetcherFetchBlocksBackwardsByHashShouldPenalizePeerWhenErrNonSequentialHeaderNumbers(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1233)
	headers := newMockBlockHeaders(3)
	hash := headers[2].Hash()
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, []*types.Header{headers[2], headers[0]}),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       hash,
		wantRequestAmount:           2,
		wantReverse:                 true,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId1))
	test.mockSentryStreams(mockRequestResponse1)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errNonSequentialHeaderNumbers *ErrNonSequentialHeaderNumbers
		blocks, err := test.penalizingFetcher.FetchBlocksBackwardsByHash(ctx, hash, 2, peerId)
		require.ErrorAs(t, err, &errNonSequentialHeaderNumbers)
		require.Equal(t, uint64(1), errNonSequentialHeaderNumbers.current)
		require.Equal(t, uint64(2), errNonSequentialHeaderNumbers.expected)
		require.Nil(t, blocks.Data)
	})
}

func TestPenalizingFetcherFetchBlocksBackwardsByHashShouldPenalizePeerWhenErrNonSequentialHeaderHashes(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1233)
	headers := newMockBlockHeaders(2)
	hash := headers[1].Hash()
	incorrectHeader := &types.Header{
		Number:   big.NewInt(1),
		GasLimit: 1234,
	}
	incorrectHeader.Hash()
	mockInboundMessages1 := []*sentryproto.InboundMessage{
		{
			Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   blockHeadersPacket66Bytes(t, requestId1, []*types.Header{headers[1], incorrectHeader}),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginHash:       hash,
		wantRequestAmount:           2,
		wantReverse:                 true,
	}

	test := newPenalizingFetcherTest(t, newMockRequestGenerator(requestId1))
	test.mockSentryStreams(mockRequestResponse1)
	// setup expectation that peer should be penalized
	mockExpectPenalizePeer(t, test.sentryClient, peerId)
	test.run(func(ctx context.Context, t *testing.T) {
		var errNonSequentialHeaderHashes *ErrNonSequentialHeaderHashes
		blocks, err := test.penalizingFetcher.FetchBlocksBackwardsByHash(ctx, hash, 2, peerId)
		require.ErrorAs(t, err, &errNonSequentialHeaderHashes)
		require.Equal(t, headers[1].Hash(), errNonSequentialHeaderHashes.hash)
		require.Equal(t, headers[0].Hash(), errNonSequentialHeaderHashes.parentHash)
		require.Equal(t, incorrectHeader.Hash(), errNonSequentialHeaderHashes.prevHash)
		require.Nil(t, blocks.Data)
	})
}

func newPenalizingFetcherTest(t *testing.T, requestIdGenerator RequestIdGenerator) *penalizingFetcherTest {
	fetcherTest := newFetcherTest(t, requestIdGenerator)
	peerPenalizer := NewPeerPenalizer(fetcherTest.sentryClient)
	penalizingFetcher := NewPenalizingFetcher(fetcherTest.logger, fetcherTest.fetcher, peerPenalizer)
	return &penalizingFetcherTest{
		fetcherTest:       fetcherTest,
		penalizingFetcher: penalizingFetcher,
	}
}

type penalizingFetcherTest struct {
	*fetcherTest
	penalizingFetcher *PenalizingFetcher
}

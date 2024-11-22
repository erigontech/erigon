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
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/protocols/eth"
)

func NewFetcher(logger log.Logger, ml *MessageListener, ms *MessageSender, opts ...FetcherOption) *FetcherBase {
	return &FetcherBase{
		logger:          logger,
		config:          defaultFetcherConfig.CopyWithOptions(opts...),
		messageListener: ml,
		messageSender:   ms,
	}
}

type FetcherBase struct {
	logger          log.Logger
	config          FetcherConfig
	messageListener *MessageListener
	messageSender   *MessageSender
}

func (f *FetcherBase) FetchHeaders(
	ctx context.Context,
	start uint64,
	end uint64,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Header], error) {
	if start >= end {
		return FetcherResponse[[]*types.Header]{}, &ErrInvalidFetchHeadersRange{
			start: start,
			end:   end,
		}
	}

	// Soft response limits are:
	//   1. 2 MB size
	//   2. 1024 headers
	//
	// A header is approximately 500 bytes, hence 1024 headers should be less than 2 MB.
	// As a simplification we can only use MaxHeadersServe for chunking.
	amount := end - start
	numChunks := amount / eth.MaxHeadersServe
	if amount%eth.MaxHeadersServe > 0 {
		numChunks++
	}

	totalHeadersSize := 0
	headers := make([]*types.Header, 0, amount)
	for chunkNum := uint64(0); chunkNum < numChunks; chunkNum++ {
		chunkStart := start + chunkNum*eth.MaxHeadersServe
		chunkEnd := min(end, chunkStart+eth.MaxHeadersServe)
		for chunkStart < chunkEnd {
			// a node may not respond with all MaxHeadersServe in 1 response,
			// so we keep on consuming from last received number (akin to consuming a paging api)
			// until we have all headers of the chunk or the peer stopped returning headers
			request := &eth.GetBlockHeadersPacket{
				Origin: eth.HashOrNumber{
					Number: chunkStart,
				},
				Amount: chunkEnd - chunkStart,
			}

			headersChunk, err := f.fetchHeadersWithRetry(ctx, request, peerId, f.config.CopyWithOptions(opts...))
			if err != nil {
				return FetcherResponse[[]*types.Header]{}, err
			}
			if len(headersChunk.Data) == 0 {
				break
			}

			headers = append(headers, headersChunk.Data...)
			chunkStart += uint64(len(headersChunk.Data))
			totalHeadersSize += headersChunk.TotalSize
		}
	}

	if err := f.validateHeadersResponse(headers, start, amount); err != nil {
		return FetcherResponse[[]*types.Header]{}, err
	}

	return FetcherResponse[[]*types.Header]{
		Data:      headers,
		TotalSize: totalHeadersSize,
	}, nil
}

func (f *FetcherBase) FetchBodies(
	ctx context.Context,
	headers []*types.Header,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Body], error) {
	var bodies []*types.Body
	totalBodiesSize := 0

	for len(headers) > 0 {
		// Note: we always request MaxBodiesServe for optimal response sizes (fully utilising the 2 MB soft limit).
		// In most cases the response will contain incomplete bodies list (ie < MaxBodiesServe) so we just
		// continue asking it for more starting from the first hash in the sequence after the last received one.
		// This is akin to how a paging API is consumed.
		var headersChunk []*types.Header
		if len(headers) > eth.MaxBodiesServe {
			headersChunk = headers[:eth.MaxBodiesServe]
		} else {
			headersChunk = headers
		}

		bodiesChunk, err := f.fetchBodiesWithRetry(ctx, headersChunk, peerId, f.config.CopyWithOptions(opts...))
		if err != nil {
			return FetcherResponse[[]*types.Body]{}, err
		}
		if len(bodiesChunk.Data) == 0 {
			return FetcherResponse[[]*types.Body]{}, NewErrMissingBodies(headers)
		}

		bodies = append(bodies, bodiesChunk.Data...)
		headers = headers[len(bodiesChunk.Data):]
		totalBodiesSize += bodiesChunk.TotalSize
	}

	return FetcherResponse[[]*types.Body]{
		Data:      bodies,
		TotalSize: totalBodiesSize,
	}, nil
}

func (f *FetcherBase) FetchBlocksBackwardsByHash(
	ctx context.Context,
	hash common.Hash,
	amount uint64,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Block], error) {
	if amount == 0 || amount > eth.MaxHeadersServe {
		return FetcherResponse[[]*types.Block]{}, fmt.Errorf("%w: amount=%d", ErrInvalidFetchBlocksAmount, amount)
	}
	request := &eth.GetBlockHeadersPacket{
		Origin: eth.HashOrNumber{
			Hash: hash,
		},
		Amount:  amount,
		Reverse: true,
	}

	config := f.config.CopyWithOptions(opts...)
	headersResponse, err := f.fetchHeadersWithRetry(ctx, request, peerId, config)
	if err != nil {
		return FetcherResponse[[]*types.Block]{}, err
	}

	headers := headersResponse.Data
	if len(headers) == 0 {
		return FetcherResponse[[]*types.Block]{}, &ErrMissingHeaderHash{requested: hash}
	}

	startHeader := headers[0]
	if startHeader.Hash() != hash {
		err := &ErrUnexpectedHeaderHash{requested: hash, received: startHeader.Hash()}
		return FetcherResponse[[]*types.Block]{}, err
	}

	offset := amount - 1 // safe, we check that amount > 0 at function start
	startNum := startHeader.Number.Uint64()
	if startNum > offset {
		startNum = startNum - offset
	} else {
		startNum = 0
	}

	slices.Reverse(headers)

	if err := f.validateHeadersResponse(headers, startNum, amount); err != nil {
		return FetcherResponse[[]*types.Block]{}, err
	}

	bodiesResponse, err := f.FetchBodies(ctx, headers, peerId, opts...)
	if err != nil {
		return FetcherResponse[[]*types.Block]{}, err
	}

	bodies := bodiesResponse.Data
	blocks := make([]*types.Block, len(headers))
	for i, header := range headers {
		blocks[i] = types.NewBlockFromNetwork(header, bodies[i])
	}

	response := FetcherResponse[[]*types.Block]{
		Data:      blocks,
		TotalSize: headersResponse.TotalSize + bodiesResponse.TotalSize,
	}

	return response, nil
}

func (f *FetcherBase) fetchHeadersWithRetry(
	ctx context.Context,
	request *eth.GetBlockHeadersPacket,
	peerId *PeerId,
	config FetcherConfig,
) (FetcherResponse[[]*types.Header], error) {
	attempt := 1
	return fetchWithRetry(config, func() (FetcherResponse[[]*types.Header], error) {
		response, err := f.fetchHeaders(ctx, request, peerId, config.responseTimeout)
		if err != nil {
			f.logger.Debug("[p2p.fetcher] fetching headers failure", "attempt", attempt, "peerId", peerId, "err", err)
			attempt++
		}
		return response, err
	})
}

func (f *FetcherBase) fetchHeaders(
	ctx context.Context,
	request *eth.GetBlockHeadersPacket,
	peerId *PeerId,
	responseTimeout time.Duration,
) (FetcherResponse[[]*types.Header], error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	messages := make(chan *DecodedInboundMessage[*eth.BlockHeadersPacket66])
	observer := func(message *DecodedInboundMessage[*eth.BlockHeadersPacket66]) {
		select {
		case <-ctx.Done():
			return
		case messages <- message:
			// no-op
		}
	}

	unregister := f.messageListener.RegisterBlockHeadersObserver(observer)
	defer unregister()

	requestId := f.config.requestIdGenerator()
	err := f.messageSender.SendGetBlockHeaders(ctx, peerId, eth.GetBlockHeadersPacket66{
		RequestId:             requestId,
		GetBlockHeadersPacket: request,
	})
	if err != nil {
		return FetcherResponse[[]*types.Header]{}, err
	}

	message, messageSize, err := awaitResponse(ctx, responseTimeout, messages, filterBlockHeaders(peerId, requestId))
	if err != nil {
		return FetcherResponse[[]*types.Header]{}, err
	}

	return FetcherResponse[[]*types.Header]{
		Data:      message.BlockHeadersPacket,
		TotalSize: messageSize,
	}, nil
}

func (f *FetcherBase) validateHeadersResponse(headers []*types.Header, start, amount uint64) error {
	headersLen := uint64(len(headers))
	if headersLen > amount {
		return &ErrTooManyHeaders{
			requested: int(amount),
			received:  len(headers),
		}
	}

	var prevHash common.Hash
	for i, header := range headers {
		expectedHeaderNum := start + uint64(i)
		currentHeaderNumber := header.Number.Uint64()
		if currentHeaderNumber != expectedHeaderNum {
			return &ErrNonSequentialHeaderNumbers{
				current:  currentHeaderNumber,
				expected: expectedHeaderNum,
			}
		}

		if i == 0 {
			prevHash = header.Hash()
			continue
		}

		if prevHash != header.ParentHash {
			return &ErrNonSequentialHeaderHashes{
				hash:       header.Hash(),
				parentHash: header.ParentHash,
				prevHash:   prevHash,
			}
		}

		prevHash = header.Hash()
	}

	if headersLen < amount {
		return &ErrIncompleteHeaders{
			start:     start,
			requested: amount,
			received:  headersLen,
		}
	}

	return nil
}

func (f *FetcherBase) fetchBodiesWithRetry(
	ctx context.Context,
	headers []*types.Header,
	peerId *PeerId,
	config FetcherConfig,
) (*FetcherResponse[[]*types.Body], error) {
	attempt := 1
	return fetchWithRetry(config, func() (*FetcherResponse[[]*types.Body], error) {
		response, err := f.fetchBodies(ctx, headers, peerId, config.responseTimeout)
		if err != nil {
			f.logger.Debug("[p2p.fetcher] fetching bodies failure", "attempt", attempt, "peerId", peerId, "err", err)
			attempt++
		}
		return response, err
	})
}

func (f *FetcherBase) fetchBodies(
	ctx context.Context,
	headers []*types.Header,
	peerId *PeerId,
	responseTimeout time.Duration,
) (*FetcherResponse[[]*types.Body], error) {
	// cleanup for the chan message observer
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	messages := make(chan *DecodedInboundMessage[*eth.BlockBodiesPacket66])
	observer := func(message *DecodedInboundMessage[*eth.BlockBodiesPacket66]) {
		select {
		case <-ctx.Done():
			return
		case messages <- message:
			// no-op
		}
	}

	unregister := f.messageListener.RegisterBlockBodiesObserver(observer)
	defer unregister()

	requestId := f.config.requestIdGenerator()
	hashes := make([]common.Hash, len(headers))
	for i, header := range headers {
		hashes[i] = header.Hash()
	}

	err := f.messageSender.SendGetBlockBodies(ctx, peerId, eth.GetBlockBodiesPacket66{
		RequestId:            requestId,
		GetBlockBodiesPacket: hashes,
	})
	if err != nil {
		return nil, err
	}

	message, messageSize, err := awaitResponse(ctx, responseTimeout, messages, filterBlockBodies(peerId, requestId))
	if err != nil {
		return nil, err
	}

	if err := f.validateBodies(message.BlockBodiesPacket, headers); err != nil {
		return nil, err
	}

	return &FetcherResponse[[]*types.Body]{
		Data:      message.BlockBodiesPacket,
		TotalSize: messageSize,
	}, nil
}

func (f *FetcherBase) validateBodies(bodies []*types.Body, headers []*types.Header) error {
	if len(bodies) > len(headers) {
		return &ErrTooManyBodies{
			requested: len(headers),
			received:  len(bodies),
		}
	}

	return nil
}

func fetchWithRetry[TData any](config FetcherConfig, fetch func() (TData, error)) (TData, error) {
	data, err := backoff.RetryWithData(func() (TData, error) {
		data, err := fetch()
		if err != nil {
			// retry timeouts
			if errors.Is(err, context.DeadlineExceeded) {
				return generics.Zero[TData](), err
			}

			// permanent errors are not retried
			return generics.Zero[TData](), backoff.Permanent(err)
		}

		return data, nil
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(config.retryBackOff), config.maxRetries))
	if err != nil {
		return generics.Zero[TData](), err
	}

	return data, nil
}

func awaitResponse[TPacket any](
	ctx context.Context,
	timeout time.Duration,
	messages chan *DecodedInboundMessage[TPacket],
	filter func(*DecodedInboundMessage[TPacket]) bool,
) (TPacket, int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			packet := generics.Zero[TPacket]()
			return packet, 0, fmt.Errorf("await %v response interrupted: %w", reflect.TypeOf(packet), ctx.Err())
		case message := <-messages:
			if filter(message) {
				continue
			}

			return message.Decoded, len(message.Data), nil
		}
	}
}

func filterBlockHeaders(peerId *PeerId, requestId uint64) func(*DecodedInboundMessage[*eth.BlockHeadersPacket66]) bool {
	return func(message *DecodedInboundMessage[*eth.BlockHeadersPacket66]) bool {
		return filter(peerId, message.PeerId, requestId, message.Decoded.RequestId)
	}
}

func filterBlockBodies(peerId *PeerId, requestId uint64) func(*DecodedInboundMessage[*eth.BlockBodiesPacket66]) bool {
	return func(message *DecodedInboundMessage[*eth.BlockBodiesPacket66]) bool {
		return filter(peerId, message.PeerId, requestId, message.Decoded.RequestId)
	}
}

func filter(requestPeerId, responsePeerId *PeerId, requestId, responseId uint64) bool {
	return !requestPeerId.Equal(responsePeerId) && requestId != responseId
}

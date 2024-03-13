package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ledgerwatch/log/v3"
	"modernc.org/mathutil"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
)

type RequestIdGenerator func() uint64

type FetcherConfig struct {
	responseTimeout time.Duration
	retryBackOff    time.Duration
	maxRetries      uint64
}

type Fetcher interface {
	// FetchHeaders fetches [start,end) headers from a peer. Blocks until data is received.
	FetchHeaders(ctx context.Context, start uint64, end uint64, peerId *PeerId) ([]*types.Header, error)
	// FetchBodies fetches block bodies for the given headers from a peer. Blocks until data is received.
	FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId) ([]*types.Body, error)
	// FetchBlocks fetches headers and bodies for a given [start, end) range from a peer and
	// assembles them into blocks. Blocks until data is received.
	FetchBlocks(ctx context.Context, start uint64, end uint64, peerId *PeerId) ([]*types.Block, error)
}

func NewFetcher(
	config FetcherConfig,
	logger log.Logger,
	messageListener MessageListener,
	messageSender MessageSender,
	requestIdGenerator RequestIdGenerator,
) Fetcher {
	return newFetcher(config, logger, messageListener, messageSender, requestIdGenerator)
}

func newFetcher(
	config FetcherConfig,
	logger log.Logger,
	messageListener MessageListener,
	messageSender MessageSender,
	requestIdGenerator RequestIdGenerator,
) *fetcher {
	return &fetcher{
		config:             config,
		logger:             logger,
		messageListener:    messageListener,
		messageSender:      messageSender,
		requestIdGenerator: requestIdGenerator,
	}
}

type fetcher struct {
	config             FetcherConfig
	logger             log.Logger
	messageListener    MessageListener
	messageSender      MessageSender
	requestIdGenerator RequestIdGenerator
}

func (f *fetcher) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId *PeerId) ([]*types.Header, error) {
	if start >= end {
		return nil, &ErrInvalidFetchHeadersRange{
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

	headers := make([]*types.Header, 0, amount)
	for chunkNum := uint64(0); chunkNum < numChunks; chunkNum++ {
		chunkStart := start + chunkNum*eth.MaxHeadersServe
		chunkEnd := mathutil.MinUint64(end, chunkStart+eth.MaxHeadersServe)
		headersChunk, err := fetchWithRetry(f.config, func() ([]*types.Header, error) {
			return f.fetchHeaders(ctx, chunkStart, chunkEnd, peerId)
		})
		if err != nil {
			return nil, err
		}

		headers = append(headers, headersChunk...)
	}

	if err := f.validateHeadersResponse(headers, start, amount); err != nil {
		return nil, err
	}

	return headers, nil
}

func (f *fetcher) FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId) ([]*types.Body, error) {
	var bodies []*types.Body

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

		bodiesChunk, err := fetchWithRetry(f.config, func() ([]*types.Body, error) {
			return f.fetchBodies(ctx, headersChunk, peerId)
		})
		if err != nil {
			return nil, err
		}
		if len(bodiesChunk) == 0 {
			return nil, NewErrMissingBodies(headers)
		}

		bodies = append(bodies, bodiesChunk...)
		headers = headers[len(bodiesChunk):]
	}

	return bodies, nil
}

func (f *fetcher) FetchBlocks(ctx context.Context, start, end uint64, peerId *PeerId) ([]*types.Block, error) {
	headers, err := f.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		return nil, err
	}

	bodies, err := f.FetchBodies(ctx, headers, peerId)
	if err != nil {
		return nil, err
	}

	blocks := make([]*types.Block, len(headers))
	for i, header := range headers {
		blocks[i] = types.NewBlockFromNetwork(header, bodies[i])
	}

	return blocks, nil
}

func (f *fetcher) fetchHeaders(ctx context.Context, start, end uint64, peerId *PeerId) ([]*types.Header, error) {
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

	requestId := f.requestIdGenerator()
	err := f.messageSender.SendGetBlockHeaders(ctx, peerId, eth.GetBlockHeadersPacket66{
		RequestId: requestId,
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Origin: eth.HashOrNumber{
				Number: start,
			},
			Amount: end - start,
		},
	})
	if err != nil {
		return nil, err
	}

	message, err := awaitResponse(ctx, f.config.responseTimeout, messages, filterBlockHeaders(peerId, requestId))
	if err != nil {
		return nil, err
	}

	return message.BlockHeadersPacket, nil
}

func (f *fetcher) validateHeadersResponse(headers []*types.Header, start, amount uint64) error {
	headersLen := uint64(len(headers))
	if headersLen > amount {
		return &ErrTooManyHeaders{
			requested: int(amount),
			received:  len(headers),
		}
	}

	for i, header := range headers {
		expectedHeaderNum := start + uint64(i)
		currentHeaderNumber := header.Number.Uint64()
		if currentHeaderNumber != expectedHeaderNum {
			return &ErrNonSequentialHeaderNumbers{
				current:  currentHeaderNumber,
				expected: expectedHeaderNum,
			}
		}
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

func (f *fetcher) fetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId) ([]*types.Body, error) {
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

	requestId := f.requestIdGenerator()
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

	message, err := awaitResponse(ctx, f.config.responseTimeout, messages, filterBlockBodies(peerId, requestId))
	if err != nil {
		return nil, err
	}

	if err := f.validateBodies(message.BlockBodiesPacket, headers); err != nil {
		return nil, err
	}

	return message.BlockBodiesPacket, nil
}

func (f *fetcher) validateBodies(bodies []*types.Body, headers []*types.Header) error {
	if len(bodies) > len(headers) {
		return &ErrTooManyBodies{
			requested: len(headers),
			received:  len(bodies),
		}
	}

	for _, body := range bodies {
		if len(body.Transactions) == 0 && len(body.Withdrawals) == 0 && len(body.Uncles) == 0 {
			return ErrEmptyBody
		}
	}

	return nil
}

func fetchWithRetry[TData any](config FetcherConfig, fetch func() (TData, error)) (TData, error) {
	data, err := backoff.RetryWithData(func() (TData, error) {
		data, err := fetch()
		if err != nil {
			var nilData TData
			// retry timeouts
			if errors.Is(err, context.DeadlineExceeded) {
				return nilData, err
			}

			// permanent errors are not retried
			return nilData, backoff.Permanent(err)
		}

		return data, nil
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(config.retryBackOff), config.maxRetries))
	if err != nil {
		var nilData TData
		return nilData, err
	}

	return data, nil
}

func awaitResponse[TPacket any](
	ctx context.Context,
	timeout time.Duration,
	messages chan *DecodedInboundMessage[TPacket],
	filter func(*DecodedInboundMessage[TPacket]) bool,
) (TPacket, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			var nilPacket TPacket
			return nilPacket, fmt.Errorf("await response interrupted: %w", ctx.Err())
		case message := <-messages:
			if filter(message) {
				continue
			}

			return message.Decoded, nil
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

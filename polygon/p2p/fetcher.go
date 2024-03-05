package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ledgerwatch/log/v3"
	"modernc.org/mathutil"

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
	FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error)
}

func NewFetcher(
	config FetcherConfig,
	logger log.Logger,
	messageListener MessageListener,
	messageSender MessageSender,
	requestIdGenerator RequestIdGenerator,
) Fetcher {
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

func (f *fetcher) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
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
		headerChunk, err := f.fetchHeaderChunkWithRetry(ctx, start, end, chunkNum, peerId)
		if err != nil {
			return nil, err
		}

		headers = append(headers, headerChunk...)
	}

	if err := f.validateHeadersResponse(headers, start, amount); err != nil {
		return nil, err
	}

	return headers, nil
}

func (f *fetcher) fetchHeaderChunkWithRetry(ctx context.Context, start, end, chunkNum uint64, peerId PeerId) ([]*types.Header, error) {
	headers, err := backoff.RetryWithData(func() ([]*types.Header, error) {
		headers, err := f.fetchHeaderChunk(ctx, start, end, chunkNum, peerId)
		if err != nil {
			// retry timeouts
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}

			// permanent errors are not retried
			return nil, backoff.Permanent(err)
		}

		return headers, nil
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(f.config.retryBackOff), f.config.maxRetries))
	if err != nil {
		return nil, err
	}

	return headers, nil
}

func (f *fetcher) fetchHeaderChunk(ctx context.Context, start, end, chunkNum uint64, peerId PeerId) ([]*types.Header, error) {
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

	chunkStart := start + chunkNum*eth.MaxHeadersServe
	chunkAmount := mathutil.MinUint64(end-chunkStart, eth.MaxHeadersServe)
	requestId := f.requestIdGenerator()

	err := f.messageSender.SendGetBlockHeaders(ctx, peerId, eth.GetBlockHeadersPacket66{
		RequestId: requestId,
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Origin: eth.HashOrNumber{
				Number: chunkStart,
			},
			Amount: chunkAmount,
		},
	})
	if err != nil {
		return nil, err
	}

	headers, err := f.awaitHeadersResponse(ctx, requestId, peerId, messages)
	if err != nil {
		return nil, err
	}

	return headers, nil
}

func (f *fetcher) awaitHeadersResponse(
	ctx context.Context,
	requestId uint64,
	peerId PeerId,
	messages <-chan *DecodedInboundMessage[*eth.BlockHeadersPacket66],
) ([]*types.Header, error) {
	ctx, cancel := context.WithTimeout(ctx, f.config.responseTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("await headers response interrupted: %w", ctx.Err())
		case message := <-messages:
			if message.PeerId != peerId {
				continue
			}

			if message.DecodeErr != nil {
				return nil, message.DecodeErr
			}

			if message.Decoded.RequestId != requestId {
				continue
			}

			return message.Decoded.BlockHeadersPacket, nil
		}
	}
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

type ErrInvalidFetchHeadersRange struct {
	start uint64
	end   uint64
}

func (e ErrInvalidFetchHeadersRange) Error() string {
	return fmt.Sprintf("invalid fetch headers range: start=%d, end=%d", e.start, e.end)
}

type ErrIncompleteHeaders struct {
	start     uint64
	requested uint64
	received  uint64
}

func (e ErrIncompleteHeaders) Error() string {
	return fmt.Sprintf(
		"incomplete fetch headers response: start=%d, requested=%d, received=%d",
		e.start, e.requested, e.received,
	)
}

func (e ErrIncompleteHeaders) LowestMissingBlockNum() uint64 {
	return e.start + e.received
}

type ErrTooManyHeaders struct {
	requested int
	received  int
}

func (e ErrTooManyHeaders) Error() string {
	return fmt.Sprintf("too many headers in fetch headers response: requested=%d, received=%d", e.requested, e.received)
}

func (e ErrTooManyHeaders) Is(err error) bool {
	var errTooManyHeaders *ErrTooManyHeaders
	switch {
	case errors.As(err, &errTooManyHeaders):
		return true
	default:
		return false
	}
}

type ErrNonSequentialHeaderNumbers struct {
	current  uint64
	expected uint64
}

func (e ErrNonSequentialHeaderNumbers) Error() string {
	return fmt.Sprintf(
		"non sequential header numbers in fetch headers response: current=%d, expected=%d",
		e.current, e.expected,
	)
}

func (e ErrNonSequentialHeaderNumbers) Is(err error) bool {
	var errDisconnectedHeaders *ErrNonSequentialHeaderNumbers
	switch {
	case errors.As(err, &errDisconnectedHeaders):
		return true
	default:
		return false
	}
}

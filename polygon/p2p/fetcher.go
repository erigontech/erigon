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
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
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

	if err := f.validateHeadersResponse(headers, start, end, amount); err != nil {
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
	// cleanup for the chan message observer
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	observer := NewChanMessageObserver[*sentry.InboundMessage](ctx)
	f.messageListener.RegisterBlockHeadersObserver(observer)
	defer f.messageListener.UnregisterBlockHeadersObserver(observer)

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

	headers, err := f.awaitHeadersResponse(ctx, requestId, peerId, observer)
	if err != nil {
		return nil, err
	}

	return headers, nil
}

func (f *fetcher) awaitHeadersResponse(
	ctx context.Context,
	requestId uint64,
	peerId PeerId,
	observer ChanMessageObserver[*sentry.InboundMessage],
) ([]*types.Header, error) {
	ctx, cancel := context.WithTimeout(ctx, f.config.responseTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("await headers response interrupted: %w", ctx.Err())
		case msg := <-observer.MessageChan():
			msgPeerId := PeerIdFromH512(msg.PeerId)
			if msgPeerId != peerId {
				continue
			}

			var pkt eth.BlockHeadersPacket66
			if err := rlp.DecodeBytes(msg.Data, &pkt); err != nil {
				return nil, fmt.Errorf("failed to decode BlockHeadersPacket66: %w", err)
			}

			if pkt.RequestId != requestId {
				continue
			}

			return pkt.BlockHeadersPacket, nil
		}
	}
}

func (f *fetcher) validateHeadersResponse(headers []*types.Header, start, end, amount uint64) error {
	if uint64(len(headers)) < amount {
		var first, last uint64
		if len(headers) > 0 {
			first = headers[0].Number.Uint64()
			last = headers[len(headers)-1].Number.Uint64()
		}

		return &ErrIncompleteHeaders{
			requestStart: start,
			requestEnd:   end,
			first:        first,
			last:         last,
			amount:       len(headers),
		}
	}

	if uint64(len(headers)) > amount {
		return &ErrTooManyHeaders{
			requested: int(amount),
			received:  len(headers),
		}
	}

	if start != headers[0].Number.Uint64() {
		return &ErrIncorrectOriginHeader{
			requested: start,
			received:  headers[0].Number.Uint64(),
		}
	}

	var parentHeader *types.Header
	for _, header := range headers {
		if parentHeader == nil {
			parentHeader = header
			continue
		}

		parentHeaderHash := parentHeader.Hash()
		currentHeaderNum := header.Number.Uint64()
		parentHeaderNum := parentHeader.Number.Uint64()
		if header.ParentHash != parentHeaderHash || currentHeaderNum != parentHeaderNum+1 {
			return &ErrDisconnectedHeaders{
				currentHash:       header.Hash(),
				currentParentHash: header.ParentHash,
				currentNum:        currentHeaderNum,
				parentHash:        parentHeaderHash,
				parentNum:         parentHeaderNum,
			}
		}

		parentHeader = header
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
	requestStart uint64
	requestEnd   uint64
	first        uint64
	last         uint64
	amount       int
}

func (e ErrIncompleteHeaders) Error() string {
	return fmt.Sprintf(
		"incomplete fetch headers response: first=%d, last=%d, amount=%d, requested [%d, %d)",
		e.first, e.last, e.amount, e.requestStart, e.requestEnd,
	)
}

func (e ErrIncompleteHeaders) LowestMissingBlockNum() uint64 {
	if e.last == 0 || e.first == 0 || e.first != e.requestStart {
		return e.requestStart
	}

	return e.last + 1
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

type ErrDisconnectedHeaders struct {
	currentHash       common.Hash
	currentParentHash common.Hash
	currentNum        uint64
	parentHash        common.Hash
	parentNum         uint64
}

func (e ErrDisconnectedHeaders) Error() string {
	return fmt.Sprintf(
		"disconnected headers in fetch headers response: %s, %s, %s, %s, %s",
		fmt.Sprintf("currentHash=%v", e.currentHash),
		fmt.Sprintf("currentParentHash=%v", e.currentParentHash),
		fmt.Sprintf("currentNum=%v", e.currentNum),
		fmt.Sprintf("parentHash=%v", e.parentHash),
		fmt.Sprintf("parentNum=%v", e.parentNum),
	)
}

func (e ErrDisconnectedHeaders) Is(err error) bool {
	var errDisconnectedHeaders *ErrDisconnectedHeaders
	switch {
	case errors.As(err, &errDisconnectedHeaders):
		return true
	default:
		return false
	}
}

type ErrIncorrectOriginHeader struct {
	requested uint64
	received  uint64
}

func (e ErrIncorrectOriginHeader) Error() string {
	return fmt.Sprintf("incorrect origin header: requested=%d, received=%d", e.requested, e.received)
}

func (e ErrIncorrectOriginHeader) Is(err error) bool {
	var errIncorrectOriginHeader *ErrIncorrectOriginHeader
	switch {
	case errors.As(err, &errIncorrectOriginHeader):
		return true
	default:
		return false
	}
}

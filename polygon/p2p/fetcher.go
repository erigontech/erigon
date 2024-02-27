package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

const responseTimeout = 5 * time.Second

type RequestIdGenerator func() uint64

type Fetcher interface {
	FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error)
}

func NewFetcher(
	logger log.Logger,
	messageListener MessageListener,
	messageSender MessageSender,
	peerPenalizer PeerPenalizer,
	requestIdGenerator RequestIdGenerator,
) Fetcher {
	return &fetcher{
		logger:             logger,
		messageListener:    messageListener,
		messageSender:      messageSender,
		peerPenalizer:      peerPenalizer,
		requestIdGenerator: requestIdGenerator,
	}
}

type fetcher struct {
	logger             log.Logger
	messageListener    MessageListener
	messageSender      MessageSender
	peerPenalizer      PeerPenalizer
	requestIdGenerator RequestIdGenerator
}

func (f *fetcher) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	if start >= end {
		return nil, &ErrInvalidFetchHeadersRange{
			start: start,
			end:   end,
		}
	}

	amount := end - start
	requestId := f.requestIdGenerator()
	observer := make(ChanMessageObserver[*sentry.InboundMessage])

	f.messageListener.RegisterBlockHeadersObserver(observer)
	defer f.messageListener.UnregisterBlockHeadersObserver(observer)

	//
	// TODO 1) chunk request into smaller ranges if needed to fit in the 2 MiB response size soft limit
	//      and also 1024 max headers server (check AnswerGetBlockHeadersQuery)
	err := f.messageSender.SendGetBlockHeaders(ctx, peerId, eth.GetBlockHeadersPacket66{
		RequestId: requestId,
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Origin: eth.HashOrNumber{
				Number: start,
			},
			Amount: amount,
		},
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, responseTimeout)
	defer cancel()

	var headers []*types.Header
	var requestReceived bool
	for !requestReceived {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("interrupted while waiting for msg from peer: %w", ctx.Err())
		case msg := <-observer:
			msgPeerId := PeerIdFromH512(msg.PeerId)
			if msgPeerId != peerId {
				continue
			}

			var pkt eth.BlockHeadersPacket66
			if err := rlp.DecodeBytes(msg.Data, &pkt); err != nil {
				if rlp.IsInvalidRLPError(err) {
					f.logger.Debug("penalizing peer for invalid rlp response", "peerId", peerId)
					penalizeErr := f.peerPenalizer.Penalize(ctx, peerId)
					if penalizeErr != nil {
						err = fmt.Errorf("%w: %w", penalizeErr, err)
					}
				}

				return nil, fmt.Errorf("failed to decode BlockHeadersPacket66: %w", err)
			}

			if pkt.RequestId != requestId {
				continue
			}

			headers = pkt.BlockHeadersPacket
			requestReceived = true
		}
	}

	if err = f.validateHeadersResponse(headers, start, end, amount); err != nil {
		shouldPenalize := errors.Is(err, &ErrIncorrectOriginHeader{}) ||
			errors.Is(err, &ErrTooManyHeaders{}) ||
			errors.Is(err, &ErrDisconnectedHeaders{})

		if shouldPenalize {
			f.logger.Debug("penalizing peer", "peerId", peerId, "err", err.Error())

			penalizeErr := f.peerPenalizer.Penalize(ctx, peerId)
			if penalizeErr != nil {
				err = fmt.Errorf("%w: %w", penalizeErr, err)
			}
		}

		return nil, err
	}

	return headers, nil
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
	currentHash       libcommon.Hash
	currentParentHash libcommon.Hash
	currentNum        uint64
	parentHash        libcommon.Hash
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

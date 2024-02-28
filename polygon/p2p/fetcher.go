package p2p

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"

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
	//      2) peer should return <= amount, check for > amount and penalize peer
	//
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

	if uint64(len(headers)) != amount {
		var first, last uint64
		if len(headers) > 0 {
			first = headers[0].Number.Uint64()
			last = headers[len(headers)-1].Number.Uint64()
		}

		return nil, &ErrIncompleteFetchHeadersResponse{
			requestStart: start,
			requestEnd:   end,
			first:        first,
			last:         last,
			amount:       len(headers),
		}
	}

	return headers, nil
}

type ErrInvalidFetchHeadersRange struct {
	start uint64
	end   uint64
}

func (e ErrInvalidFetchHeadersRange) Error() string {
	return fmt.Sprintf("invalid fetch headers range: start=%d, end=%d", e.start, e.end)
}

type ErrIncompleteFetchHeadersResponse struct {
	requestStart uint64
	requestEnd   uint64
	first        uint64
	last         uint64
	amount       int
}

func (e ErrIncompleteFetchHeadersResponse) Error() string {
	return fmt.Sprintf(
		"incomplete fetch headers response: first=%d, last=%d, amount=%d, requested [%d, %d)",
		e.first, e.last, e.amount, e.requestStart, e.requestEnd,
	)
}

func (e ErrIncompleteFetchHeadersResponse) LowestMissingBlockNum() uint64 {
	if e.last == 0 || e.first == 0 || e.first != e.requestStart {
		return e.requestStart
	}

	return e.last + 1
}

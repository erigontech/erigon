package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

const responseTimeout = 5 * time.Second

var invalidFetchHeadersRangeErr = errors.New("invalid fetch headers range")

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
		return nil, fmt.Errorf("%w: start=%d, end=%d", invalidFetchHeadersRangeErr, start, end)
	}

	var headers []*types.Header
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
			Amount: end - start,
		},
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, responseTimeout)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("interrupted while waiting for msg from peer: %w", ctx.Err())
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

					return fmt.Errorf("failed to decode BlockHeadersPacket66: %w", err)
				}

				if pkt.RequestId != requestId {
					continue
				}

				headers = pkt.BlockHeadersPacket
				return nil
			}
		}
	})

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	return headers, nil
}

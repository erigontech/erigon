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

const reqRespTimeout = 5 * time.Second

var (
	invalidDownloadHeadersRangeErr       = errors.New("invalid download headers range")
	incompleteDownloadHeadersResponseErr = errors.New("incomplete download headers response")
)

type RequestIdGenerator func() uint64

type Downloader interface {
	DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error)
}

func NewTrackingDownloader(
	logger log.Logger,
	messageListener MessageListener,
	messageBroadcaster MessageBroadcaster,
	peerPenalizer PeerPenalizer,
	requestIdGenerator RequestIdGenerator,
	peerTracker PeerTracker,
) Downloader {
	return &trackingDownloader{
		Downloader:  NewDownloader(logger, messageListener, messageBroadcaster, peerPenalizer, requestIdGenerator),
		peerTracker: peerTracker,
	}
}

type trackingDownloader struct {
	Downloader
	peerTracker PeerTracker
}

func (td *trackingDownloader) DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	res, err := td.Downloader.DownloadHeaders(ctx, start, end, peerId)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, incompleteDownloadHeadersResponseErr) {
			td.peerTracker.BlockNumMissing(peerId, start)
		}

		return nil, err
	}

	return res, nil
}

func NewDownloader(
	logger log.Logger,
	messageListener MessageListener,
	messageBroadcaster MessageBroadcaster,
	peerPenalizer PeerPenalizer,
	requestIdGenerator RequestIdGenerator,
) Downloader {
	return &downloader{
		logger:             logger,
		messageListener:    messageListener,
		messageBroadcaster: messageBroadcaster,
		peerPenalizer:      peerPenalizer,
		requestIdGenerator: requestIdGenerator,
	}
}

type downloader struct {
	logger             log.Logger
	messageListener    MessageListener
	messageBroadcaster MessageBroadcaster
	peerPenalizer      PeerPenalizer
	requestIdGenerator RequestIdGenerator
}

func (d *downloader) DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	if start > end {
		return nil, fmt.Errorf("%w: start=%d, end=%d", invalidDownloadHeadersRangeErr, start, end)
	}

	var headers []*types.Header
	requestId := d.requestIdGenerator()
	amount := end - start + 1

	observer := make(ChanMessageObserver[*sentry.InboundMessage])
	d.messageListener.RegisterBlockHeaders66Observer(observer)
	defer d.messageListener.UnregisterBlockHeaders66Observer(observer)

	ctx, cancel := context.WithTimeout(ctx, reqRespTimeout)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	//
	// TODO chunk request into smaller ranges if needed to fit in the 10 MiB response size
	// TODO peer should return <= amount, check for > amount and penalize peer
	//

	g.Go(func() error {
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
						d.logger.Debug("penalizing peer for invalid rlp response", "peerId", peerId)
						penalizeErr := d.peerPenalizer.Penalize(ctx, peerId)
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

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return d.messageBroadcaster.GetBlockHeaders66(ctx, peerId, eth.GetBlockHeadersPacket66{
				RequestId: requestId,
				GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
					Origin: eth.HashOrNumber{
						Number: start,
					},
					Amount: amount,
				},
			})
		}
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if uint64(len(headers)) != amount {
		return nil, fmt.Errorf(
			"%w: requested=%d, received=%d",
			incompleteDownloadHeadersResponseErr,
			amount,
			len(headers),
		)
	}

	return headers, nil
}

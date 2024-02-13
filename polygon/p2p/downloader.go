package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

const reqRespTimeout = 5 * time.Second

var invalidDownloadHeadersRangeErr = errors.New("invalid download headers range")

type RequestIdGenerator func() uint64

type Downloader interface {
	DownloadHeaders(ctx context.Context, start uint64, end uint64, pid PeerId) ([]*types.Header, error)
}

func NewDownloader(
	logger log.Logger,
	messageListener MessageListener,
	messageBroadcaster MessageBroadcaster,
	peerManager PeerManager,
	requestIdGenerator RequestIdGenerator,
) Downloader {
	return &downloader{
		logger:             logger,
		messageListener:    messageListener,
		messageBroadcaster: messageBroadcaster,
		peerManager:        peerManager,
		requestIdGenerator: requestIdGenerator,
	}
}

type downloader struct {
	logger             log.Logger
	messageListener    MessageListener
	messageBroadcaster MessageBroadcaster
	peerManager        PeerManager
	requestIdGenerator RequestIdGenerator
}

func (d *downloader) DownloadHeaders(ctx context.Context, start uint64, end uint64, pid PeerId) ([]*types.Header, error) {
	if start > end {
		return nil, fmt.Errorf("%w: start=%d, end=%d", invalidDownloadHeadersRangeErr, start, end)
	}

	var headers []*types.Header
	requestId := d.requestIdGenerator()

	observer := make(chanMessageObserver)
	d.messageListener.RegisterBlockHeaders66(observer)
	defer d.messageListener.UnregisterBlockHeaders66(observer)

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
				msgPid := PeerIdFromH512(msg.PeerId)
				if msgPid != pid {
					continue
				}

				var pkt eth.BlockHeadersPacket66
				if err := rlp.DecodeBytes(msg.Data, &pkt); err != nil {
					if rlp.IsInvalidRLPError(err) {
						d.logger.Debug("penalizing peer for invalid rlp response", "pid", pid.String())
						penalizeErr := d.peerManager.Penalize(ctx, pid)
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
			return d.messageBroadcaster.GetBlockHeaders66(ctx, pid, eth.GetBlockHeadersPacket66{
				RequestId: requestId,
				GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
					Origin: eth.HashOrNumber{
						Number: start,
					},
					Amount: end - start + 1,
				},
			})
		}
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return headers, nil
}

package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

var invalidDownloadHeadersRangeErr = errors.New("invalid download headers range")

type requestIdGenerator func() uint64

type downloader struct {
	messageListener    *messageListener
	messageBroadcaster *messageBroadcaster
	requestIdGenerator requestIdGenerator
}

func (d *downloader) DownloadHeaders(ctx context.Context, start uint64, end uint64, pid PeerId) ([]*types.Header, error) {
	if start > end {
		return nil, fmt.Errorf("%w: start=%d, end=%d", invalidDownloadHeadersRangeErr, start, end)
	}

	var headers []*types.Header
	reqId := d.requestIdGenerator()

	observer := make(chanMessageObserver)
	d.messageListener.RegisterBlockHeaders66(observer)
	defer d.messageListener.UnregisterBlockHeaders66(observer)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

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
					return fmt.Errorf("failed to decode BlockHeadersPacket66: %w", err)
				}

				if pkt.RequestId != reqId {
					continue
				}

				//
				// TODO what sort of validation if any should we do here?
				//      - check sentry_multi_client & refactor if necessary
				//

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
				RequestId: reqId,
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

package p2p

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

type downloader struct {
	messageListener    *messageListener
	messageBroadcaster *messageBroadcaster
}

func (d *downloader) DownloadHeaders(ctx context.Context, start uint64, end uint64, pid PeerId) ([]*types.Header, error) {
	if start > end {
		return nil, fmt.Errorf("invalid start and end in DownloadHeaders: start=%d, end=%d", start, end)
	}

	var headers []*types.Header
	reqId := rand.Uint64()

	observer := make(chanMessageObserver)
	d.messageListener.RegisterBlockHeaders66(observer)
	defer d.messageListener.UnregisterBlockHeaders66(observer)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
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

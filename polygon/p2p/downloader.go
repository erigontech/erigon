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
					if rlp.IsInvalidRLPError(err) {
						d.logger.Debug("penalizing peer for invalid rlp response", "pid", pid.String())
						d.peerManager.Penalize(pid)
					}

					return fmt.Errorf("failed to decode BlockHeadersPacket66: %w", err)
				}

				if pkt.RequestId != reqId {
					continue
				}

				//
				// TODO what sort of validation if any should we do here? check sentry_multi_client & refactor if necessary
				// Looks like sentry_multi_client does not actually do any validations
				// corresponding to the spec at this layer (p2p message processing).
				//
				// We should make a decision which of the following validations we want to do here
				// and which at higher layers.
				//
				// From the spec:
				/*
					The validity of block headers depends on the context in which they are used. For a single block header,
					only the validity of the proof-of-work seal (mix-digest, block-nonce) can be verified. When a header is
					used to extend the client's local chain, or multiple headers are processed in sequence during chain
					synchronization, the following rules apply:

					- Headers must form a chain where block numbers are consecutive and the parent-hash of each header
					matches the hash of the preceding header.
					- When extending the locally-stored chain, implementations must also verify that the values of
					difficulty, gas-limit and time are within the bounds of protocol rules given in the Yellow Paper.
					- The gas-used header field must be less than or equal to the gas-limit.
					- basefee-per-gas must be present in headers after the London hard fork. It must be absent for earlier
					blocks. This rule was added by EIP-1559.
					- For PoS blocks after The Merge, ommers-hash must be the empty keccak256 hash since no ommer headers
					can exist.
					- withdrawals-root must be present in headers after the Shanghai fork. The field must be absent for
					blocks before the fork. This rule was added by EIP-4895.
				*/

				//
				// TODO also limit on size of message 10MiB and penalize?
				// TODO should we check for too large of a range for download headers?
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

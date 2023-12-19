package blocksync

import (
	"context"
	"errors"
	"math/big"

	"github.com/ledgerwatch/erigon/core/types"
)

func NewBlockDownloader(p2pLayer p2pLayer, dbLayer dbLayer, heimdallLayer heimdallLayer) *BlockDownloader {
	return &BlockDownloader{
		p2pLayer:      p2pLayer,
		dbLayer:       dbLayer,
		heimdallLayer: heimdallLayer,
	}
}

type BlockDownloader struct {
	p2pLayer      p2pLayer
	dbLayer       dbLayer
	heimdallLayer heimdallLayer
}

func (bd BlockDownloader) ForwardDownloadUsingCheckpoints(ctx context.Context, fromBlockNum uint64) error {
	return bd.downloadUsingCheckpoints(ctx, fromBlockNum, false)
}

func (bd BlockDownloader) ForwardDownloadUsingMilestones(ctx context.Context, fromBlockNum uint64) error {
	return bd.downloadUsingMilestones(ctx, fromBlockNum, false)
}

func (bd BlockDownloader) BackwardDownloadUsingCheckpoints(ctx context.Context, fromBlockNum uint64) error {
	return bd.downloadUsingCheckpoints(ctx, fromBlockNum, true)
}

func (bd BlockDownloader) BackwardDownloadUsingMilestones(ctx context.Context, fromBlockNum uint64) error {
	return bd.downloadUsingMilestones(ctx, fromBlockNum, true)
}

func (bd BlockDownloader) downloadUsingMilestones(ctx context.Context, fromBlockNum uint64, reverse bool) error {
	milestones, err := bd.heimdallLayer.FetchMilestones(fromBlockNum)
	if err != nil {
		return err
	}

	err = bd.downloadUsingStatePoints(ctx, statePointListFromMilestones(milestones), reverse)
	if err != nil {
		return err
	}

	return nil
}

func (bd BlockDownloader) downloadUsingCheckpoints(ctx context.Context, fromBlockNum uint64, reverse bool) error {
	checkpoints, err := bd.heimdallLayer.FetchCheckpoints(fromBlockNum)
	if err != nil {
		return err
	}

	err = bd.downloadUsingStatePoints(ctx, statePointListFromCheckpoints(checkpoints), reverse)
	if err != nil {
		return err
	}

	return nil
}

func (bd BlockDownloader) downloadUsingStatePoints(ctx context.Context, spl statePointList, reverse bool) error {
	var statePointsBatch statePointList
	for len(spl) > 0 {
		peerCount := bd.p2pLayer.PeerCount()
		statePointsBatch, spl = spl.extractBatch(peerCount, reverse)

		blocksBatchChannels := make([]chan []*types.Block, len(statePointsBatch))
		errChannels := make([]chan error, len(statePointsBatch))
		maxStatePointLength := 0
		for i, sp := range statePointsBatch {
			// TODO generic max func
			length := sp.length()
			if length > maxStatePointLength {
				maxStatePointLength = length
			}

			blocksBatchChannels[i], errChannels[i] = bd.downloadBlockRange(sp.startBlock, sp.endBlock, i%peerCount)
		}

		blocks := make([]*types.Block, 0, maxStatePointLength*peerCount)
		for i := 0; i < len(blocksBatchChannels); i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case blocksBatch := <-blocksBatchChannels[i]:
				blocks = append(blocks, blocksBatch...)
			case err := <-errChannels[i]:
				if errors.Is(err, peerMissingBlocksErr) {
					// TODO mark as syncing - discuss if maintaining syncing nodes state should live inside the p2player
					spl = append(spl, statePointsBatch[i])
				} else if errors.Is(err, badPeerErr) {
					bd.p2pLayer.Penalize(i % peerCount)
					spl = append(spl, statePointsBatch[i])
				} else {
					return err
				}
			}
		}

		// TODO explore ETL and only call WriteBlocks once instead of per batch
		if err := bd.dbLayer.WriteBlocks(blocks); err != nil {
			return err
		}
	}

	return nil
}

func (bd BlockDownloader) downloadBlockRange(from, to *big.Int, peerIndex int) (chan []*types.Block, chan error) {
	blocksChannel := make(chan []*types.Block, 1)
	errChannel := make(chan error, 1)

	go func(from, to *big.Int, peerIndex int) {
		blocks, err := bd.p2pLayer.DownloadBlocks(from, to, peerIndex)
		if err != nil {
			errChannel <- err
			return
		}

		blocksChannel <- blocks
	}(from, to, peerIndex)

	return blocksChannel, errChannel
}

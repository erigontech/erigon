// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/protocols/eth"
)

type Publisher interface {
	PublishNewBlock(block *types.Block, td *big.Int)
	PublishNewBlockHashes(block *types.Block)
	Run(ctx context.Context) error
}

func NewPublisher(messageSender MessageSender) Publisher {
	return &publisher{
		messageSender: messageSender,
		tasks:         make(chan publishTask, 1024),
	}
}

// publisher manages block announcements according to the devp2p specs:
// https://github.com/ethereum/devp2p/blob/master/caps/eth.md#block-propagation
//
// It co-operates with the PeerTracker to ensure that we do not publish block/block hash announcements to peers if:
// 1) we have already published a given block/block hash to this peer or
// 2) that peer has already notified us of the given block/block hash
//
// It also handles the NewBlock publish requirement of only sending it to a small random portion (sqrt) of peers.
//
// All publish tasks are done asynchronously by putting them on a queue. If the publisher is struggling to keep up
// then newly enqueued publish tasks will get dropped.
type publisher struct {
	logger        log.Logger
	messageSender MessageSender
	peerTracker   PeerTracker
	tasks         chan publishTask
}

func (p publisher) PublishNewBlock(block *types.Block, td *big.Int) {
	p.enqueueTask(publishTask{
		taskType: newBlockPublishTask,
		block:    block,
		td:       td,
	})
}

func (p publisher) PublishNewBlockHashes(block *types.Block) {
	p.enqueueTask(publishTask{
		taskType: newBlockHashesPublishTask,
		block:    block,
	})
}

func (p publisher) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t := <-p.tasks:
			p.processPublishTask(ctx, t)
		}
	}
}

func (p publisher) enqueueTask(t publishTask) {
	select {
	case p.tasks <- t: // enqueued
	default:
		p.logger.Warn(
			"[p2p-publisher] task queue is full, dropping task",
			"blockNumber", t.block.Number(),
			"blockHash", t.block.Hash(),
			"taskType", t.taskType,
		)
	}
}

func (p publisher) processPublishTask(ctx context.Context, t publishTask) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	switch t.taskType {
	case newBlockPublishTask:
		p.processNewBlocksPublishTask(ctx, t)
	case newBlockHashesPublishTask:
		p.processNewBlockHashesPublishTask(ctx, t)
	default:
		panic(fmt.Sprintf("unknown task type: %v", t.taskType))
	}
}

func (p publisher) processNewBlocksPublishTask(ctx context.Context, t publishTask) {
	newBlockPacket := eth.NewBlockPacket{
		Block: t.block,
		TD:    t.td,
	}

	peers := p.peerTracker.ListPeersMayMissBlockHash(t.block.Hash())
	// devp2p spec: publish NewBlock to random sqrt(peers)
	// note ListPeersMayMissBlockHash has already done the shuffling for us
	peers = peers[:int(math.Sqrt(float64(len(peers))))]

	eg := errgroup.Group{}
	for _, peerId := range peers {
		eg.Go(func() error {
			// note underlying peer send message is async so this should finish quick
			if err := p.messageSender.SendNewBlock(ctx, peerId, newBlockPacket); err != nil {
				p.logger.Warn(
					"[p2p-publisher] could not publish new block to peer",
					"peerId", peerId,
					"blockNum", t.block.NumberU64(),
					"blockHash", t.block.Hash(),
					"err", err,
				)

				return nil // best effort async publish
			}

			// devp2p spec: mark as known, so that we do not re-announce same block hash to same peer
			p.peerTracker.BlockHashPresent(peerId, t.block.Hash())
			return nil
		})
	}

	_ = eg.Wait() // best effort async publish
}

func (p publisher) processNewBlockHashesPublishTask(ctx context.Context, t publishTask) {
	blockHash := t.block.Hash()
	blockNum := t.block.NumberU64()
	newBlockHashesPacket := eth.NewBlockHashesPacket{
		{
			Hash:   blockHash,
			Number: blockNum,
		},
	}

	eg := errgroup.Group{}
	peers := p.peerTracker.ListPeersMayMissBlockHash(blockHash)
	for _, peerId := range peers {
		eg.Go(func() error {
			// note underlying peer send message is async so this should finish quickly
			if err := p.messageSender.SendNewBlockHashes(ctx, peerId, newBlockHashesPacket); err != nil {
				p.logger.Warn(
					"[p2p-publisher] could not publish new block hashes to peer",
					"peerId", peerId,
					"blockNum", blockNum,
					"blockHash", blockHash,
					"err", err,
				)

				return nil // best effort async publish
			}

			// devp2p spec: mark as known, so that we do not re-announce same block hash to same peer
			p.peerTracker.BlockHashPresent(peerId, blockHash)
			return nil
		})
	}

	_ = eg.Wait() // best effort async publish
}

type publishTask struct {
	taskType publishTaskType
	block    *types.Block
	td       *big.Int
}

type publishTaskType int

const (
	newBlockHashesPublishTask publishTaskType = iota
	newBlockPublishTask
)

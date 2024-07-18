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

package blocks

import (
	"context"

	ethereum "github.com/erigontech/erigon"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/jsonrpc"
)

type BlockHandler interface {
	Handle(ctx context.Context, node devnet.Node, block *requests.Block, transaction *jsonrpc.RPCTransaction) error
}

type BlockHandlerFunc func(ctx context.Context, node devnet.Node, block *requests.Block, transaction *jsonrpc.RPCTransaction) error

func (f BlockHandlerFunc) Handle(ctx context.Context, node devnet.Node, block *requests.Block, transaction *jsonrpc.RPCTransaction) error {
	return f(ctx, node, block, transaction)
}

type BlockMap map[libcommon.Hash]*requests.Block

type waitResult struct {
	err      error
	blockMap BlockMap
}

type blockWaiter struct {
	result     chan waitResult
	hashes     chan map[libcommon.Hash]struct{}
	waitHashes map[libcommon.Hash]struct{}
	headersSub ethereum.Subscription
	handler    BlockHandler
	logger     log.Logger
}

type Waiter interface {
	Await(libcommon.Hash) (*requests.Block, error)
	AwaitMany(...libcommon.Hash) (BlockMap, error)
}

type waitError struct {
	err error
}

func (w waitError) Await(libcommon.Hash) (*requests.Block, error) {
	return nil, w.err
}

func (w waitError) AwaitMany(...libcommon.Hash) (BlockMap, error) {
	return nil, w.err
}

type wait struct {
	waiter *blockWaiter
}

func (w wait) Await(hash libcommon.Hash) (*requests.Block, error) {
	w.waiter.hashes <- map[libcommon.Hash]struct{}{hash: {}}
	res := <-w.waiter.result

	if len(res.blockMap) > 0 {
		for _, block := range res.blockMap {
			return block, res.err
		}
	}

	return nil, res.err
}

func (w wait) AwaitMany(hashes ...libcommon.Hash) (BlockMap, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	hashMap := map[libcommon.Hash]struct{}{}

	for _, hash := range hashes {
		hashMap[hash] = struct{}{}
	}

	w.waiter.hashes <- hashMap

	res := <-w.waiter.result
	return res.blockMap, res.err
}

func BlockWaiter(ctx context.Context, handler BlockHandler) (Waiter, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)

	node := devnet.SelectBlockProducer(ctx)

	waiter := &blockWaiter{
		result:  make(chan waitResult, 1),
		hashes:  make(chan map[libcommon.Hash]struct{}, 1),
		handler: handler,
		logger:  devnet.Logger(ctx),
	}

	var err error

	headers := make(chan types.Header)
	waiter.headersSub, err = node.Subscribe(ctx, requests.Methods.ETHNewHeads, headers)

	if err != nil {
		defer close(waiter.result)
		return waitError{err}, cancel
	}

	go waiter.receive(ctx, node, headers)

	return wait{waiter}, cancel
}

func (c *blockWaiter) receive(ctx context.Context, node devnet.Node, headers chan types.Header) {
	blockMap := map[libcommon.Hash]*requests.Block{}

	defer close(c.result)

	for header := range headers {

		select {
		case <-ctx.Done():
			c.headersSub.Unsubscribe()
			c.result <- waitResult{blockMap: blockMap, err: ctx.Err()}
			return
		default:
		}

		block, err := node.GetBlockByNumber(ctx, rpc.AsBlockNumber(header.Number), true)

		if err != nil {
			c.logger.Error("Block waiter failed to get block", "err", err)
			continue
		}

		if len(block.Transactions) > 0 && c.waitHashes == nil {
			c.waitHashes = <-c.hashes
		}

		for i := range block.Transactions {
			tx := block.Transactions[i] // avoid implicit memory aliasing

			if _, ok := c.waitHashes[tx.Hash]; ok {
				c.logger.Info("Tx included into block", "txHash", tx.Hash, "blockNum", block.Number)
				blockMap[tx.Hash] = block
				delete(c.waitHashes, tx.Hash)

				if len(c.waitHashes) == 0 {
					c.headersSub.Unsubscribe()
					res := waitResult{
						err: c.handler.Handle(ctx, node, block, tx),
					}

					if res.err == nil {
						res.blockMap = blockMap
					}

					c.result <- res
					return
				}
			}
		}
	}
}

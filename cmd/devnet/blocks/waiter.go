package blocks

import (
	"context"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type BlockHandler interface {
	Handle(ctx context.Context, node devnet.Node, block *requests.BlockResult, transaction *requests.Transaction) error
}

type BlockHandlerFunc func(ctx context.Context, node devnet.Node, block *requests.BlockResult, transaction *requests.Transaction) error

func (f BlockHandlerFunc) Handle(ctx context.Context, node devnet.Node, block *requests.BlockResult, transaction *requests.Transaction) error {
	return f(ctx, node, block, transaction)
}

type BlockMap map[libcommon.Hash]*requests.BlockResult

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
	Await(libcommon.Hash) (*requests.BlockResult, error)
	AwaitMany(...libcommon.Hash) (BlockMap, error)
}

type waitError struct {
	err error
}

func (w waitError) Await(libcommon.Hash) (*requests.BlockResult, error) {
	return nil, w.err
}

func (w waitError) AwaitMany(...libcommon.Hash) (BlockMap, error) {
	return nil, w.err
}

type wait struct {
	waiter *blockWaiter
}

func (w wait) Await(hash libcommon.Hash) (*requests.BlockResult, error) {
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
	blockMap := map[libcommon.Hash]*requests.BlockResult{}

	defer close(c.result)

	for header := range headers {
		select {
		case <-ctx.Done():
			c.headersSub.Unsubscribe()
			c.result <- waitResult{blockMap: blockMap, err: ctx.Err()}
			return
		default:
		}

		blockNum := header.Number

		block, err := node.GetBlockByNumber(blockNum.Uint64(), true)

		if err != nil {
			c.logger.Error("Block waiter failed to get block", "err", err)
			continue
		}

		if len(block.Transactions) > 0 && c.waitHashes == nil {
			c.waitHashes = <-c.hashes
		}

		for i := range block.Transactions {
			tx := &block.Transactions[i] // avoid implicit memory aliasing

			txHash := libcommon.HexToHash(tx.Hash)

			if _, ok := c.waitHashes[txHash]; ok {
				c.logger.Info("Tx included into block", "txHash", txHash, "blockNum", block.BlockNumber)
				blockMap[txHash] = block
				delete(c.waitHashes, txHash)

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

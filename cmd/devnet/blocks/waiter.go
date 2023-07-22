package blocks

import (
	"context"
	"math/big"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/log/v3"
)

type BlockHandler interface {
	Handle(ctx context.Context, node devnet.Node, block *requests.BlockResult, transaction *requests.Transaction) error
}

type BlockHandlerFunc func(ctx context.Context, node devnet.Node, block *requests.BlockResult, transaction *requests.Transaction) error

func (f BlockHandlerFunc) Handle(ctx context.Context, node devnet.Node, block *requests.BlockResult, transaction *requests.Transaction) error {
	return f(ctx, node, block, transaction)
}

type blockWaiter struct {
	err        chan error
	hash       chan libcommon.Hash
	waitHash   *libcommon.Hash
	headersSub ethereum.Subscription
	handler    BlockHandler
	logger     log.Logger
}

type Waiter interface {
	Await(libcommon.Hash) error
}

type WaiterFunc func(libcommon.Hash) error

func (f WaiterFunc) Await(hash libcommon.Hash) error {
	return f(hash)
}

func BlockWaiter(ctx context.Context, handler BlockHandler) (Waiter, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)

	node := devnet.SelectBlockProducer(ctx)

	waiter := &blockWaiter{
		err:     make(chan error, 1),
		hash:    make(chan libcommon.Hash, 1),
		handler: handler,
		logger:  devnet.Logger(ctx),
	}

	var err error

	waiter.headersSub, err = node.Subscribe(ctx, requests.Methods.ETHNewHeads, func(header interface{}) {
		waiter.receive(ctx, node, header)
	})

	if err != nil {
		close(waiter.err)
		return WaiterFunc(func(libcommon.Hash) error {
			return err
		}), cancel
	}

	return WaiterFunc(func(hash libcommon.Hash) error {
		waiter.hash <- hash
		err := <-waiter.err
		return err
	}), cancel
}

func (c *blockWaiter) receive(ctx context.Context, node devnet.Node, header interface{}) {
	select {
	case <-ctx.Done():
		c.headersSub.Unsubscribe()
		c.err <- ctx.Err()
		close(c.err)
	default:
	}

	blockNum, _ := (&big.Int{}).SetString(header.(map[string]interface{})["number"].(string)[2:], 16)

	if block, err := node.GetBlockByNumber(blockNum.Uint64(), true); err == nil {

		if len(block.Transactions) > 0 && c.waitHash == nil {
			waitHash := <-c.hash
			c.waitHash = &waitHash
		}

		for _, tx := range block.Transactions {
			if libcommon.HexToHash(tx.Hash) == *c.waitHash {
				c.headersSub.Unsubscribe()
				c.err <- c.handler.Handle(ctx, node, block, &tx)
				close(c.err)
				break
			}
		}
	} else {
		c.logger.Error("Block waiter failed to get block", "err", err)
	}
}

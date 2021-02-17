package txpoolprovider

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/txpool-provider/pb"
)

type TxPoolControlServer struct {
	pb.UnimplementedTxpoolControlServer
	chainConfig *params.ChainConfig
	kv          ethdb.KV
	events      *remotedbserver.Events
}

func NewTxPoolControlServer(cc *params.ChainConfig, kv ethdb.KV, events *remotedbserver.Events) *TxPoolControlServer {
	return &TxPoolControlServer{chainConfig: cc, kv: kv, events: events}
}

func (c *TxPoolControlServer) AccountInfo(ctx context.Context, request *pb.AccountInfoRequest) (*pb.AccountInfoReply, error) {
	var (
		db   = ethdb.NewObjectDatabase(c.kv)
		hash = common.BytesToHash(request.BlockHash)
		addr = common.BytesToAddress(request.Account)
	)

	head, err := rawdb.ReadHeaderByHash(db, hash)
	if err != nil {
		return nil, err
	}

	acc, err := readAccountAtBlock(ctx, c.kv, addr, head.Number.Uint64())
	if err != nil {
		return nil, err
	}

	return &pb.AccountInfoReply{
		Balance: acc.Balance.Bytes(),
		Nonce:   i64tob(acc.Nonce),
	}, nil
}

func (c *TxPoolControlServer) BlockStream(request *pb.BlockStreamRequest, stream pb.TxpoolControl_BlockStreamServer) error {
	var (
		db           = ethdb.NewObjectDatabase(c.kv)
		lastHeader   = new(types.Header)
		newRequestCh = make(chan *pb.BlockStreamRequest)
		newHeadCh    = make(chan *types.Header)
	)

	queueRequest := func(ch chan *pb.BlockStreamRequest) error {
		for {
			var msg *pb.BlockStreamRequest
			if err := stream.RecvMsg(msg); err != nil {
				return err
			}
			ch <- msg
		}

	}

	queueHeader := func(h *types.Header) error {
		newHeadCh <- h
		return nil
	}

	// Respond to the original request from the client.
	if err := handleStreamRequest(request, stream, lastHeader, c.chainConfig.ChainID, db); err != nil {
		return err
	}

	// Spawn listeners.
	go queueRequest(newRequestCh)
	c.events.AddHeaderSubscription(queueHeader)

	for {
		select {
		case msg := <-newRequestCh:
			if err := handleStreamRequest(msg, stream, lastHeader, c.chainConfig.ChainID, db); err != nil {
				return err
			}
		case newHeader := <-newHeadCh:
			diff, err := buildBlockDiff(lastHeader, newHeader, c.chainConfig.ChainID, db)
			if err != nil {
				return err
			}
			*lastHeader = *newHeader
			stream.Send(diff)

		}
	}
}

// handleStreamRequest is function that can be spawned as a routine to respond to stream requests.
func handleStreamRequest(request *pb.BlockStreamRequest, stream pb.TxpoolControl_BlockStreamServer, latest *types.Header, chainId *big.Int, db ethdb.Database) error {
	var err error

	if request.GetLatest() != nil {
		var (
			diff *pb.BlockDiff
			head *types.Header
		)

		if diff, err = buildBlockDiff(nil, nil, chainId, db); err != nil {
			return err
		}
		if head, err = rawdb.ReadHeaderByHash(db, rawdb.ReadHeadHeaderHash(db)); err != nil {
			return err
		}

		*latest = *head
		stream.Send(diff)
	} else {
		var (
			diff                 *pb.BlockDiff
			newHeader, oldHeader *types.Header
			hash                 = common.BytesToHash(request.GetBlockHash())
		)

		if newHeader, err = rawdb.ReadHeaderByHash(db, hash); err != nil {
			return err
		}
		if oldHeader, err = rawdb.ReadHeaderByHash(db, newHeader.ParentHash); err != nil {
			return err
		}
		if diff, err = buildBlockDiff(newHeader, oldHeader, chainId, db); err != nil {
			return err
		}

		stream.Send(diff)
	}
	return nil
}

func (c *TxPoolControlServer) mustEmbedUnimplementedTxpoolControlServer() {}

func readAccountAtBlock(ctx context.Context, kv ethdb.KV, addr common.Address, num uint64) (*accounts.Account, error) {
	acc := new(accounts.Account)
	getter := func(tx ethdb.Tx) error {
		reader := adapter.NewStateReader(tx, num)
		tmp, err := reader.ReadAccountData(addr)
		acc = tmp
		return err
	}
	if err := kv.View(ctx, getter); err != nil {
		return nil, fmt.Errorf("can't read account %q for block %v: %s", addr.String(), num, err)
	}

	return acc, nil
}

func i64tob(val uint64) []byte {
	r := make([]byte, 8)
	for i := uint64(0); i < 8; i++ {
		r[i] = byte((val >> (i * 8)) & 0xff)
	}
	return r
}

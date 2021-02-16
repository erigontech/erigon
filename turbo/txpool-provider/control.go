package txpoolprovider

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/txpool-provider/pb"
)

type TxPoolControlServer struct {
	pb.UnimplementedTxpoolControlServer
	kv     ethdb.KV
	events *remotedbserver.Events
}

func NewTxPoolControlServer(kv ethdb.KV, events *remotedbserver.Events) *TxPoolControlServer {
	return &TxPoolControlServer{kv: kv, events: events}
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
	db := ethdb.NewObjectDatabase(c.kv)

	// Tracks the last "latest" header sent in the stream.
	var (
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
	if err := handleStreamRequest(request, stream, lastHeader, db); err != nil {
		return err
	}
	fmt.Println(lastHeader.Hash().String())

	// Spawn listeners.
	go queueRequest(newRequestCh)
	c.events.AddHeaderSubscription(queueHeader)

	for {
		select {
		case msg := <-newRequestCh:
			if err := handleStreamRequest(msg, stream, lastHeader, db); err != nil {
				return err
			}
		case newHeader := <-newHeadCh:
			diff, err := buildBlockDiff(lastHeader, newHeader, db)
			if err != nil {
				return err
			}
			*lastHeader = *newHeader
			stream.Send(diff)

		}
	}
}

func handleStreamRequest(request *pb.BlockStreamRequest, stream pb.TxpoolControl_BlockStreamServer, latest *types.Header, db ethdb.Database) error {
	if request.GetLatest() != nil {
		diff, err := buildBlockDiff(nil, nil, db)
		if err != nil {
			return err
		}
		tmp, err := rawdb.ReadHeaderByHash(db, rawdb.ReadHeadHeaderHash(db))
		if err != nil {
			return err
		}
		*latest = *tmp
		stream.Send(diff)
	} else {
		hash := common.BytesToHash(request.GetBlockHash())
		newHeader, err := rawdb.ReadHeaderByHash(db, hash)
		if err != nil {
			return err
		}
		oldHeader, err := rawdb.ReadHeaderByHash(db, newHeader.ParentHash)
		if err != nil {
			return err
		}
		diff, err := buildBlockDiff(newHeader, oldHeader, db)
		if err != nil {
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

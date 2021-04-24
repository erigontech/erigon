package download

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
	"github.com/ledgerwatch/turbo-geth/eth/fetcher"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"google.golang.org/grpc"
)

func CombinedTxPool(db ethdb.Database, sentries []proto_sentry.SentryClient, chain string) error {
	ctx := rootContext()
	chainConfig, _, _, _ := cfg(db, chain)
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	txPool := core.NewTxPool(ethconfig.Defaults.TxPool, chainConfig, db, txCacher)
	controlServer, err := NewTxPoolService(sentries, txPool)
	if err != nil {
		return fmt.Errorf("create core P2P server: %w", err)
	}

	fetchTx := func(peerID string, hashes []common.Hash) error {
		controlServer.sendTxsRequest(context.TODO(), peerID, hashes)
		return nil
	}
	txFetcher := fetcher.NewTxFetcher(controlServer.txPool.Has, controlServer.txPool.AddRemotes, fetchTx)
	txFetcher.Start()

	go recvMessage(ctx, sentries[0], controlServer.handleInboundMessage)
	go recvUploadMessage(ctx, sentries[0], controlServer.handleInboundMessage)

	return nil
}

type TxPoolService struct {
	lock     sync.RWMutex
	sentries []proto_sentry.SentryClient
	txPool   *core.TxPool
	fetcher  *fetcher.TxFetcher
}

func NewTxPoolService(sentries []proto_sentry.SentryClient, txPool *core.TxPool) (*TxPoolService, error) {
	cs := &TxPoolService{
		sentries: sentries,
		txPool:   txPool,
	}
	return cs, nil
}

func (tp *TxPoolService) newPooledTransactionHashes(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.NewPooledTransactionHashesPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v, data: %x", err, inreq.Data)
	}
	return tp.fetcher.Notify(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query)
}

func (tp *TxPoolService) pooledTransactions(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.PooledTransactionsPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v, data: %x", err, inreq.Data)
	}

	return tp.fetcher.Enqueue(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query.PooledTransactionsPacket, true)
}

func (tp *TxPoolService) transactions(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	if tp.txPool == nil {
		return nil
	}
	var query eth.TransactionsPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v, data: %x", err, inreq.Data)
	}
	return tp.fetcher.Enqueue(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query, false)
}

func (tp *TxPoolService) getPooledTransactions(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	if tp.txPool == nil {
		return nil
	}
	var query eth.GetPooledTransactionsPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v, data: %x", err, inreq.Data)
	}
	_, txs := eth.AnswerGetPooledTransactions(tp.txPool, query.GetPooledTransactionsPacket)
	b, err := rlp.EncodeToBytes(&eth.PooledTransactionsRLPPacket66{
		RequestId:                   query.RequestId,
		PooledTransactionsRLPPacket: txs,
	})
	if err != nil {
		return fmt.Errorf("encode header response: %v", err)
	}
	// TODO: implement logic from perr.ReplyPooledTransactionsRLP - to remember tx ids
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data:   &proto_sentry.OutboundMessageData{Id: proto_sentry.MessageId_PooledTransactions, Data: b},
	}
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		return fmt.Errorf("send pooled transactions response: %v", err)
	}
	return nil
}

func (tp *TxPoolService) sendTxsRequest(ctx context.Context, peerID string, hashes []common.Hash) []byte {
	bytes, err := rlp.EncodeToBytes(&eth.GetPooledTransactionsPacket66{
		RequestId:                   rand.Uint64(), //nolint:gosec
		GetPooledTransactionsPacket: hashes,
	})
	if err != nil {
		log.Error("Could not send transactions request", "err", err)
		return nil
	}

	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: gointerfaces.ConvertBytesToH512([]byte(peerID)),
		Data:   &proto_sentry.OutboundMessageData{Id: proto_sentry.MessageId_GetPooledTransactions, Data: bytes},
	}

	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := tp.randSentryIndex(); ok; i, ok = next() {
		sentPeers, err1 := tp.sentries[i].SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
		if err1 != nil {
			log.Error("Could not send block bodies request", "err", err1)
			continue
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}
		return gointerfaces.ConvertH512ToBytes(sentPeers.Peers[0])
	}
	return nil
}

func (tp *TxPoolService) randSentryIndex() (int, bool, func() (int, bool)) {
	var i int
	if len(tp.sentries) > 1 {
		i = rand.Intn(len(tp.sentries) - 1)
	}
	to := i
	return i, true, func() (int, bool) {
		i = (i + 1) % len(tp.sentries)
		return i, i != to
	}
}

func (tp *TxPoolService) handleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	switch inreq.Id {
	case proto_sentry.MessageId_NewPooledTransactionHashes:
		return tp.newPooledTransactionHashes(ctx, inreq, sentry)
	case proto_sentry.MessageId_PooledTransactions:
		return tp.pooledTransactions(ctx, inreq, sentry)
	case proto_sentry.MessageId_Transactions:
		return tp.transactions(ctx, inreq, sentry)
	case proto_sentry.MessageId_GetPooledTransactions:
		return tp.getPooledTransactions(ctx, inreq, sentry)
	default:
		return fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

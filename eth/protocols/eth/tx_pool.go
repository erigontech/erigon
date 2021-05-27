package eth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ledgerwatch/erigon/cmd/headers/download"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/fetcher"
	"github.com/ledgerwatch/erigon/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/rlp"
	"google.golang.org/grpc"
)

type TxPoolServer struct {
	ctx       context.Context
	sentries  []download.SentryClient
	txPool    *core.TxPool
	TxFetcher *fetcher.TxFetcher
}

func NewTxPoolServer(ctx context.Context, sentries []download.SentryClient, txPool *core.TxPool) (*TxPoolServer, error) {
	cs := &TxPoolServer{
		ctx:      ctx,
		sentries: sentries,
		txPool:   txPool,
	}

	return cs, nil
}

func (tp *TxPoolServer) Start() {
	go RecvTxMessage(tp.ctx, tp.sentries[0], tp.HandleInboundMessage)
}

func (tp *TxPoolServer) newPooledTransactionHashes66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query NewPooledTransactionHashesPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding newPooledTransactionHashes66: %v, data: %x", err, inreq.Data)
	}
	return tp.TxFetcher.Notify(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query)
}

func (tp *TxPoolServer) newPooledTransactionHashes65(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query NewPooledTransactionHashesPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding newPooledTransactionHashes65: %v, data: %x", err, inreq.Data)
	}
	return tp.TxFetcher.Notify(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query)
}

func (tp *TxPoolServer) pooledTransactions66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	txs := &PooledTransactionsPacket66{}
	if err := txs.DecodeRLP(rlp.NewStream(bytes.NewReader(inreq.Data), 0)); err != nil {
		return fmt.Errorf("decoding pooledTransactions66: %v, data: %x", err, inreq.Data)
	}

	return tp.TxFetcher.Enqueue(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), txs.PooledTransactionsPacket, true)
}

func (tp *TxPoolServer) pooledTransactions65(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	txs := &PooledTransactionsPacket{}
	if err := txs.DecodeRLP(rlp.NewStream(bytes.NewReader(inreq.Data), 0)); err != nil {
		return fmt.Errorf("decoding pooledTransactions65: %v, data: %x", err, inreq.Data)
	}

	return tp.TxFetcher.Enqueue(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), *txs, true)
}

func (tp *TxPoolServer) transactions66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	return tp.transactions65(ctx, inreq, sentry)
}

func (tp *TxPoolServer) transactions65(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	if tp.txPool == nil {
		return nil
	}
	var query TransactionsPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding TransactionsPacket: %v, data: %x", err, inreq.Data)
	}
	return tp.TxFetcher.Enqueue(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query, false)
}

func (tp *TxPoolServer) getPooledTransactions66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	if tp.txPool == nil {
		return nil
	}
	var query GetPooledTransactionsPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetPooledTransactionsPacket66: %v, data: %x", err, inreq.Data)
	}
	_, txs := AnswerGetPooledTransactions(tp.txPool, query.GetPooledTransactionsPacket)
	b, err := rlp.EncodeToBytes(&PooledTransactionsRLPPacket66{
		RequestId:                   query.RequestId,
		PooledTransactionsRLPPacket: txs,
	})
	if err != nil {
		return fmt.Errorf("encode GetPooledTransactionsPacket66 response: %v", err)
	}
	// TODO: implement logic from perr.ReplyPooledTransactionsRLP - to remember tx ids
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data:   &proto_sentry.OutboundMessageData{Id: proto_sentry.MessageId_POOLED_TRANSACTIONS_66, Data: b},
	}
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		return fmt.Errorf("send pooled transactions response: %v", err)
	}
	return nil
}

func (tp *TxPoolServer) getPooledTransactions65(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	if tp.txPool == nil {
		return nil
	}
	var query GetPooledTransactionsPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getPooledTransactions65: %v, data: %x", err, inreq.Data)
	}
	_, txs := AnswerGetPooledTransactions(tp.txPool, query)
	b, err := rlp.EncodeToBytes(PooledTransactionsRLPPacket(txs))
	if err != nil {
		return fmt.Errorf("encode getPooledTransactions65 response: %v", err)
	}
	// TODO: implement logic from perr.ReplyPooledTransactionsRLP - to remember tx ids
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data:   &proto_sentry.OutboundMessageData{Id: proto_sentry.MessageId_POOLED_TRANSACTIONS_65, Data: b},
	}
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		return fmt.Errorf("send pooled transactions response: %v", err)
	}
	return nil
}

func (tp *TxPoolServer) SendTxsRequest(ctx context.Context, peerID string, hashes []common.Hash) []byte {
	var outreq65, outreq66 *proto_sentry.SendMessageByIdRequest

	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := tp.randSentryIndex(); ok; i, ok = next() {
		protocol, ok := tp.sentries[i].Protocol()
		if !ok {
			continue
		}
		if protocol == ETH65 {
			if outreq65 == nil {
				data65, err := rlp.EncodeToBytes(GetPooledTransactionsPacket(hashes))
				if err != nil {
					log.Error("Could not send transactions request", "err", err)
					return nil
				}

				outreq65 = &proto_sentry.SendMessageByIdRequest{
					PeerId: gointerfaces.ConvertBytesToH512([]byte(peerID)),
					Data:   &proto_sentry.OutboundMessageData{Id: proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_65, Data: data65},
				}
			}

			sentPeers, err1 := tp.sentries[i].SendMessageById(ctx, outreq65, &grpc.EmptyCallOption{})
			if err1 != nil {
				log.Error("Could not send get pooled tx request", "err", err1)
				continue
			}
			if sentPeers == nil || len(sentPeers.Peers) == 0 {
				continue
			}
			return gointerfaces.ConvertH512ToBytes(sentPeers.Peers[0])
		}
		if protocol == ETH66 {
			if outreq66 == nil {
				data66, err := rlp.EncodeToBytes(&GetPooledTransactionsPacket66{
					RequestId:                   rand.Uint64(), //nolint:gosec
					GetPooledTransactionsPacket: hashes,
				})
				if err != nil {
					log.Error("Could not send transactions request", "err", err)
					return nil
				}

				outreq66 = &proto_sentry.SendMessageByIdRequest{
					PeerId: gointerfaces.ConvertBytesToH512([]byte(peerID)),
					Data:   &proto_sentry.OutboundMessageData{Id: proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_66, Data: data66},
				}
			}

			sentPeers, err1 := tp.sentries[i].SendMessageById(ctx, outreq66, &grpc.EmptyCallOption{})
			if err1 != nil {
				log.Error("Could not send get pooled tx request", "err", err1)
				continue
			}
			if sentPeers == nil || len(sentPeers.Peers) == 0 {
				continue
			}
			return gointerfaces.ConvertH512ToBytes(sentPeers.Peers[0])
		}
	}
	return nil
}

func (tp *TxPoolServer) randSentryIndex() (int, bool, func() (int, bool)) {
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

func (tp *TxPoolServer) HandleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	switch inreq.Id {

	// ==== eth 65 ====
	case proto_sentry.MessageId_TRANSACTIONS_65:
		return tp.transactions65(ctx, inreq, sentry)
	case proto_sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65:
		return tp.newPooledTransactionHashes65(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_65:
		return tp.getPooledTransactions65(ctx, inreq, sentry)
	case proto_sentry.MessageId_POOLED_TRANSACTIONS_65:
		return tp.pooledTransactions65(ctx, inreq, sentry)

	// ==== eth 66 ====
	case proto_sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66:
		return tp.newPooledTransactionHashes66(ctx, inreq, sentry)
	case proto_sentry.MessageId_POOLED_TRANSACTIONS_66:
		return tp.pooledTransactions66(ctx, inreq, sentry)
	case proto_sentry.MessageId_TRANSACTIONS_66:
		return tp.transactions66(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_66:
		return tp.getPooledTransactions66(ctx, inreq, sentry)
	default:
		return fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

func RecvTxMessage(ctx context.Context, sentry proto_sentry.SentryClient, handleInboundMessage func(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	receiveClient, err2 := sentry.ReceiveTxMessages(streamCtx, &empty.Empty{}, &grpc.EmptyCallOption{})
	if err2 != nil {
		log.Error("ReceiveTx messages failed", "error", err2)
		return
	}

	for req, err := receiveClient.Recv(); ; req, err = receiveClient.Recv() {
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Error("ReceiveTx loop terminated", "error", err)
				return
			}
			return
		}
		if req == nil {
			return
		}
		if err = handleInboundMessage(ctx, req, sentry); err != nil {
			log.Error("RecvTxMessage: Handling incoming message", "error", err)
		}
	}
}

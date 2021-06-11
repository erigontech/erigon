package txpool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/fetcher"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// P2PServer - receiving and sending messages to Sentries
type P2PServer struct {
	ctx       context.Context
	Sentries  []remote.SentryClient
	txPool    *core.TxPool
	TxFetcher *fetcher.TxFetcher
}

func NewP2PServer(ctx context.Context, sentries []remote.SentryClient, txPool *core.TxPool) (*P2PServer, error) {
	cs := &P2PServer{
		ctx:      ctx,
		Sentries: sentries,
		txPool:   txPool,
	}

	return cs, nil
}

func (tp *P2PServer) newPooledTransactionHashes66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
	var query eth.NewPooledTransactionHashesPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding newPooledTransactionHashes66: %v, data: %x", err, inreq.Data)
	}
	return tp.TxFetcher.Notify(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query)
}

func (tp *P2PServer) newPooledTransactionHashes65(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
	var query eth.NewPooledTransactionHashesPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding newPooledTransactionHashes65: %v, data: %x", err, inreq.Data)
	}
	return tp.TxFetcher.Notify(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query)
}

func (tp *P2PServer) pooledTransactions66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
	txs := &eth.PooledTransactionsPacket66{}
	if err := txs.DecodeRLP(rlp.NewStream(bytes.NewReader(inreq.Data), 0)); err != nil {
		return fmt.Errorf("decoding pooledTransactions66: %v, data: %x", err, inreq.Data)
	}

	return tp.TxFetcher.Enqueue(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), txs.PooledTransactionsPacket, true)
}

func (tp *P2PServer) pooledTransactions65(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
	txs := &eth.PooledTransactionsPacket{}
	if err := txs.DecodeRLP(rlp.NewStream(bytes.NewReader(inreq.Data), 0)); err != nil {
		return fmt.Errorf("decoding pooledTransactions65: %v, data: %x", err, inreq.Data)
	}

	return tp.TxFetcher.Enqueue(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), *txs, true)
}

func (tp *P2PServer) transactions66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
	return tp.transactions65(ctx, inreq, sentry)
}

func (tp *P2PServer) transactions65(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
	if tp.txPool == nil {
		return nil
	}
	var query eth.TransactionsPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding TransactionsPacket: %v, data: %x", err, inreq.Data)
	}
	return tp.TxFetcher.Enqueue(string(gointerfaces.ConvertH512ToBytes(inreq.PeerId)), query, false)
}

func (tp *P2PServer) getPooledTransactions66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
	if tp.txPool == nil {
		return nil
	}
	var query eth.GetPooledTransactionsPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetPooledTransactionsPacket66: %v, data: %x", err, inreq.Data)
	}
	_, txs := eth.AnswerGetPooledTransactions(tp.txPool, query.GetPooledTransactionsPacket)
	b, err := rlp.EncodeToBytes(&eth.PooledTransactionsRLPPacket66{
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

func (tp *P2PServer) getPooledTransactions65(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
	if tp.txPool == nil {
		return nil
	}
	var query eth.GetPooledTransactionsPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getPooledTransactions65: %v, data: %x", err, inreq.Data)
	}
	_, txs := eth.AnswerGetPooledTransactions(tp.txPool, query)
	b, err := rlp.EncodeToBytes(eth.PooledTransactionsRLPPacket(txs))
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

func (tp *P2PServer) SendTxsRequest(ctx context.Context, peerID string, hashes []common.Hash) []byte {
	var outreq65, outreq66 *proto_sentry.SendMessageByIdRequest
	var lastErr error

	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := tp.randSentryIndex(); ok; i, ok = next() {
		if !tp.Sentries[i].Ready() {
			continue
		}

		switch tp.Sentries[i].Protocol() {
		case eth.ETH65:
			if outreq65 == nil {
				data65, err := rlp.EncodeToBytes(eth.GetPooledTransactionsPacket(hashes))
				if err != nil {
					log.Error("Could not encode transactions request", "err", err)
					return nil
				}

				outreq65 = &proto_sentry.SendMessageByIdRequest{
					PeerId: gointerfaces.ConvertBytesToH512([]byte(peerID)),
					Data:   &proto_sentry.OutboundMessageData{Id: proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_65, Data: data65},
				}
			}

			if sentPeers, err1 := tp.Sentries[i].SendMessageById(ctx, outreq65, &grpc.EmptyCallOption{}); err1 != nil {
				lastErr = err1
			} else if sentPeers != nil && len(sentPeers.Peers) != 0 {
				return gointerfaces.ConvertH512ToBytes(sentPeers.Peers[0])
			}
		case eth.ETH66:
			if outreq66 == nil {
				data66, err := rlp.EncodeToBytes(&eth.GetPooledTransactionsPacket66{
					RequestId:                   rand.Uint64(), //nolint:gosec
					GetPooledTransactionsPacket: hashes,
				})
				if err != nil {
					log.Error("Could not encode transactions request", "err", err)
					return nil
				}

				outreq66 = &proto_sentry.SendMessageByIdRequest{
					PeerId: gointerfaces.ConvertBytesToH512([]byte(peerID)),
					Data:   &proto_sentry.OutboundMessageData{Id: proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_66, Data: data66},
				}
			}

			if sentPeers, err1 := tp.Sentries[i].SendMessageById(ctx, outreq66, &grpc.EmptyCallOption{}); err1 != nil {
				lastErr = err1
			} else if sentPeers != nil && len(sentPeers.Peers) != 0 {
				return gointerfaces.ConvertH512ToBytes(sentPeers.Peers[0])
			}
		}
	}
	if lastErr != nil {
		log.Error("Could not sent get pooled txs request to any sentry", "error", lastErr)
	}
	return nil
}

func (tp *P2PServer) randSentryIndex() (int, bool, func() (int, bool)) {
	var i int
	if len(tp.Sentries) > 1 {
		i = rand.Intn(len(tp.Sentries))
	}
	to := i
	return i, true, func() (int, bool) {
		i = (i + 1) % len(tp.Sentries)
		return i, i != to
	}
}

func (tp *P2PServer) HandleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error {
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

func RecvTxMessageLoop(ctx context.Context,
	sentry remote.SentryClient,
	cs *download.ControlServerImpl,
	handleInboundMessage func(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error,
	wg *sync.WaitGroup,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		download.SentryHandshake(ctx, sentry, cs)
		RecvTxMessage(ctx, sentry, handleInboundMessage, wg)
	}
}

// RecvTxMessage
// wg is used only in tests to avoid time.Sleep. For non-test code wg == nil
func RecvTxMessage(ctx context.Context,
	sentry remote.SentryClient,
	handleInboundMessage func(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry remote.SentryClient) error,
	wg *sync.WaitGroup,
) {
	// avoid crash because Erigon's core does many things
	defer func() {
		if r := recover(); r != nil { // just log is enough
			panicReplacer := strings.NewReplacer("\n", " ", "\t", "", "\r", "")
			stack := panicReplacer.Replace(string(debug.Stack()))
			switch typed := r.(type) {
			case error:
				log.Error("[RecvTxMessage] fail", "err", fmt.Errorf("%w, trace: %s", typed, stack))
			default:
				log.Error("[RecvTxMessage] fail", "err", fmt.Errorf("%w, trace: %s", typed, stack))
			}
		}
	}()
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := sentry.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: []proto_sentry.MessageId{
		eth.ToProto[eth.ETH65][eth.NewPooledTransactionHashesMsg],
		eth.ToProto[eth.ETH65][eth.GetPooledTransactionsMsg],
		eth.ToProto[eth.ETH65][eth.TransactionsMsg],
		eth.ToProto[eth.ETH65][eth.PooledTransactionsMsg],

		eth.ToProto[eth.ETH66][eth.NewPooledTransactionHashesMsg],
		eth.ToProto[eth.ETH66][eth.GetPooledTransactionsMsg],
		eth.ToProto[eth.ETH66][eth.TransactionsMsg],
		eth.ToProto[eth.ETH66][eth.PooledTransactionsMsg],
	}}, grpc.WaitForReady(true))
	if err != nil {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
			return
		}
		if errors.Is(err, io.EOF) {
			return
		}
		log.Error("ReceiveTx messages failed", "error", err)
		return
	}

	for req, err := stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
				return
			}
			if errors.Is(err, io.EOF) {
				return
			}
			log.Error("[RecvTxMessage] Sentry disconnected", "error", err)
			return
		}
		if req == nil {
			return
		}
		if err = handleInboundMessage(ctx, req, sentry); err != nil {
			log.Error("RecvTxMessage: Handling incoming message", "error", err)
		}
		if wg != nil {
			wg.Done()
		}
	}
}

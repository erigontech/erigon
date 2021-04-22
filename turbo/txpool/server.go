package txpool

import (
	"bytes"
	"context"

	mapset "github.com/deckarep/golang-set"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_txpool "github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// maxTxUnderpricedSetSize is the size of the underpriced transaction set that
	// is used to track recent transactions that have been dropped so we don't
	// re-request them.
	maxTxUnderpricedSetSize = 32768
)

var (
	txAnnounceInMeter          = metrics.NewRegisteredMeter("eth/fetcher/transaction/announces/in", nil)
	txAnnounceKnownMeter       = metrics.NewRegisteredMeter("eth/fetcher/transaction/announces/known", nil)
	txAnnounceUnderpricedMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/announces/underpriced", nil)

	txBroadcastKnownMeter       = metrics.NewRegisteredMeter("eth/fetcher/transaction/broadcasts/known", nil)
	txBroadcastUnderpricedMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/broadcasts/underpriced", nil)
	txBroadcastOtherRejectMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/broadcasts/otherreject", nil)

	//txRequestOutMeter     = metrics.NewRegisteredMeter("eth/fetcher/transaction/request/out", nil)
	//txRequestFailMeter    = metrics.NewRegisteredMeter("eth/fetcher/transaction/request/fail", nil)
	//txRequestDoneMeter    = metrics.NewRegisteredMeter("eth/fetcher/transaction/request/done", nil)
	//txRequestTimeoutMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/request/timeout", nil)

	//txReplyKnownMeter       = metrics.NewRegisteredMeter("eth/fetcher/transaction/replies/known", nil)
	//txReplyUnderpricedMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/replies/underpriced", nil)
	//txReplyOtherRejectMeter = metrics.NewRegisteredMeter("eth/fetcher/transaction/replies/otherreject", nil)

)

type Server struct {
	proto_txpool.UnimplementedTxpoolServer
	ctx         context.Context
	txPool      txPool
	underpriced mapset.Set // Transactions discarded as too cheap (don't re-fetch)
}

// txPool defines the methods needed from a transaction pool implementation to
// support all the operations needed by the Ethereum chain protocols.
type txPool interface {
	// Has returns an indicator whether txpool has a transaction
	// cached with the given hash.
	Has(hash common.Hash) bool

	// Get retrieves the transaction from local txpool with given
	// tx hash.
	Get(hash common.Hash) *types.Transaction

	// AddRemotes should add the given transactions to the pool.
	AddRemotes([]*types.Transaction) []error

	// Pending should return pending transactions.
	// The slice should be modifiable by the caller.
	Pending() (types.TransactionsGroupedBySender, error)

	// SubscribeNewTxsEvent should return an event subscription of
	// NewTxsEvent and send events to the given channel.
	SubscribeNewTxsEvent(chan<- core.NewTxsEvent) event.Subscription
}

func NewServer(ctx context.Context, txPool txPool) *Server {
	return &Server{ctx: ctx, txPool: txPool, underpriced: mapset.NewSet()}
}

func (s *Server) FindUnknownTransactions(ctx context.Context, in *proto_txpool.TxHashes) (*proto_txpool.TxHashes, error) {
	reply := &proto_txpool.TxHashes{}
	var underpriced int
	for i := range in.Hashes {
		h := gointerfaces.ConvertH256ToHash(in.Hashes[i])
		if s.txPool.Has(h) {
			continue
		}
		if s.underpriced.Contains(h) {
			underpriced++
		}
		reply.Hashes = append(reply.Hashes, in.Hashes[i])
	}

	txAnnounceInMeter.Mark(int64(len(in.Hashes)))
	txAnnounceKnownMeter.Mark(int64(len(in.Hashes) - len(reply.Hashes)))
	txAnnounceUnderpricedMeter.Mark(int64(underpriced))

	return reply, nil
}

func (s *Server) ImportTransactions(ctx context.Context, in *proto_txpool.ImportRequest) (*proto_txpool.ImportReply, error) {
	reply := &proto_txpool.ImportReply{Imported: make([]proto_txpool.ImportResult, len(in.Txs))}
	txs, err := UnmarshalTxs(in.Txs)
	if err != nil {
		return nil, err
	}
	var duplicate int64
	var underpriced int64
	var otherreject int64
	errs := s.txPool.AddRemotes(txs)
	for i, err := range errs {
		if err != nil {
			// Track the transaction hash if the price is too low for us.
			// Avoid re-request this transaction when we receive another
			// announcement.
			if err == core.ErrUnderpriced || err == core.ErrReplaceUnderpriced {
				for s.underpriced.Cardinality() >= maxTxUnderpricedSetSize {
					s.underpriced.Pop()
				}
				s.underpriced.Add(txs[i].Hash())
			}
			// Track a few interesting failure types
			switch err {
			case nil: // Noop, but need to handle to not count these

			case core.ErrAlreadyKnown:
				duplicate++
				reply.Imported[i] = proto_txpool.ImportResult_ALREADY_EXISTS

			case core.ErrUnderpriced, core.ErrReplaceUnderpriced:
				underpriced++
				reply.Imported[i] = proto_txpool.ImportResult_FEE_TOO_LOW

			case core.ErrInvalidSender, core.ErrGasLimit, core.ErrNegativeValue, core.ErrOversizedData:
				otherreject++
				reply.Imported[i] = proto_txpool.ImportResult_INVALID

			default:
				otherreject++
				reply.Imported[i] = proto_txpool.ImportResult_INTERNAL_ERROR
			}
		}
	}
	txBroadcastKnownMeter.Mark(duplicate)
	txBroadcastUnderpricedMeter.Mark(underpriced)
	txBroadcastOtherRejectMeter.Mark(otherreject)
	return reply, nil
}

func (s *Server) GetTransactions(ctx context.Context, in *proto_txpool.GetTransactionsRequest) (*proto_txpool.GetTransactionsReply, error) {
	buf := bytes.NewBuffer(nil)
	reply := &proto_txpool.GetTransactionsReply{Txs: make([][]byte, len(in.Hashes))}
	for i := range in.Hashes {
		txn := s.txPool.Get(gointerfaces.ConvertH256ToHash(in.Hashes[i]))
		if txn == nil {
			reply.Txs[i] = nil
			continue
		}
		buf.Reset()
		if err := rlp.Encode(buf, txn); err != nil {
			return nil, err
		}
		reply.Txs[i] = common.CopyBytes(buf.Bytes())
	}

	return reply, nil
}

func MarshalTxs(txs types.Transactions) ([][]byte, error) {
	var err error
	result := make([][]byte, len(txs))
	for i := range txs {
		if txs[i] == nil {
			result[i] = nil
			continue
		}
		result[i], err = rlp.EncodeToBytes(txs[i])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func UnmarshalTxs(txs [][]byte) (types.Transactions, error) {
	result := make(types.Transactions, len(txs))
	for i := range txs {
		result[i] = &types.Transaction{}
		if err := rlp.DecodeBytes(txs[i], result[i]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

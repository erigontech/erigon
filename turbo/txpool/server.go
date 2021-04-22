package txpool

import (
	"bytes"
	"context"

	mapset "github.com/deckarep/golang-set"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_txpool "github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
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

type Server struct {
	proto_txpool.UnimplementedTxpoolServer
	ctx         context.Context
	txPool      *core.TxPool
	underpriced mapset.Set // Transactions discarded as too cheap (don't re-fetch)
}

func NewServer(ctx context.Context, txPool *core.TxPool) *Server {
	return &Server{ctx: ctx, txPool: txPool, underpriced: mapset.NewSet()}
}

func (s *Server) FindUnknownTransactions(ctx context.Context, in *proto_txpool.TxHashes) (*proto_txpool.TxHashes, error) {
	reply := &proto_txpool.TxHashes{}
	for i := range in.Hashes {
		if s.txPool.Has(gointerfaces.ConvertH256ToHash(in.Hashes[i])) {
			continue
		}
		reply.Hashes = append(reply.Hashes, in.Hashes[i])
	}

	return reply, nil
}

func (s *Server) ImportTransactions(ctx context.Context, in *proto_txpool.ImportRequest) (*proto_txpool.ImportReply, error) {
	txs := make(types.Transactions, len(in.Txs))
	reply := &proto_txpool.ImportReply{Imported: make([]proto_txpool.ImportResult, len(in.Txs))}
	for i := range in.Txs {
		err := rlp.DecodeBytes(in.Txs[i], txs[i])
		if err != nil {
			return nil, err
		}
	}
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
				reply.Imported[i] = proto_txpool.ImportResult_ALREADY_EXISTS

			case core.ErrUnderpriced, core.ErrReplaceUnderpriced:
				reply.Imported[i] = proto_txpool.ImportResult_FEE_TOO_LOW

			case core.ErrInvalidSender, core.ErrGasLimit, core.ErrNegativeValue, core.ErrOversizedData:
				reply.Imported[i] = proto_txpool.ImportResult_INVALID

			default:
				reply.Imported[i] = proto_txpool.ImportResult_INTERNAL_ERROR
			}
		}
	}
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
	buf := bytes.NewBuffer(nil)
	result := make([][]byte, len(txs))
	for i := range txs {
		buf.Reset()
		if err := txs[i].EncodeRLP(buf); err != nil {
			return nil, err
		}
		result[i] = common.CopyBytes(buf.Bytes())
	}
	return result, nil
}

func UnmarshalTxs(txs [][]byte) (types.Transactions, error) {
	result := make(types.Transactions, len(txs))
	for i := range txs {
		if err := rlp.DecodeBytes(txs[i], result[i]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

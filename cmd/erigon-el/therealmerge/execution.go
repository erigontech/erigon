package therealmerge

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"math/big"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/turbo/services"
)

type Eth3Execution struct {
	execution.UnimplementedExecutionServer

	db          kv.RwDB
	blockReader services.FullBlockReader
	mu          sync.Mutex
}

func NewEth3Execution(db kv.RwDB, blockReader services.FullBlockReader) *Eth3Execution {
	return &Eth3Execution{
		db:          db,
		blockReader: blockReader,
	}
}

func (e *Eth3Execution) InsertHeaders(ctx context.Context, req *execution.InsertHeadersRequest) (*execution.EmptyMessage, error) {
	return nil, nil
}

func (e *Eth3Execution) InsertBodies(ctx context.Context, req *execution.InsertBodiesRequest) (*execution.EmptyMessage, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, body := range req.Bodies {
		uncles := make([]*types.Header, 0, len(body.Uncles))
		for _, uncle := range body.Uncles {
			h, err := HeaderRpcToHeader(uncle)
			if err != nil {
				return nil, err
			}
			uncles = append(uncles, h)
		}
		// Withdrawals processing
		if _, _, err := rawdb.WriteRawBodyIfNotExists(tx, gointerfaces.ConvertH256ToHash(body.BlockHash),
			body.BlockNumber, &types.RawBody{
				Transactions: body.Transactions,
				Uncles:       uncles,
				Withdrawals:  privateapi.ConvertWithdrawalsFromRpc(body.Withdrawals),
			}); err != nil {
			return nil, err
		}
	}
	return &execution.EmptyMessage{}, tx.Commit()
}

type canonicalEntry struct {
	hash   libcommon.Hash
	number uint64
}

func (e *Eth3Execution) UpdateForkChoice(ctx context.Context, hash *types2.H256) (*execution.ForkChoiceReceipt, error) {
	return nil, nil
}

func (e *Eth3Execution) GetHeader(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetHeaderResponse, error) {
	return nil, nil
}

func (e *Eth3Execution) GetBody(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetBodyResponse, error) {
	return nil, nil
}

func (e *Eth3Execution) IsCanonicalHash(ctx context.Context, req *types2.H256) (*execution.IsCanonicalResponse, error) {
	return nil, nil
}

func (e *Eth3Execution) GetHeaderHashNumber(ctx context.Context, req *types2.H256) (*execution.GetHeaderHashNumberResponse, error) {
	return nil, nil
}

func HeaderRpcToHeader(header *execution.Header) (*types.Header, error) {
	var blockNonce types.BlockNonce
	binary.BigEndian.PutUint64(blockNonce[:], header.Nonce)
	h := &types.Header{
		ParentHash:  gointerfaces.ConvertH256ToHash(header.ParentHash),
		UncleHash:   gointerfaces.ConvertH256ToHash(header.OmmerHash),
		Coinbase:    gointerfaces.ConvertH160toAddress(header.Coinbase),
		Root:        gointerfaces.ConvertH256ToHash(header.StateRoot),
		TxHash:      gointerfaces.ConvertH256ToHash(header.TransactionHash),
		ReceiptHash: gointerfaces.ConvertH256ToHash(header.ReceiptRoot),
		Bloom:       gointerfaces.ConvertH2048ToBloom(header.LogsBloom),
		Difficulty:  gointerfaces.ConvertH256ToUint256Int(header.Difficulty).ToBig(),
		Number:      big.NewInt(int64(header.BlockNumber)),
		GasLimit:    header.GasLimit,
		GasUsed:     header.GasUsed,
		Time:        header.Timestamp,
		Extra:       header.ExtraData,
		MixDigest:   gointerfaces.ConvertH256ToHash(header.MixDigest),
		Nonce:       blockNonce,
	}
	if header.BaseFeePerGas != nil {
		h.BaseFee = gointerfaces.ConvertH256ToUint256Int(header.BaseFeePerGas).ToBig()
	}
	if header.WithdrawalHash != nil {
		h.WithdrawalsHash = new(libcommon.Hash)
		*h.WithdrawalsHash = gointerfaces.ConvertH256ToHash(header.WithdrawalHash)
	}
	blockHash := gointerfaces.ConvertH256ToHash(header.BlockHash)
	if blockHash != h.Hash() {
		return nil, fmt.Errorf("block %d, %x has invalid hash. expected: %x", header.BlockNumber, h.Hash(), blockHash)
	}
	return h, nil
}

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
)

type Eth1Execution struct {
	execution.UnimplementedExecutionServer

	db kv.RwDB
	mu sync.Mutex
}

func NewEth1Execution(db kv.RwDB) *Eth1Execution {
	return &Eth1Execution{
		db: db,
	}
}

func (e *Eth1Execution) InsertHeaders(ctx context.Context, req *execution.InsertHeadersRequest) (*execution.EmptyMessage, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, header := range req.Headers {
		h, err := HeaderRpcToHeader(header)
		if err != nil {
			return nil, err
		}
		rawdb.WriteHeader(tx, h)
	}
	return &execution.EmptyMessage{}, tx.Commit()
}

func (e *Eth1Execution) InsertBodies(ctx context.Context, req *execution.InsertBodiesRequest) (*execution.EmptyMessage, error) {
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
		withdrawals := make([]*types.Withdrawal, 0, len(body.Withdrawals))
		for _, withdrawal := range body.Withdrawals {
			withdrawals = append(withdrawals, &types.Withdrawal{
				Index:     withdrawal.Index,
				Validator: withdrawal.ValidatorIndex,
				Address:   gointerfaces.ConvertH160toAddress(withdrawal.Address),
				Amount:    withdrawal.Amount,
			})
		}
		if _, _, err := rawdb.WriteRawBodyIfNotExists(tx, gointerfaces.ConvertH256ToHash(body.BlockHash),
			body.BlockNumber, &types.RawBody{
				Transactions: body.Transactions,
				Uncles:       uncles,
				Withdrawals:  withdrawals,
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

func (e *Eth1Execution) UpdateForkChoice(ctx context.Context, hash *types2.H256) (*execution.ForkChoiceReceipt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return &execution.ForkChoiceReceipt{
		LatestValidHash: hash,
		Success:         true,
	}, nil
}

func (e *Eth1Execution) GetHeader(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetHeaderResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve header
	var header *types.Header
	if req.BlockHash != nil && req.BlockNumber != nil {
		blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
		header = rawdb.ReadHeader(tx, blockHash, *req.BlockNumber)
	} else if req.BlockHash != nil {
		blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
		header, err = rawdb.ReadHeaderByHash(tx, blockHash)
	} else if req.BlockNumber != nil {
		header = rawdb.ReadHeaderByNumber(tx, *req.BlockNumber)
	}
	if err != nil {
		return nil, err
	}
	// Got nothing? return nothing :)
	if header == nil {
		return &execution.GetHeaderResponse{}, nil
	}

	return &execution.GetHeaderResponse{
		Header: HeaderToHeaderRPC(header),
	}, nil
}

func (e *Eth1Execution) GetBody(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetBodyResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Retrieve header
	var body *types.Body
	if req.BlockHash != nil && req.BlockNumber != nil {
		blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
		body = rawdb.ReadCanonicalBodyWithTransactions(tx, blockHash, *req.BlockNumber)
	} else if req.BlockHash != nil {
		blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
		blockNumber := rawdb.ReadHeaderNumber(tx, blockHash)
		if blockNumber == nil {
			return nil, nil
		}
		body = rawdb.ReadCanonicalBodyWithTransactions(tx, blockHash, *blockNumber)
	}
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	encodedTransactions, err := types.MarshalTransactionsBinary(body.Transactions)
	if err != nil {
		return nil, err
	}
	rpcWithdrawals := privateapi.ConvertWithdrawalsToRpc(body.Withdrawals)
	unclesRpc := make([]*execution.Header, 0, len(body.Uncles))
	for _, uncle := range body.Uncles {
		unclesRpc = append(unclesRpc, HeaderToHeaderRPC(uncle))
	}
	return &execution.GetBodyResponse{
		Body: &execution.BlockBody{
			Transactions: encodedTransactions,
			Withdrawals:  rpcWithdrawals,
			Uncles:       unclesRpc,
		},
	}, nil

}

func (e *Eth1Execution) IsCanonicalHash(ctx context.Context, req *types2.H256) (*execution.IsCanonicalResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return &execution.IsCanonicalResponse{Canonical: true}, nil
}

func (e *Eth1Execution) GetHeaderHashNumber(ctx context.Context, req *types2.H256) (*execution.GetHeaderHashNumberResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return &execution.GetHeaderHashNumberResponse{
		BlockNumber: rawdb.ReadHeaderNumber(tx, gointerfaces.ConvertH256ToHash(req)),
	}, nil
}

func HeaderRpcToHeader(header *execution.Header) (*types.Header, error) {
	var blockNonce types.BlockNonce
	binary.BigEndian.PutUint64(blockNonce[:], header.Nonce)
	var baseFee *big.Int
	var withdrawalHash *common.Hash
	if header.BaseFeePerGas != nil {
		baseFee = gointerfaces.ConvertH256ToUint256Int(header.BaseFeePerGas).ToBig()
	}
	if header.WithdrawalHash != nil {
		withdrawalHash = new(libcommon.Hash)
		*withdrawalHash = gointerfaces.ConvertH256ToHash(header.WithdrawalHash)
	}
	h := &types.Header{
		ParentHash:      gointerfaces.ConvertH256ToHash(header.ParentHash),
		UncleHash:       gointerfaces.ConvertH256ToHash(header.OmmerHash),
		Coinbase:        gointerfaces.ConvertH160toAddress(header.Coinbase),
		Root:            gointerfaces.ConvertH256ToHash(header.StateRoot),
		TxHash:          gointerfaces.ConvertH256ToHash(header.TransactionHash),
		ReceiptHash:     gointerfaces.ConvertH256ToHash(header.ReceiptRoot),
		Bloom:           gointerfaces.ConvertH2048ToBloom(header.LogsBloom),
		Difficulty:      gointerfaces.ConvertH256ToUint256Int(header.Difficulty).ToBig(),
		Number:          big.NewInt(int64(header.BlockNumber)),
		GasLimit:        header.GasLimit,
		GasUsed:         header.GasUsed,
		Time:            header.Timestamp,
		Extra:           header.ExtraData,
		MixDigest:       gointerfaces.ConvertH256ToHash(header.MixDigest),
		Nonce:           blockNonce,
		BaseFee:         baseFee,
		WithdrawalsHash: withdrawalHash,
	}

	blockHash := gointerfaces.ConvertH256ToHash(header.BlockHash)
	if blockHash != h.Hash() {
		return nil, fmt.Errorf("block %d, %x has invalid hash. expected: %x", header.BlockNumber, h.Hash(), blockHash)
	}
	return types.CopyHeader(h), nil
}

func HeaderToHeaderRPC(header *types.Header) *execution.Header {
	difficulty := new(uint256.Int)
	difficulty.SetFromBig(header.Difficulty)

	var baseFeeReply *types2.H256
	if header.BaseFee != nil {
		var baseFee uint256.Int
		baseFee.SetFromBig(header.BaseFee)
		baseFeeReply = gointerfaces.ConvertUint256IntToH256(&baseFee)
	}
	var withdrawalHashReply *types2.H256
	if header.WithdrawalsHash != nil {
		withdrawalHashReply = gointerfaces.ConvertHashToH256(*header.WithdrawalsHash)
	}
	return &execution.Header{
		ParentHash:      gointerfaces.ConvertHashToH256(header.ParentHash),
		Coinbase:        gointerfaces.ConvertAddressToH160(header.Coinbase),
		StateRoot:       gointerfaces.ConvertHashToH256(header.Root),
		TransactionHash: gointerfaces.ConvertHashToH256(header.TxHash),
		LogsBloom:       gointerfaces.ConvertBytesToH2048(header.Bloom[:]),
		ReceiptRoot:     gointerfaces.ConvertHashToH256(header.ReceiptHash),
		MixDigest:       gointerfaces.ConvertHashToH256(header.MixDigest),
		BlockNumber:     header.Number.Uint64(),
		Nonce:           header.Nonce.Uint64(),
		GasLimit:        header.GasLimit,
		GasUsed:         header.GasUsed,
		Timestamp:       header.Time,
		ExtraData:       header.Extra,
		Difficulty:      gointerfaces.ConvertUint256IntToH256(difficulty),
		BlockHash:       gointerfaces.ConvertHashToH256(header.Hash()),
		OmmerHash:       gointerfaces.ConvertHashToH256(header.UncleHash),
		BaseFeePerGas:   baseFeeReply,
		WithdrawalHash:  withdrawalHashReply,
	}

}

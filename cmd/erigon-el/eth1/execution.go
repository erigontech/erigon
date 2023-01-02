package eth1

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
)

type Eth1Execution struct {
	execution.UnimplementedExecutionServer

	db                kv.RwDB
	executionPipeline *stagedsync.Sync
}

func NewEth1Execution(db kv.RwDB, executionPipeline *stagedsync.Sync) *Eth1Execution {
	return &Eth1Execution{
		db:                db,
		executionPipeline: executionPipeline,
	}
}

func (e *Eth1Execution) InsertHeaders(ctx context.Context, req *execution.InsertHeadersRequest) (*execution.EmptyMessage, error) {
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
				Amount:    *gointerfaces.ConvertH256ToUint256Int(withdrawal.Amount),
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
	hash   common.Hash
	number uint64
}

func (e *Eth1Execution) UpdateForkChoice(ctx context.Context, hash *types2.H256) (*execution.ForkChoiceReceipt, error) {
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockHash := gointerfaces.ConvertH256ToHash(hash)
	// Step one, find reconnection point, and mark all of those headers as canonical.
	fcuHeader, err := rawdb.ReadHeaderByHash(tx, blockHash)
	if err != nil {
		return nil, err
	}
	// If we dont have it, too bad
	if fcuHeader == nil {
		return &execution.ForkChoiceReceipt{
			Success:         false,
			LatestValidHash: &types2.H256{},
		}, nil
	}
	currentParentHash := fcuHeader.ParentHash
	currentParentNumber := fcuHeader.Number.Uint64() - 1
	isCanonicalHash, err := rawdb.IsCanonicalHash(tx, currentParentHash)
	if err != nil {
		return nil, err
	}
	// Find such point, and collect all hashes
	newCanonicals := make([]*canonicalEntry, 0, 2048)
	newCanonicals = append(newCanonicals, &canonicalEntry{
		hash:   fcuHeader.Hash(),
		number: fcuHeader.Number.Uint64(),
	})
	for !isCanonicalHash {
		newCanonicals = append(newCanonicals, &canonicalEntry{
			hash:   currentParentHash,
			number: currentParentNumber,
		})
		currentHeader := rawdb.ReadHeader(tx, currentParentHash, currentParentNumber)
		if currentHeader == nil {
			return &execution.ForkChoiceReceipt{
				Success:         false,
				LatestValidHash: &types2.H256{},
			}, nil
		}
		currentParentHash = currentHeader.ParentHash
		currentParentNumber = currentHeader.Number.Uint64() - 1
		isCanonicalHash, err = rawdb.IsCanonicalHash(tx, currentParentHash)
		if err != nil {
			return nil, err
		}
	}
	e.executionPipeline.UnwindTo(currentParentNumber, common.Hash{})
	// Run the unwind
	if err := e.executionPipeline.RunUnwind(e.db, tx); err != nil {
		return nil, err
	}
	// Mark all new canonicals as canonicals
	for _, canonicalSegment := range newCanonicals {
		if err := rawdb.WriteCanonicalHash(tx, canonicalSegment.hash, canonicalSegment.number); err != nil {
			return nil, err
		}
	}
	// Set Progress for headers and bodies accordingly.
	if err := stages.SaveStageProgress(tx, stages.Headers, fcuHeader.Number.Uint64()); err != nil {
		return nil, err
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, fcuHeader.Number.Uint64()); err != nil {
		return nil, err
	}
	// Run the forkchoice
	if err := e.executionPipeline.Run(e.db, tx, false, false); err != nil {
		return nil, err
	}
	// if head hash was set then success otherwise no
	headHash := rawdb.ReadHeadBlockHash(tx)
	return &execution.ForkChoiceReceipt{
		LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		Success:         headHash == fcuHeader.Hash(),
	}, tx.Commit()
}

func (e *Eth1Execution) GetHeader(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetHeaderResponse, error) {
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
		if err != nil {
			return nil, err
		}
	} else if req.BlockNumber != nil {
		header = rawdb.ReadHeaderByNumber(tx, *req.BlockNumber)
	}
	// Got nothing? return nothing :)
	if header == nil {
		return nil, nil
	}

	return &execution.GetHeaderResponse{
		Header: HeaderToHeaderRPC(header),
	}, nil
}

func (e *Eth1Execution) GetBody(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetBodyResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Retrieve header
	var body *types.Body
	if req.BlockHash != nil && req.BlockNumber != nil {
		blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
		body, _, _ = rawdb.ReadBody(tx, blockHash, *req.BlockNumber)
	} else if req.BlockHash != nil {
		blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
		blockNumber := rawdb.ReadHeaderNumber(tx, blockHash)
		if blockNumber == nil {
			return nil, nil
		}
		body, _, _ = rawdb.ReadBody(tx, blockHash, *blockNumber)
	} else if req.BlockNumber != nil {
		body, _, _, err = rawdb.ReadBodyByNumber(tx, *req.BlockNumber)
		if err != nil {
			return nil, err
		}
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
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockHash := gointerfaces.ConvertH256ToHash(req)
	isCanonical, err := rawdb.IsCanonicalHash(tx, blockHash)
	if err != nil {
		return nil, err
	}
	return &execution.IsCanonicalResponse{Canonical: isCanonical}, nil
}

func (e *Eth1Execution) GetHeaderHashNumber(ctx context.Context, req *types2.H256) (*execution.GetHeaderHashNumberResponse, error) {
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
		h.WithdrawalsHash = new(common.Hash)
		*h.WithdrawalsHash = gointerfaces.ConvertH256ToHash(header.WithdrawalHash)
	}
	blockHash := gointerfaces.ConvertH256ToHash(header.BlockHash)
	if blockHash != h.Hash() {
		return nil, fmt.Errorf("block %d, %x has invalid hash. expected: %x", header.BlockNumber, h.Hash(), blockHash)
	}
	return h, nil
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

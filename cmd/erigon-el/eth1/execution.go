package eth1

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

type Eth1Execution struct {
	execution.UnimplementedExecutionServer

	db                kv.RwDB
	executionPipeline stagedsync.Sync
}

func NewEth1Execution(db kv.RwDB, executionPipeline stagedsync.Sync) *Eth1Execution {
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
		h := &types.Header{
			ParentHash:      gointerfaces.ConvertH256ToHash(header.ParentHash),
			UncleHash:       gointerfaces.ConvertH256ToHash(header.OmmerHash),
			Coinbase:        gointerfaces.ConvertH160toAddress(header.Coinbase),
			Root:            gointerfaces.ConvertH256ToHash(header.StateRoot),
			TxHash:          common.Hash{},
			ReceiptHash:     gointerfaces.ConvertH256ToHash(header.ReceiptRoot),
			Bloom:           gointerfaces.ConvertH2048ToBloom(header.LogsBloom),
			Difficulty:      big.NewInt(0),
			Number:          big.NewInt(int64(header.BlockNumber)),
			GasLimit:        header.GasLimit,
			GasUsed:         header.GasUsed,
			Time:            header.Timestamp,
			Extra:           header.ExtraData,
			MixDigest:       gointerfaces.ConvertH256ToHash(header.MixDigest),
			Nonce:           types.BlockNonce{},
			BaseFee:         gointerfaces.ConvertH256ToUint256Int(header.BaseFeePerGas).ToBig(),
			WithdrawalsHash: nil,
		}
		blockHash := gointerfaces.ConvertH256ToHash(header.BlockHash)
		if blockHash != h.Hash() {
			return nil, fmt.Errorf("Block %d, %x has invalid hash. expected: %x", header.BlockNumber, h.Hash(), blockHash)
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
		if _, _, err := rawdb.WriteRawBodyIfNotExists(tx, gointerfaces.ConvertH256ToHash(body.BlockHash),
			body.BlockNumber, &types.RawBody{
				Transactions: body.Transactions,
				Uncles:       body.Uncles, // TODO
				Withdrawals:  body.Withdrawals,
			}); err != nil {
			return nil, err
		}
	}
	return &execution.EmptyMessage{}, tx.Commit()
}

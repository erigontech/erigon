package eth1

import (
	"context"
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/kv"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
)

var errNotFound = errors.New("notfound")

func (e *EthereumExecutionModule) parseSegmentRequest(ctx context.Context, tx kv.Tx, req *execution.GetSegmentRequest) (blockHash libcommon.Hash, blockNumber uint64, err error) {
	switch {
	// Case 1: Only hash is given.
	case req.BlockHash != nil && req.BlockNumber == nil:
		blockHash = gointerfaces.ConvertH256ToHash(req.BlockHash)
		blockNumberPtr := rawdb.ReadHeaderNumber(tx, blockHash)
		if blockNumberPtr == nil {
			err = errNotFound
			return
		}
		blockNumber = *blockNumberPtr
	case req.BlockHash == nil && req.BlockNumber != nil:
		blockNumber = *req.BlockNumber
		blockHash, err = e.canonicalHash(ctx, tx, blockNumber)
		if err != nil {
			err = errNotFound
			return
		}
	case req.BlockHash != nil && req.BlockNumber != nil:
		blockHash = gointerfaces.ConvertH256ToHash(req.BlockHash)
		blockNumber = *req.BlockNumber
	}
	return
}

func (e *EthereumExecutionModule) GetBody(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetBodyResponse, error) {
	// Invalid case: request is invalid.
	if req == nil || (req.BlockHash == nil && req.BlockNumber == nil) {
		return nil, errors.New("ethereumExecutionModule.GetBody: bad request")
	}
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: could not open database: %s", err)
	}
	defer tx.Rollback()

	blockHash, blockNumber, err := e.parseSegmentRequest(ctx, tx, req)
	if err == errNotFound {
		return &execution.GetBodyResponse{Body: nil}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: %s", err)
	}
	td, err := rawdb.ReadTd(tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: %s", err)
	}
	if td == nil {
		return &execution.GetBodyResponse{Body: nil}, nil
	}
	body, err := e.getBody(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: coild not read body: %s", err)
	}
	if body == nil {
		return &execution.GetBodyResponse{Body: nil}, nil
	}
	rawBody := body.RawBody()

	return &execution.GetBodyResponse{Body: eth1_utils.ConvertRawBlockBodyToRpc(rawBody, blockNumber, blockHash)}, nil
}

func (e *EthereumExecutionModule) GetHeader(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetHeaderResponse, error) {
	// Invalid case: request is invalid.
	if req == nil || (req.BlockHash == nil && req.BlockNumber == nil) {
		return nil, errors.New("ethereumExecutionModule.GetHeader: bad request")
	}
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: could not open database: %s", err)
	}
	defer tx.Rollback()

	blockHash, blockNumber, err := e.parseSegmentRequest(ctx, tx, req)
	if err == errNotFound {
		return &execution.GetHeaderResponse{Header: nil}, nil
	}
	td, err := rawdb.ReadTd(tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: %s", err)
	}
	if td == nil {
		return &execution.GetHeaderResponse{Header: nil}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: %s", err)
	}
	header, err := e.getHeader(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: coild not read body: %s", err)
	}
	if header == nil {
		return &execution.GetHeaderResponse{Header: nil}, nil
	}

	return &execution.GetHeaderResponse{Header: eth1_utils.HeaderToHeaderRPC(header)}, nil
}

func (e *EthereumExecutionModule) GetBodiesByHashes(ctx context.Context, req *execution.GetBodiesByHashesRequest) (*execution.GetBodiesBatchResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bodies := make([]*execution.BlockBody, 0, len(req.Hashes))

	for _, hash := range req.Hashes {
		h := gointerfaces.ConvertH256ToHash(hash)
		number := rawdb.ReadHeaderNumber(tx, h)
		if number == nil {
			break
		}
		body, err := e.getBody(ctx, tx, h, *number)
		if err != nil {
			return nil, err
		}
		if body == nil {
			break
		}
		txs, err := types.MarshalTransactionsBinary(body.Transactions)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, &execution.BlockBody{
			Transactions: txs,
			Withdrawals:  eth1_utils.ConvertWithdrawalsToRpc(body.Withdrawals),
		})
	}

	return &execution.GetBodiesBatchResponse{Bodies: bodies}, nil
}

func (e *EthereumExecutionModule) GetBodiesByRange(ctx context.Context, req *execution.GetBodiesByRangeRequest) (*execution.GetBodiesBatchResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bodies := make([]*execution.BlockBody, 0, req.Count)

	for i := uint64(0); i < req.Count; i++ {
		hash, err := rawdb.ReadCanonicalHash(tx, req.Start+i)
		if err != nil {
			return nil, err
		}
		if hash == (libcommon.Hash{}) {
			// break early if beyond the last known canonical header
			break
		}

		body, err := e.getBody(ctx, tx, hash, req.Start+i)
		if err != nil {
			return nil, err
		}

		txs, err := types.MarshalTransactionsBinary(body.Transactions)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, &execution.BlockBody{
			Transactions: txs,
			Withdrawals:  eth1_utils.ConvertWithdrawalsToRpc(body.Withdrawals),
		})
	}

	return &execution.GetBodiesBatchResponse{
		Bodies: bodies,
	}, nil
}

func (e *EthereumExecutionModule) GetHeaderHashNumber(ctx context.Context, req *types2.H256) (*execution.GetHeaderHashNumberResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: could not open database: %s", err)
	}
	defer tx.Rollback()
	blockNumber := rawdb.ReadHeaderNumber(tx, gointerfaces.ConvertH256ToHash(req))
	if blockNumber == nil {
		return &execution.GetHeaderHashNumberResponse{BlockNumber: nil}, nil
	}
	return &execution.GetHeaderHashNumberResponse{BlockNumber: blockNumber}, nil
}

func (e *EthereumExecutionModule) isCanonicalHash(ctx context.Context, tx kv.Tx, hash libcommon.Hash) (bool, error) {
	blockNumber := rawdb.ReadHeaderNumber(tx, hash)
	if blockNumber == nil {
		return false, nil
	}
	expectedHash, err := e.canonicalHash(ctx, tx, *blockNumber)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.CanonicalHash: could not read canonical hash")
	}
	td, err := rawdb.ReadTd(tx, hash, *blockNumber)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.GetBody: %s", err)
	}
	if td == nil {
		return false, nil
	}
	return expectedHash == hash, nil
}

func (e *EthereumExecutionModule) IsCanonicalHash(ctx context.Context, req *types2.H256) (*execution.IsCanonicalResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CanonicalHash: could not open database: %s", err)
	}
	defer tx.Rollback()

	isCanonical, err := e.isCanonicalHash(ctx, tx, gointerfaces.ConvertH256ToHash(req))
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CanonicalHash: could not read canonical hash")
	}

	return &execution.IsCanonicalResponse{Canonical: isCanonical}, nil
}

func (e *EthereumExecutionModule) CurrentHeader(ctx context.Context, _ *emptypb.Empty) (*execution.GetHeaderResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: could not open database: %s", err)
	}
	defer tx.Rollback()
	hash := rawdb.ReadHeadHeaderHash(tx)
	number := rawdb.ReadHeaderNumber(tx, hash)
	h, _ := e.blockReader.Header(context.Background(), tx, hash, *number)
	return &execution.GetHeaderResponse{
		Header: eth1_utils.HeaderToHeaderRPC(h),
	}, nil
}

func (e *EthereumExecutionModule) GetTD(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetTDResponse, error) {
	// Invalid case: request is invalid.
	if req == nil || (req.BlockHash == nil && req.BlockNumber == nil) {
		return nil, errors.New("ethereumExecutionModule.GetHeader: bad request")
	}
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: could not open database: %s", err)
	}
	defer tx.Rollback()

	blockHash, blockNumber, err := e.parseSegmentRequest(ctx, tx, req)
	if err == errNotFound {
		return &execution.GetTDResponse{Td: nil}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: %s", err)
	}
	td, err := e.getTD(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: coild not read body: %s", err)
	}
	if td == nil {
		return &execution.GetTDResponse{Td: nil}, nil
	}

	return &execution.GetTDResponse{Td: eth1_utils.ConvertBigIntToRpc(td)}, nil
}

func (e *EthereumExecutionModule) GetForkChoice(ctx context.Context, _ *emptypb.Empty) (*execution.ForkChoice, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: could not open database: %s", err)
	}
	defer tx.Rollback()
	return &execution.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(rawdb.ReadForkchoiceHead(tx)),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(rawdb.ReadForkchoiceFinalized(tx)),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(rawdb.ReadForkchoiceSafe(tx)),
	}, nil
}

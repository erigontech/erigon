// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package eth1

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/eth1/eth1_utils"
	"github.com/erigontech/erigon/execution/types"
)

var errNotFound = errors.New("notfound")

func (e *EthereumExecutionModule) parseSegmentRequest(ctx context.Context, tx kv.Tx, req *execution.GetSegmentRequest) (blockHash common.Hash, blockNumber uint64, err error) {
	switch {
	// Case 1: Only hash is given.
	case req.BlockHash != nil && req.BlockNumber == nil:
		blockHash = gointerfaces.ConvertH256ToHash(req.BlockHash)
		var blockNumberPtr *uint64
		blockNumberPtr, err = e.blockReader.HeaderNumber(ctx, tx, blockHash)
		if err != nil {
			return common.Hash{}, 0, err
		}
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
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: could not begin database tx %w", err)
	}
	defer tx.Rollback()

	blockHash, blockNumber, err := e.parseSegmentRequest(ctx, tx, req)
	if errors.Is(err, errNotFound) {
		return &execution.GetBodyResponse{Body: nil}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: parseSegmentRequest error %w", err)
	}
	body, err := e.getBody(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: getBody error %w", err)
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
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: could not begin database tx %w", err)
	}
	defer tx.Rollback()

	blockHash, blockNumber, err := e.parseSegmentRequest(ctx, tx, req)
	if errors.Is(err, errNotFound) {
		return &execution.GetHeaderResponse{Header: nil}, nil
	}

	header, err := e.getHeader(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: getHeader error %w", err)
	}
	if header == nil {
		return &execution.GetHeaderResponse{Header: nil}, nil
	}

	return &execution.GetHeaderResponse{Header: eth1_utils.HeaderToHeaderRPC(header)}, nil
}

func (e *EthereumExecutionModule) GetBodiesByHashes(ctx context.Context, req *execution.GetBodiesByHashesRequest) (*execution.GetBodiesBatchResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: could not begin database tx %w", err)
	}
	defer tx.Rollback()

	bodies := make([]*execution.BlockBody, 0, len(req.Hashes))

	for _, hash := range req.Hashes {
		h := gointerfaces.ConvertH256ToHash(hash)
		number, err := e.blockReader.HeaderNumber(ctx, tx, h)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: HeaderNumber error %w", err)
		}
		if number == nil {
			bodies = append(bodies, nil)
			continue
		}
		body, err := e.getBody(ctx, tx, h, *number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: getBody error %w", err)
		}
		if body == nil {
			bodies = append(bodies, nil)
			continue
		}
		txs, err := types.MarshalTransactionsBinary(body.Transactions)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: MarshalTransactionsBinary error %w", err)
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
		return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: could not begin database tx %w", err)
	}
	defer tx.Rollback()

	bodies := make([]*execution.BlockBody, 0, req.Count)

	for i := uint64(0); i < req.Count; i++ {
		hash, err := e.canonicalHash(ctx, tx, req.Start+i)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: ReadCanonicalHash error %w", err)
		}
		if hash == (common.Hash{}) {
			// break early if beyond the last known canonical header
			break
		}

		body, err := e.getBody(ctx, tx, hash, req.Start+i)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: getBody error %w", err)
		}
		if body == nil {
			// Append nil and no further processing
			bodies = append(bodies, nil)
			continue
		}

		txs, err := types.MarshalTransactionsBinary(body.Transactions)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: MarshalTransactionsBinary error %w", err)
		}

		bodies = append(bodies, &execution.BlockBody{
			Transactions: txs,
			Withdrawals:  eth1_utils.ConvertWithdrawalsToRpc(body.Withdrawals),
		})
	}
	// Remove trailing nil values as per spec
	// See point 4 in https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#specification-4
	for i := len(bodies) - 1; i >= 0; i-- {
		if bodies[i] == nil {
			bodies = bodies[:i]
		}
	}

	return &execution.GetBodiesBatchResponse{
		Bodies: bodies,
	}, nil
}

func (e *EthereumExecutionModule) GetHeaderHashNumber(ctx context.Context, req *types2.H256) (*execution.GetHeaderHashNumberResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeaderHashNumber: could not begin database tx %w", err)
	}
	defer tx.Rollback()

	blockNumber, err := e.blockReader.HeaderNumber(ctx, tx, gointerfaces.ConvertH256ToHash(req))
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeaderHashNumber: HeaderNumber error %w", err)
	}
	return &execution.GetHeaderHashNumberResponse{BlockNumber: blockNumber}, nil
}

func (e *EthereumExecutionModule) isCanonicalHash(ctx context.Context, tx kv.Tx, hash common.Hash) (bool, error) {
	blockNumber, err := e.blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: HeaderNumber error %w", err)
	}
	if blockNumber == nil {
		return false, nil
	}

	expectedHash, err := e.canonicalHash(ctx, tx, *blockNumber)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: could not read canonical hash %w", err)
	}
	td, err := rawdb.ReadTd(tx, hash, *blockNumber)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: ReadTd error %w", err)
	}
	if td == nil {
		return false, nil
	}
	return expectedHash == hash, nil
}

func (e *EthereumExecutionModule) IsCanonicalHash(ctx context.Context, req *types2.H256) (*execution.IsCanonicalResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CanonicalHash: could not begin database tx %w", err)
	}
	defer tx.Rollback()

	isCanonical, err := e.isCanonicalHash(ctx, tx, gointerfaces.ConvertH256ToHash(req))
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CanonicalHash: could not read canonical hash %w", err)
	}

	return &execution.IsCanonicalResponse{Canonical: isCanonical}, nil
}

func (e *EthereumExecutionModule) CurrentHeader(ctx context.Context, _ *emptypb.Empty) (*execution.GetHeaderResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: could not begin database tx %w", err)
	}
	defer tx.Rollback()
	hash := rawdb.ReadHeadHeaderHash(tx)
	number, err := e.blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: blockReader.HeaderNumber error %w", err)
	}
	if number == nil {
		return nil, errors.New("ethereumExecutionModule.CurrentHeader: blockReader.HeaderNumber returned nil - probabably node not synced yet")
	}
	h, err := e.blockReader.Header(ctx, tx, hash, *number)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: blockReader.Header error %w", err)
	}
	if h == nil {
		return nil, errors.New("ethereumExecutionModule.CurrentHeader: no current header yet - probabably node not synced yet")
	}
	return &execution.GetHeaderResponse{
		Header: eth1_utils.HeaderToHeaderRPC(h),
	}, nil
}

func (e *EthereumExecutionModule) GetTD(ctx context.Context, req *execution.GetSegmentRequest) (*execution.GetTDResponse, error) {
	// Invalid case: request is invalid.
	if req == nil || (req.BlockHash == nil && req.BlockNumber == nil) {
		return nil, errors.New("ethereumExecutionModule.GetTD: bad request")
	}
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetTD: could not begin database tx %w", err)
	}
	defer tx.Rollback()

	blockHash, blockNumber, err := e.parseSegmentRequest(ctx, tx, req)
	if errors.Is(err, errNotFound) {
		return &execution.GetTDResponse{Td: nil}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetTD: parseSegmentRequest error %w", err)
	}
	td, err := e.getTD(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetTD: getTD error %w", err)
	}
	if td == nil {
		return &execution.GetTDResponse{Td: nil}, nil
	}

	return &execution.GetTDResponse{Td: eth1_utils.ConvertBigIntToRpc(td)}, nil
}

func (e *EthereumExecutionModule) GetForkChoice(ctx context.Context, _ *emptypb.Empty) (*execution.ForkChoice, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetForkChoice: could not begin database tx %w", err)
	}
	defer tx.Rollback()
	return &execution.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(rawdb.ReadForkchoiceHead(tx)),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(rawdb.ReadForkchoiceFinalized(tx)),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(rawdb.ReadForkchoiceSafe(tx)),
	}, nil
}

func (e *EthereumExecutionModule) FrozenBlocks(ctx context.Context, _ *emptypb.Empty) (*execution.FrozenBlocksResponse, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetForkChoice: could not begin database tx %w", err)
	}
	defer tx.Rollback()

	firstNonGenesisBlockNumber, ok, err := rawdb.ReadFirstNonGenesisHeaderNumber(tx)
	if err != nil {
		return nil, err
	}
	gap := false
	if ok {
		gap = e.blockReader.Snapshots().SegmentsMax()+1 < firstNonGenesisBlockNumber
	}
	return &execution.FrozenBlocksResponse{
		FrozenBlocks: e.blockReader.FrozenBlocks(),
		HasGap:       gap,
	}, nil
}

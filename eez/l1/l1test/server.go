// Copyright 2026 The Erigon Authors
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

package l1test

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// NewServer serves r over the JSON-RPC calls the rpc reader makes.
func NewServer(r l1.ChainReader, logger log.Logger) (*rpc.Server, error) {
	s := rpc.NewServer(1, false, false, true, logger, 0)
	if err := s.RegisterName("eth", &ethService{r: r}); err != nil {
		return nil, err
	}
	return s, nil
}

type ethService struct{ r l1.ChainReader }

type rpcHeader struct {
	Number     hexutil.Uint64 `json:"number"`
	Hash       common.Hash    `json:"hash"`
	ParentHash common.Hash    `json:"parentHash"`
	Timestamp  hexutil.Uint64 `json:"timestamp"`
}

func header(h *l1.Header, err error) (*rpcHeader, error) {
	if errors.Is(err, l1.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rpcHeader{Number: hexutil.Uint64(h.Number), Hash: h.Hash, ParentHash: h.ParentHash, Timestamp: hexutil.Uint64(h.Time)}, nil
}

func (s *ethService) ChainId(ctx context.Context) (hexutil.Uint64, error) { //nolint:revive
	id, err := s.r.ChainID(ctx)
	return hexutil.Uint64(id), err
}

func (s *ethService) GetBlockByNumber(ctx context.Context, n rpc.BlockNumber, _ bool) (*rpcHeader, error) {
	switch n {
	case rpc.LatestBlockNumber:
		return header(s.r.Head(ctx))
	case rpc.FinalizedBlockNumber:
		h, err := s.r.Finalized(ctx)
		if errors.Is(err, l1.ErrNotFound) {
			return nil, errors.New("finalized block not found")
		}
		return header(h, err)
	}
	return header(s.r.HeaderByNumber(ctx, uint64(n)))
}

func (s *ethService) GetBlockByHash(ctx context.Context, hash common.Hash, _ bool) (*rpcHeader, error) {
	return header(s.r.HeaderByHash(ctx, hash))
}

type filter struct {
	FromBlock hexutil.Uint64  `json:"fromBlock"`
	ToBlock   hexutil.Uint64  `json:"toBlock"`
	Address   common.Address  `json:"address"`
	Topics    [][]common.Hash `json:"topics"`
}

func (s *ethService) GetLogs(ctx context.Context, f filter) ([]*types.Log, error) {
	logs, err := s.r.Logs(ctx, uint64(f.FromBlock), uint64(f.ToBlock), f.Address, f.Topics)
	if errors.Is(err, l1.ErrRangeTooWide) {
		return nil, errors.New("block range too large")
	}
	if logs == nil && err == nil {
		logs = []*types.Log{}
	}
	return logs, err
}

func (s *ethService) GetTransactionByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint64) (*ethapi.RPCTransaction, error) {
	tx, err := s.r.TransactionByBlockHashAndIndex(ctx, hash, uint64(index))
	if errors.Is(err, l1.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	h, err := s.r.HeaderByHash(ctx, hash)
	if err != nil {
		return nil, err
	}
	return ethapi.NewRPCTransaction(tx, hash, h.Time, h.Number, uint64(index), nil), nil
}

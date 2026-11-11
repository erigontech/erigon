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
	"fmt"

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
	MixHash    common.Hash    `json:"mixHash"`
}

func header(h *l1.Header, err error) (*rpcHeader, error) {
	if errors.Is(err, l1.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rpcHeader{Number: hexutil.Uint64(h.Number), Hash: h.Hash, ParentHash: h.ParentHash, Timestamp: hexutil.Uint64(h.Time), MixHash: h.MixDigest}, nil
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

func (s *ethService) GetCode(ctx context.Context, addr common.Address, at rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	sr, hash, err := s.stateAt(at)
	if err != nil {
		return nil, err
	}
	code, err := sr.CodeAt(ctx, addr, hash)
	if err != nil {
		return nil, stateError(hash, err)
	}
	return append(hexutil.Bytes{}, code...), nil
}

func (s *ethService) Call(ctx context.Context, args ethapi.CallArgs, at rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	sr, hash, err := s.stateAt(at)
	if err != nil {
		return nil, err
	}
	if args.To == nil {
		return nil, errors.New("call without a recipient")
	}
	call := l1.Call{To: *args.To}
	if args.Input != nil {
		call.Data = *args.Input
	} else if args.Data != nil {
		call.Data = *args.Data
	}
	out, err := sr.CallAt(ctx, call, hash)
	if err != nil {
		return nil, stateError(hash, err)
	}
	return append(hexutil.Bytes{}, out...), nil
}

// stateAt serves state reads only at a block hash required canonical, the
// one form the rpc reader sends.
func (s *ethService) stateAt(at rpc.BlockNumberOrHash) (l1.StateReader, common.Hash, error) {
	sr, ok := s.r.(l1.StateReader)
	if !ok {
		return nil, common.Hash{}, errors.New("state reads not served")
	}
	hash, ok := at.Hash()
	if !ok || !at.RequireCanonical {
		return nil, common.Hash{}, fmt.Errorf("state read at %s, not at a block hash required canonical", at.String())
	}
	return sr, hash, nil
}

// revertError is an execution revert as a JSON-RPC error.
type revertError struct{ err error }

func (e revertError) Error() string  { return "execution reverted: " + e.err.Error() }
func (e revertError) ErrorCode() int { return 3 }

// stateError words a refused state read as an Erigon L1 node does.
func stateError(hash common.Hash, err error) error {
	switch {
	case errors.Is(err, l1.ErrReverted):
		return revertError{err}
	case errors.Is(err, l1.ErrNotCanonical):
		return fmt.Errorf("hash %x is not currently canonical", hash)
	case errors.Is(err, l1.ErrNotFound):
		return rpc.BlockNotFoundErr{BlockId: hash.String()}
	}
	return err
}

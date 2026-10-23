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

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

// ErrNoBlock reports a block a source does not serve.
var ErrNoBlock = errors.New("block not served")

// Source serves an L2 chain's blocks as eth_getBlockByNumber renders them,
// transactions by hash.
type Source interface {
	Block(ctx context.Context, number uint64) (map[string]json.RawMessage, error)
}

// RPCSource is a Source over JSON-RPC.
type RPCSource struct{ client *rpc.Client }

// DialSource connects to the JSON-RPC endpoint at url.
func DialSource(ctx context.Context, url string, logger log.Logger) (*RPCSource, error) {
	c, err := rpc.DialContext(ctx, url, logger)
	if err != nil {
		return nil, err
	}
	return &RPCSource{client: c}, nil
}

// Close releases the connection.
func (s *RPCSource) Close() { s.client.Close() }

func (s *RPCSource) Block(ctx context.Context, number uint64) (map[string]json.RawMessage, error) {
	var b map[string]json.RawMessage
	if err := s.client.CallContext(ctx, &b, "eth_getBlockByNumber", hexutil.Uint64(number), false); err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("%w: %d", ErrNoBlock, number)
	}
	return b, nil
}

// Difference is the first block two chains disagree on.
type Difference struct {
	Block uint64
	// Fields are the fields both sources render and that differ.
	Fields []string
	A, B   map[string]json.RawMessage
}

// Diff returns the first block in [from, to] whose hash differs between a
// and b, nil when every one agrees. A block hash commits to its ancestors,
// so the search halves the range.
func Diff(ctx context.Context, a, b Source, from, to uint64) (*Difference, error) {
	if from > to {
		return nil, fmt.Errorf("empty range [%d, %d]", from, to)
	}
	same := func(n uint64) (bool, map[string]json.RawMessage, map[string]json.RawMessage, error) {
		ba, err := a.Block(ctx, n)
		if err != nil {
			return false, nil, nil, err
		}
		bb, err := b.Block(ctx, n)
		if err != nil {
			return false, nil, nil, err
		}
		var ha, hb common.Hash
		if err := json.Unmarshal(ba["hash"], &ha); err != nil {
			return false, nil, nil, fmt.Errorf("block %d hash: %w", n, err)
		}
		if err := json.Unmarshal(bb["hash"], &hb); err != nil {
			return false, nil, nil, fmt.Errorf("block %d hash: %w", n, err)
		}
		return ha == hb, ba, bb, nil
	}
	if ok, _, _, err := same(to); err != nil || ok {
		return nil, err
	}
	lo, hi := from, to
	for lo < hi {
		mid := lo + (hi-lo)/2
		ok, _, _, err := same(mid)
		if err != nil {
			return nil, err
		}
		if ok {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	_, ba, bb, err := same(lo)
	if err != nil {
		return nil, err
	}
	d := &Difference{Block: lo, A: ba, B: bb}
	for _, k := range slices.Sorted(maps.Keys(ba)) {
		if v, ok := bb[k]; ok && string(v) != string(ba[k]) {
			d.Fields = append(d.Fields, k)
		}
	}
	return d, nil
}

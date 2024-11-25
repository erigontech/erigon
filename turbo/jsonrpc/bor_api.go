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

package jsonrpc

import (
	"context"
	"fmt"
	"reflect"

	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/rpc"
)

// BorAPI Bor specific routines
type BorAPI interface {
	// Bor snapshot related (see ./bor_snapshot.go)
	GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error)
	GetAuthor(blockNrOrHash *rpc.BlockNumberOrHash) (*common.Address, error)
	GetSnapshotAtHash(hash common.Hash) (*Snapshot, error)
	GetSigners(number *rpc.BlockNumber) ([]common.Address, error)
	GetSignersAtHash(hash common.Hash) ([]common.Address, error)
	GetCurrentProposer() (common.Address, error)
	GetCurrentValidators() ([]*valset.Validator, error)
	GetSnapshotProposer(blockNrOrHash *rpc.BlockNumberOrHash) (common.Address, error)
	GetSnapshotProposerSequence(blockNrOrHash *rpc.BlockNumberOrHash) (BlockSigners, error)
	GetRootHash(start uint64, end uint64) (string, error)
}

type spanProducersReader interface {
	Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error)
}

// BorImpl is implementation of the BorAPI interface
type BorImpl struct {
	*BaseAPI
	db                     kv.RoDB // the chain db
	useSpanProducersReader bool
	spanProducersReader    spanProducersReader
}

// NewBorAPI returns BorImpl instance
func NewBorAPI(base *BaseAPI, db kv.RoDB, spanProducersReader spanProducersReader) *BorImpl {
	return &BorImpl{
		BaseAPI:                base,
		db:                     db,
		useSpanProducersReader: spanProducersReader != nil && !reflect.ValueOf(spanProducersReader).IsNil(), // needed for interface nil caveat
		spanProducersReader:    spanProducersReader,
	}
}

func (api *BorImpl) bor() (*bor.Bor, error) {
	type lazy interface {
		HasEngine() bool
		Engine() consensus.EngineReader
	}

	switch engine := api.engine().(type) {
	case *bor.Bor:
		return engine, nil
	case lazy:
		if engine.HasEngine() {
			if bor, ok := engine.Engine().(*bor.Bor); ok {
				return bor, nil
			}
		}
	}

	return nil, fmt.Errorf("unknown or invalid consensus engine: %T", api.engine())
}

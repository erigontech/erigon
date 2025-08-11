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

package merge

import (
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
)

type readerMock struct{}

func (r readerMock) Config() *chain.Config {
	return nil
}

func (r readerMock) CurrentHeader() *types.Header {
	return nil
}

func (cr readerMock) CurrentFinalizedHeader() *types.Header {
	return nil
}

func (cr readerMock) CurrentSafeHeader() *types.Header {
	return nil
}

func (r readerMock) GetHeader(common.Hash, uint64) *types.Header {
	return nil
}

func (r readerMock) GetHeaderByNumber(uint64) *types.Header {
	return nil
}

func (r readerMock) GetHeaderByHash(common.Hash) *types.Header {
	return nil
}

func (r readerMock) GetTd(common.Hash, uint64) *big.Int {
	return nil
}

func (r readerMock) FrozenBlocks() uint64 {
	return 0
}
func (r readerMock) FrozenBorBlocks(align bool) uint64 { return 0 }

// The thing only that changes between normal ethash checks other than POW, is difficulty
// and nonce so we are gonna test those
func TestVerifyHeaderDifficulty(t *testing.T) {
	header := &types.Header{
		Difficulty: big.NewInt(1),
		Time:       1,
	}

	parent := &types.Header{}

	var eth1Engine consensus.Engine
	mergeEngine := New(eth1Engine)

	err := mergeEngine.verifyHeader(readerMock{}, header, parent)
	if err != errInvalidDifficulty {
		if err != nil {
			t.Fatalf("Merge engine should not accept non-zero difficulty, got %s", err.Error())
		} else {
			t.Fatalf("Merge engine should not accept non-zero difficulty")
		}
	}
}

func TestVerifyHeaderNonce(t *testing.T) {
	header := &types.Header{
		Nonce:      types.BlockNonce{1, 0, 0, 0, 0, 0, 0, 0},
		Difficulty: big.NewInt(0),
		Time:       1,
	}

	parent := &types.Header{}

	var eth1Engine consensus.Engine
	mergeEngine := New(eth1Engine)

	err := mergeEngine.verifyHeader(readerMock{}, header, parent)
	if err != errInvalidNonce {
		if err != nil {
			t.Fatalf("Merge engine should not accept non-zero difficulty, got %s", err.Error())
		} else {
			t.Fatalf("Merge engine should not accept non-zero difficulty")
		}
	}
}

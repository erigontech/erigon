// Copyright 2018 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package ethash

import (
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// Tests whether stale solutions are correctly processed.
func TestStaleSubmission(t *testing.T) {
	ethash := NewTester(true)
	defer ethash.Close()
	api := &API{ethash}

	fakeNonce, fakeDigest := types.BlockNonce{0x01, 0x02, 0x03}, common.HexToHash("deadbeef")

	testcases := []struct {
		headers     []*types.Header
		submitIndex int
		submitRes   bool
	}{
		// Case1: submit solution for the latest mining package
		{
			[]*types.Header{
				{ParentHash: common.BytesToHash([]byte{0xa}), Number: big.NewInt(1), Difficulty: big.NewInt(100000000)},
			},
			0,
			true,
		},
		// Case2: submit solution for the previous package but have same parent.
		{
			[]*types.Header{
				{ParentHash: common.BytesToHash([]byte{0xb}), Number: big.NewInt(2), Difficulty: big.NewInt(100000000)},
				{ParentHash: common.BytesToHash([]byte{0xb}), Number: big.NewInt(2), Difficulty: big.NewInt(100000001)},
			},
			0,
			true,
		},
		// Case3: submit stale but acceptable solution
		{
			[]*types.Header{
				{ParentHash: common.BytesToHash([]byte{0xc}), Number: big.NewInt(3), Difficulty: big.NewInt(100000000)},
				{ParentHash: common.BytesToHash([]byte{0xd}), Number: big.NewInt(9), Difficulty: big.NewInt(100000000)},
			},
			0,
			true,
		},
		// Case4: submit very old solution
		{
			[]*types.Header{
				{ParentHash: common.BytesToHash([]byte{0xe}), Number: big.NewInt(10), Difficulty: big.NewInt(100000000)},
				{ParentHash: common.BytesToHash([]byte{0xf}), Number: big.NewInt(17), Difficulty: big.NewInt(100000000)},
			},
			0,
			false,
		},
	}
	results := make(chan *types.BlockWithReceipts, 16)

	for id, c := range testcases {
		for _, h := range c.headers {
			blockWithReceipts := &types.BlockWithReceipts{Block: types.NewBlockWithHeader(h)}
			err := ethash.Seal(nil, blockWithReceipts, results, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		if res := api.SubmitWork(fakeNonce, ethash.SealHash(c.headers[c.submitIndex]), fakeDigest); res != c.submitRes {
			t.Errorf("case %d submit result mismatch, want %t, get %t", id+1, c.submitRes, res)
		}
		if !c.submitRes {
			continue
		}
		select {
		case resWithReceipts := <-results:
			res := resWithReceipts.Block
			if res.Nonce() != fakeNonce {
				t.Errorf("case %d block nonce mismatch, want %x, get %x", id+1, fakeNonce, res.Nonce())
			}
			if res.MixDigest() != fakeDigest {
				t.Errorf("case %d block digest mismatch, want %x, get %x", id+1, fakeDigest, res.MixDigest())
			}
			if res.Difficulty().Uint64() != c.headers[c.submitIndex].Difficulty.Uint64() {
				t.Errorf("case %d block difficulty mismatch, want %d, get %d", id+1, c.headers[c.submitIndex].Difficulty, res.Difficulty())
			}
			if res.NumberU64() != c.headers[c.submitIndex].Number.Uint64() {
				t.Errorf("case %d block number mismatch, want %d, get %d", id+1, c.headers[c.submitIndex].Number.Uint64(), res.NumberU64())
			}
			if res.ParentHash() != c.headers[c.submitIndex].ParentHash {
				t.Errorf("case %d block parent hash mismatch, want %s, get %s", id+1, c.headers[c.submitIndex].ParentHash.Hex(), res.ParentHash().Hex())
			}
		case <-time.NewTimer(time.Second).C:
			t.Errorf("case %d fetch ethash result timeout", id+1)
		}
	}
}

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

package headerdownload_test

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
)

func TestSideChainInsert(t *testing.T) {
	t.Parallel()
	funds := big.NewInt(1000000000)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.AllProtocolChanges
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	m := mock.MockWithGenesis(t, gspec, key, false)
	db := m.DB
	genesis := m.Genesis
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	br := m.BlockReader
	hi := headerdownload.NewHeaderInserter("headers", big.NewInt(0), 0, br)

	// Chain with higher initial difficulty
	chain1 := createTestChain(3, genesis.Hash(), 2, []byte(""))

	// Smaller side chain (non-canonical)
	chain2 := createTestChain(5, genesis.Hash(), 1, []byte("side1"))

	// Bigger side chain (canonical)
	chain3 := createTestChain(7, genesis.Hash(), 1, []byte("side2"))

	// Again smaller side chain but with high difficulty (canonical)
	chain4 := createTestChain(5, genesis.Hash(), 2, []byte("side3"))

	// More smaller side chain with same difficulty (canonical)
	chain5 := createTestChain(2, genesis.Hash(), 5, []byte("side5"))

	// Bigger side chain with same difficulty (non-canonical)
	chain6 := createTestChain(10, genesis.Hash(), 1, []byte("side6"))

	// Same side chain (in terms of number and difficulty) but different hash
	chain7 := createTestChain(2, genesis.Hash(), 5, []byte("side7"))

	finalExpectedHash := chain5[len(chain5)-1].Hash()
	if bytes.Compare(chain5[len(chain5)-1].Hash().Bytes(), chain7[len(chain7)-1].Hash().Bytes()) < 0 {
		finalExpectedHash = chain7[len(chain7)-1].Hash()
	}

	testCases := []struct {
		name         string
		chain        []*types.Header
		expectedHash common.Hash
		expectedDiff int64
	}{
		{"normal initial insert", chain1, chain1[len(chain1)-1].Hash(), 6},
		{"td(current) > td(incoming)", chain2, chain1[len(chain1)-1].Hash(), 6},
		{"td(incoming) > td(current), number(incoming) > number(current)", chain3, chain3[len(chain3)-1].Hash(), 7},
		{"td(incoming) > td(current), number(current) > number(incoming)", chain4, chain4[len(chain4)-1].Hash(), 10},
		{"td(incoming) = td(current), number(current) > number(current)", chain5, chain5[len(chain5)-1].Hash(), 10},
		{"td(incoming) = td(current), number(incoming) > number(current)", chain6, chain5[len(chain5)-1].Hash(), 10},
		{"td(incoming) = td(current), number(incoming) = number(current), hash different", chain7, finalExpectedHash, 10},
	}

	for _, tc := range testCases {
		tc := tc
		for i, h := range tc.chain {
			data, _ := rlp.EncodeToBytes(h)
			if _, err = hi.FeedHeaderPoW(tx, br, h, data, h.Hash(), uint64(i+1)); err != nil {
				t.Errorf("feed empty header for %s, err: %v", tc.name, err)
			}
		}

		if hi.GetHighestHash() != tc.expectedHash {
			t.Errorf("incorrect highest hash for %s, expected %s, got %s", tc.name, tc.expectedHash, hi.GetHighestHash())
		}
		if hi.GetLocalTd().Int64() != tc.expectedDiff {
			t.Errorf("incorrect difficulty for %s, expected %d, got %d", tc.name, tc.expectedDiff, hi.GetLocalTd().Int64())
		}
	}
}

func createTestChain(length int64, parent common.Hash, diff int64, extra []byte) []*types.Header {
	var (
		i       int64
		headers []*types.Header
	)

	for i = 0; i < length; i++ {
		h := &types.Header{
			Number:     big.NewInt(i + 1),
			Difficulty: big.NewInt(diff),
			ParentHash: parent,
			Extra:      extra,
		}
		headers = append(headers, h)
		parent = h.Hash()
	}

	return headers
}

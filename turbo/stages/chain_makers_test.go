// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package stages_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"

	"github.com/ledgerwatch/erigon/turbo/stages"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

func TestGenerateChain(t *testing.T) {
	var (
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		key3, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		addr2   = crypto.PubkeyToAddress(key2.PublicKey)
		addr3   = crypto.PubkeyToAddress(key3.PublicKey)
	)

	h := log.Root().GetHandler()
	defer func() {
		log.Root().SetHandler(h)
	}()
	log.Root().SetHandler(log.DiscardHandler())

	// Ensure that key1 has some funds in the genesis block.
	gspec := &core.Genesis{
		Config: &chain.Config{HomesteadBlock: new(big.Int), ChainID: big.NewInt(1)},
		Alloc:  core.GenesisAlloc{addr1: {Balance: big.NewInt(1000000)}},
	}
	m := stages.MockWithGenesis(t, gspec, key1, false)

	// This call generates a chain of 5 blocks. The function runs for
	// each block and adds different features to gen based on the
	// block index.
	signer := types.LatestSignerForChainID(nil)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, addr1 sends addr2 some ether.
			tx, _ := types.SignTx(types.NewTransaction(gen.TxNonce(addr1), addr2, uint256.NewInt(10000), params.TxGas, nil, nil), *signer, key1)
			gen.AddTx(tx)
		case 1:
			// In block 2, addr1 sends some more ether to addr2.
			// addr2 passes it on to addr3.
			tx1, _ := types.SignTx(types.NewTransaction(gen.TxNonce(addr1), addr2, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key1)
			tx2, _ := types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr3, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key2)
			gen.AddTx(tx1)
			gen.AddTx(tx2)
		case 2:
			// Block 3 is empty but was mined by addr3.
			gen.SetCoinbase(addr3)
			gen.SetExtra([]byte("yeehaw"))
		case 3:
			// Block 4 includes blocks 2 and 3 as uncle headers (with modified extra data).
			b2 := gen.PrevBlock(1).Header()
			b2.Extra = []byte("foo")
			gen.AddUncle(b2)
			b3 := gen.PrevBlock(2).Header()
			b3.Extra = []byte("foo")
			gen.AddUncle(b3)
		}
	}, false /* intermediateHashes */)
	if err != nil {
		fmt.Printf("generate chain: %v\n", err)
	}

	// Import the chain. This runs all block validation rules.
	if err := m.InsertChain(chain); err != nil {
		fmt.Printf("insert error%v\n", err)
		return
	}

	tx, err := m.DB.BeginRo(m.Ctx)
	if err != nil {
		fmt.Printf("beginro error: %v\n", err)
		return
	}
	defer tx.Rollback()

	st := state.New(state.NewPlainStateReader(tx))
	if big.NewInt(5).Cmp(current(m.DB).Number()) != 0 {
		t.Errorf("wrong block number: %d", current(m.DB).Number())
	}
	if !uint256.NewInt(989000).Eq(st.GetBalance(addr1)) {
		t.Errorf("wrong balance of addr1: %s", st.GetBalance(addr1))
	}
	if !uint256.NewInt(10000).Eq(st.GetBalance(addr2)) {
		t.Errorf("wrong balance of addr2: %s", st.GetBalance(addr2))
	}
	if fmt.Sprintf("%s", st.GetBalance(addr3)) != "19687500000000001000" { //nolint
		t.Errorf("wrong balance of addr3: %s", st.GetBalance(addr3))
	}
}

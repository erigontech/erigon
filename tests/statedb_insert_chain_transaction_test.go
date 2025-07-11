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

package tests

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/abi/bind/backends"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/tests/contracts"
)

func TestInsertIncorrectStateRootDifferentAccounts(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := types.CopyHeader(chain.Headers[0]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root

	if chain.Headers[0].Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(data.addresses[1], to, uint256.NewInt(5000)),
			data.keys[1],
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}
	tx, err := m.DB.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(m.NewStateReader(tx))
	exist, err := st.Exist(to)
	if err != nil {
		t.Error(err)
	}
	if !exist {
		t.Error("expected account to exist")
	}

	balance, err := st.GetBalance(from)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 1000000000 {
		t.Fatalf("got %v, expected %v", balance, 1000000000)
	}
	balance, err = st.GetBalance(data.addresses[1])
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 999995000 {
		t.Fatalf("got %v, expected %v", balance, 999995000)
	}
	balance, err = st.GetBalance(to)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 5000 {
		t.Fatalf("got %v, expected %v", balance, 5000)
	}
}

func TestInsertIncorrectStateRootSameAccount(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// BLOCK 1
	incorrectHeader := types.CopyHeader(chain.Headers[0]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root

	if chain.Headers[0].Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(5000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(m.NewStateReader(tx))
	exist, err := st.Exist(to)
	if err != nil {
		t.Error(err)
	}
	if !exist {
		t.Error("expected account to exist")
	}

	balance, err := st.GetBalance(from)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 999995000 {
		t.Fatalf("got %v, expected %v", balance, 999995000)
	}
	balance, err = st.GetBalance(to)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 5000 {
		t.Fatalf("got %v, expected %v", balance, 5000)
	}
}

func TestInsertIncorrectStateRootSameAccountSameAmount(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := types.CopyHeader(chain.Headers[0]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root

	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(m.NewStateReader(tx))
	exist, err := st.Exist(to)
	if err != nil {
		t.Error(err)
	}
	if !exist {
		t.Error("expected account to exist")
	}

	balance, err := st.GetBalance(from)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 999999000 {
		t.Fatalf("got %v, expected %v", balance, 999999000)
	}
	balance, err = st.GetBalance(to)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestInsertIncorrectStateRootAllFundsRoot(t *testing.T) {
	data := getGenesis(big.NewInt(3000))
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := types.CopyHeader(chain.Headers[0]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root

	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(m.NewStateReader(tx))
	exist, err := st.Exist(to)
	if err != nil {
		t.Error(err)
	}
	if !exist {
		t.Error("expected account to exist")
	}

	balance, err := st.GetBalance(from)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 2000 {
		t.Fatalf("got %v, expected %v", balance, 2000)
	}
	balance, err = st.GetBalance(to)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestInsertIncorrectStateRootAllFunds(t *testing.T) {
	data := getGenesis(big.NewInt(3000))
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(3000)),
			fromKey,
		},
		1: {
			getBlockTx(data.addresses[1], to, uint256.NewInt(2000)),
			data.keys[1],
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := types.CopyHeader(chain.Headers[0]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root
	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}

	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(m.NewStateReader(tx))
	exist, err := st.Exist(to)
	if err != nil {
		t.Error(err)
	}
	if !exist {
		t.Error("expected account to exist")
	}

	balance, err := st.GetBalance(from)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 2000 {
		t.Fatalf("got %v, expected %v", balance, 2000)
	}
	balance, err = st.GetBalance(to)
	if err != nil {
		t.Error(err)
	}
	if balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestAccountDeployIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	var contractAddress common.Address
	eipContract := new(contracts.Testcontract)

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}
		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if exist {
			t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	incorrectHeader := types.CopyHeader(chain.Headers[1]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[0].Root
	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[1].Transactions(), chain.Blocks[1].Uncles(), chain.Receipts[1], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}

	// BLOCK 2 - INCORRECT
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}

		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if exist {
			t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 2 - CORRECT
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}

		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)
}

func TestAccountCreateIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	var contractAddress common.Address
	eipContract := new(contracts.Testcontract)

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}

		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if exist {
			t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
		}

		return nil
	})
	require.NoError(t, err)

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}

		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected contractAddress to exist at the block 2", contractAddress.Hash().String())
		}

		return nil
	})
	require.NoError(t, err)

	// BLOCK 3 - INCORRECT
	incorrectHeader := types.CopyHeader(chain.Headers[2]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root
	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[2].Transactions(), chain.Blocks[2].Uncles(), chain.Receipts[2], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}

	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 3
	if err = m.InsertChain(chain.Slice(2, 3)); err != nil {
		t.Fatal(err)
	}
}

func TestAccountUpdateIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	var contractAddress common.Address
	eipContract := new(contracts.Testcontract)

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
		3: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Update, big.NewInt(0)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}

		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if exist {
			t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
		}

		return nil
	})
	require.NoError(t, err)

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}

		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected contractAddress to exist at the block 2", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 3
	if err = m.InsertChain(chain.Slice(2, 3)); err != nil {
		t.Fatal(err)
	}

	// BLOCK 4 - INCORRECT
	incorrectHeader := types.CopyHeader(chain.Headers[3]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root
	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[3].Transactions(), chain.Blocks[3].Uncles(), chain.Receipts[3], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}

	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 4
	if err = m.InsertChain(chain.Slice(3, 4)); err != nil {
		t.Fatal(err)
	}
}

func TestAccountDeleteIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	var contractAddress common.Address
	eipContract := new(contracts.Testcontract)

	m, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {
			getBlockTx(from, to, uint256.NewInt(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
		3: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Remove),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}

		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if exist {
			t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		exist, err := st.Exist(from)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}

		exist, err = st.Exist(contractAddress)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected contractAddress to exist at the block 1", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 3
	if err = m.InsertChain(chain.Slice(2, 3)); err != nil {
		t.Fatal(err)
	}

	// BLOCK 4 - INCORRECT
	incorrectHeader := types.CopyHeader(chain.Headers[3]) // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root
	incorrectBlock := types.NewBlock(incorrectHeader, chain.Blocks[3].Transactions(), chain.Blocks[3].Uncles(), chain.Receipts[3], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 4
	if err = m.InsertChain(chain.Slice(3, 4)); err != nil {
		t.Fatal(err)
	}
}

type initialData struct {
	keys         []*ecdsa.PrivateKey
	addresses    []common.Address
	transactOpts []*bind.TransactOpts
	genesisSpec  *types.Genesis
}

func getGenesis(funds ...*big.Int) initialData {
	accountFunds := big.NewInt(1000000000)
	if len(funds) > 0 {
		accountFunds = funds[0]
	}

	keys := make([]*ecdsa.PrivateKey, 3)
	keys[0], _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	keys[1], _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	keys[2], _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")

	addresses := make([]common.Address, 0, len(keys))
	transactOpts := make([]*bind.TransactOpts, 0, len(keys))
	allocs := types.GenesisAlloc{}
	for _, key := range keys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		addresses = append(addresses, addr)
		to, err := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1))
		if err != nil {
			panic(err)
		}
		transactOpts = append(transactOpts, to)

		allocs[addr] = types.GenesisAccount{Balance: accountFunds}
	}

	return initialData{
		keys:         keys,
		addresses:    addresses,
		transactOpts: transactOpts,
		genesisSpec: &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
			},
			Alloc: allocs,
		},
	}
}

type txn struct {
	txFn blockTx
	key  *ecdsa.PrivateKey
}

func GenerateBlocks(t *testing.T, gspec *types.Genesis, txs map[int]txn) (*mock.MockSentry, *core.ChainPack, error) {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, len(txs), func(i int, block *core.BlockGen) {
		var txn types.Transaction
		var isContractCall bool
		signer := types.LatestSignerForChainID(nil)

		if txToSend, ok := txs[i]; ok {
			txn, isContractCall = txToSend.txFn(block, contractBackend)
			var err error
			txn, err = types.SignTx(txn, *signer, txToSend.key)
			if err != nil {
				return
			}
		}

		if txn != nil {
			if !isContractCall {
				err := contractBackend.SendTransaction(context.Background(), txn)
				if err != nil {
					return
				}
			}

			block.AddTx(txn)
		}

		contractBackend.Commit()
	})
	if err != nil {
		return nil, nil, fmt.Errorf("generate chain: %w", err)
	}
	return m, chain, err
}

type blockTx func(_ *core.BlockGen, backend bind.ContractBackend) (types.Transaction, bool)

func getBlockTx(from common.Address, to common.Address, amount *uint256.Int) blockTx {
	return func(block *core.BlockGen, _ bind.ContractBackend) (types.Transaction, bool) {
		return types.NewTransaction(block.TxNonce(from), to, amount, 21000, new(uint256.Int), nil), false
	}
}

func getBlockDeployTestContractTx(transactOpts *bind.TransactOpts, contractAddress *common.Address, eipContract *contracts.Testcontract) blockTx {
	return func(_ *core.BlockGen, backend bind.ContractBackend) (types.Transaction, bool) {
		contractAddressRes, tx, eipContractRes, err := contracts.DeployTestcontract(transactOpts, backend)
		if err != nil {
			panic(err)
		}

		*contractAddress = contractAddressRes
		*eipContract = *eipContractRes

		return tx, true
	}
}

func getBlockTestContractTx(transactOpts *bind.TransactOpts, contractCall interface{}, newBalance ...*big.Int) blockTx {
	return func(_ *core.BlockGen, backend bind.ContractBackend) (types.Transaction, bool) {
		var (
			tx  types.Transaction
			err error
		)

		switch fn := contractCall.(type) {
		case func(opts *bind.TransactOpts) (types.Transaction, error):
			tx, err = fn(transactOpts)
		case func(opts *bind.TransactOpts, newBalance *big.Int) (types.Transaction, error):
			if len(newBalance) != 1 {
				panic("*big.Int type new balance is expected")
			}
			tx, err = fn(transactOpts, newBalance[0])
		default:
			panic("non expected function type")
		}

		if err != nil {
			panic(err)
		}

		return tx, true
	}
}

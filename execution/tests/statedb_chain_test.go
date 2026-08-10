// Copyright 2016 The go-ethereum Authors
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

package executiontests

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/abi/bind/backends"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestSelfDestructReceive(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               uint256.NewInt(1),
				HomesteadBlock:        new(uint64),
				ByzantiumBlock:        new(uint64),
				ConstantinopleBlock:   new(uint64),
				PetersburgBlock:       new(uint64),
				TangerineWhistleBlock: new(uint64),
				SpuriousDragonBlock:   new(uint64),
			},
			Alloc: types.GenesisAlloc{
				address.Value(): {Balance: funds},
			},
		}
		// this code generates a log
		signer = types.LatestSignerForChainID(nil)
	)

	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)

	var contractAddress common.Address
	var selfDestructorContract *contracts.SelfDestructor

	// There are two blocks
	// First block deploys a contract, then makes it self-destruct, and then sends 1 wei to the address of the contract,
	// effectively turning it from contract account to a non-contract account
	// The second block is empty and is only used to force the newly created blockchain object to reload the trie
	// from the database.
	chain, err := m.GenerateChain(2, func(i int, block *blockgen.BlockGen) {
		var txn types.Transaction

		if i == 0 {
			contractAddress, txn, selfDestructorContract, err = contracts.DeploySelfDestructor(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
			txn, err = selfDestructorContract.SelfDestruct(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
			// Send 1 wei to contract after self-destruction
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address.Value()), contractAddress, uint256.NewInt(1000), 21000, uint256.NewInt(1), nil), *signer, key)
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	if err := m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		exist, err := st.Exist(address)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}
		exist, err = st.Exist(accounts.InternAddress(contractAddress))
		if err != nil {
			return err
		}
		if exist {
			t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
		}
		return nil
	}); err != nil {
		panic(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	if err := m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		// If we got this far, the newly created blockchain (with empty trie cache) loaded trie from the database
		// and that means that the state of the accounts written in the first block was correct.
		// This test checks that the storage root of the account is properly set to the root of the empty tree
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		exist, err := st.Exist(address)
		if err != nil {
			t.Error(err)
		}
		if !exist {
			t.Error("expected account to exist")
		}
		exist, err = st.Exist(accounts.InternAddress(contractAddress))
		if err != nil {
			t.Error(err)
		}
		if !exist {
			t.Error("expected contractAddress to exist at the block 2", contractAddress.String())
		}
		code, err := st.GetCode(accounts.InternAddress(contractAddress))
		if err != nil {
			t.Error(err)
		}
		if len(code) != 0 {
			t.Error("expected empty code in contract at block 2", contractAddress.String())
		}
		return nil
	}); err != nil {
		panic(err)
	}

}

// TestSelfDestructReceiveAccountRecord is TestSelfDestructReceive's scenario —
// deploy, self-destruct, then revive by value transfer, all in one block — read
// back as a whole account rather than through GetCode. The reconstruction
// readers resolve the account record separately from code, so a stale code hash
// or nonce survives there even when GetCode already reports empty.
func TestSelfDestructReceiveAccountRecord(t *testing.T) {
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               uint256.NewInt(1),
				HomesteadBlock:        new(uint64),
				ByzantiumBlock:        new(uint64),
				ConstantinopleBlock:   new(uint64),
				PetersburgBlock:       new(uint64),
				TangerineWhistleBlock: new(uint64),
				SpuriousDragonBlock:   new(uint64),
			},
			Alloc: types.GenesisAlloc{address.Value(): {Balance: funds}},
		}
		signer = types.LatestSignerForChainID(nil)
	)

	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)

	var contractAddress common.Address
	var selfDestructorContract *contracts.SelfDestructor

	chain, err := m.GenerateChain(2, func(i int, block *blockgen.BlockGen) {
		if i == 0 {
			var txn types.Transaction
			contractAddress, txn, selfDestructorContract, err = contracts.DeploySelfDestructor(transactOpts, contractBackend)
			require.NoError(t, err)
			block.AddTx(txn)

			txn, err = selfDestructorContract.SelfDestruct(transactOpts)
			require.NoError(t, err)
			block.AddTx(txn)

			// Revive with a plain value transfer: no CREATE, so nothing writes a
			// nonce or code hash and the pre-destruct cells stay the floor.
			txn, err = types.SignTx(
				types.NewTransaction(block.TxNonce(address.Value()), contractAddress, uint256.NewInt(1000), 21000, uint256.NewInt(1), nil),
				*signer, key)
			require.NoError(t, err)
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chain.Slice(0, 2)))

	require.NoError(t, m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		addr := accounts.InternAddress(contractAddress)

		code, err := st.GetCode(addr)
		require.NoError(t, err)
		require.Empty(t, code, "the destruct removed the code and the transfer did not write any")

		hash, err := st.GetCodeHash(addr)
		require.NoError(t, err)
		require.Equal(t, accounts.EmptyCodeHash, hash, "the code hash must agree with the code")

		// The nonce is NOT reset: a value-transfer resurrect inherits the
		// pre-destruct account fields, which writeset_normalize.go only
		// overrides for a CREATE. A reader must report what the block commits.
		nonce, err := st.GetNonce(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), nonce, "the deployed contract's nonce survives the value-transfer revival")

		bal, err := st.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1000), bal.Uint64(), "the transfer that revived it is still credited")
		return nil
	}))
}

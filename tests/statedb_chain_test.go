// Copyright 2016 The go-ethereum Authors
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

package tests

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/tests/contracts"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

func TestSelfDestructReceive(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				ByzantiumBlock:        new(big.Int),
				ConstantinopleBlock:   new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   new(big.Int),
			},
			Alloc: types.GenesisAlloc{
				address: {Balance: funds},
			},
		}
		// this code generates a log
		signer = types.LatestSignerForChainID(nil)
	)

	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)

	var contractAddress libcommon.Address
	var selfDestructorContract *contracts.SelfDestructor

	// There are two blocks
	// First block deploys a contract, then makes it self-destruct, and then sends 1 wei to the address of the contract,
	// effectively turning it from contract account to a non-contract account
	// The second block is empty and is only used to force the newly created blockchain object to reload the trie
	// from the database.
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
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
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), contractAddress, uint256.NewInt(1000), 21000, uint256.NewInt(1), nil), *signer, key)
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	tx, err := m.DB.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	st := state.New(m.NewStateReader(tx))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1), tx); err != nil {
		t.Fatal(err)
	}

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2), tx); err != nil {
		t.Fatal(err)
	}
	// If we got this far, the newly created blockchain (with empty trie cache) loaded trie from the database
	// and that means that the state of the accounts written in the first block was correct.
	// This test checks that the storage root of the account is properly set to the root of the empty tree

	st = state.New(m.NewStateReader(tx))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 2", contractAddress.String())
	}
	if len(st.GetCode(contractAddress)) != 0 {
		t.Error("expected empty code in contract at block 2", contractAddress.String())
	}

}

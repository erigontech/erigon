// Copyright 2019 The go-ethereum Authors
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

package state_test

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/state/contracts"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/abi/bind/backends"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
)

// Create revival problem
func TestCreate2Revive(t *testing.T) {

	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: types.GenesisAccount{Balance: funds},
			},
		}
		signer = types.LatestSignerForChainID(nil)
	)

	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var revive *contracts.Revive
	// Change this address whenever you make any changes in the code of the revive contract in
	// contracts/revive.sol
	var create2address = common.HexToAddress("e70fd65144383e1189bd710b1e23b61e26315ff4")

	// There are 4 blocks
	// In the first block, we deploy the "factory" contract Revive, which can create children contracts via CREATE2 opcode
	// In the second block, we create the first child contract
	// In the third block, we cause the first child contract to selfdestruct
	// In the forth block, we create the second child contract, and we expect it to have a "clean slate" of storage,
	// i.e. without any storage items that "inherited" from the first child contract by mistake
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			contractAddress, txn, revive, err = contracts.DeployRevive(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 1:
			txn, err = revive.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 2:
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, uint256.NewInt(0), 1000000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), txn)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 3:
			txn, err = revive.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected account to exist")
		}
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if exist {
			t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	var key2 common.Hash
	var check2 uint256.Int
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(create2address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected create2address to exist at the block 2", create2address.String())
		}
		// We expect number 0x42 in the position [2], because it is the block number 2
		key2 = common.BigToHash(big.NewInt(2))
		st.GetState(create2address, key2, &check2)
		if check2.Uint64() != 0x42 {
			t.Errorf("expected 0x42 in position 2, got: %x", check2.Uint64())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 3
	if err = m.InsertChain(chain.Slice(2, 3)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(create2address); err != nil {
			t.Error(err)
		} else if exist {
			t.Error("expected create2address to be self-destructed at the block 3", create2address.String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 4
	if err = m.InsertChain(chain.Slice(3, 4)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(create2address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected create2address to exist at the block 2", create2address.String())
		}
		// We expect number 0x42 in the position [4], because it is the block number 4
		key4 := common.BigToHash(big.NewInt(4))
		var check4 uint256.Int
		st.GetState(create2address, key4, &check4)
		if check4.Uint64() != 0x42 {
			t.Errorf("expected 0x42 in position 4, got: %x", check4.Uint64())
		}
		// We expect number 0x0 in the position [2], because it is the block number 4
		st.GetState(create2address, key2, &check2)
		if !check2.IsZero() {
			t.Errorf("expected 0x0 in position 2, got: %x", check2)
		}
		return nil
	})
	require.NoError(t, err)

}

// Polymorthic contracts via CREATE2
func TestCreate2Polymorth(t *testing.T) {

	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: types.GenesisAccount{Balance: funds},
			},
		}
		signer = types.LatestSignerForChainID(nil)
	)
	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var poly *contracts.Poly

	// Change this address whenever you make any changes in the code of the poly contract in
	// contracts/poly.sol
	var create2address = common.HexToAddress("c66aa74c220476f244b7f45897a124d1a01ca8a8")

	// There are 5 blocks
	// In the first block, we deploy the "factory" contract Poly, which can create children contracts via CREATE2 opcode
	// In the second block, we create the first child contract
	// In the third block, we cause the first child contract to selfdestruct
	// In the forth block, we create the second child contract
	// In the 5th block, we delete and re-create the child contract twice
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			contractAddress, txn, poly, err = contracts.DeployPoly(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 1:
			txn, err = poly.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 2:
			// Trigger self-destruct
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, uint256.NewInt(0), 1000000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), txn)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 3:
			txn, err = poly.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 4:
			// Trigger self-destruct
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, uint256.NewInt(0), 1000000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), txn)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
			// Recreate in the same block
			txn, err = poly.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
			// Trigger self-destruct
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, uint256.NewInt(0), 1000000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), txn)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
			// Recreate in the same block
			txn, err = poly.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {

		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected account to exist")
		}
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if exist {
			t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
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
		if exist, err := st.Exist(create2address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected create2address to exist at the block 2", create2address.String())
		}
		code, err := st.GetCode(create2address)
		if err != nil {
			return err
		}
		if !bytes.Equal(code, common.FromHex("6002ff")) {
			t.Errorf("Expected CREATE2 deployed code 6002ff, got %x", code)
		}
		if !m.HistoryV3 { //AccountsDomain: has no "incarnation" concept
			incarnation, err := st.GetIncarnation(create2address)
			if err != nil {
				return err
			}
			if incarnation != 1 {
				t.Errorf("expected incarnation 1, got %d", incarnation)
			}
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 3
	if err = m.InsertChain(chain.Slice(2, 3)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(create2address); err != nil {
			t.Error(err)
		} else if exist {
			t.Error("expected create2address to be self-destructed at the block 3", create2address.String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 4
	if err = m.InsertChain(chain.Slice(3, 4)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(create2address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected create2address to exist at the block 4", create2address.String())
		}
		code, err := st.GetCode(create2address)
		if err != nil {
			return err
		}
		if !bytes.Equal(code, common.FromHex("6004ff")) {
			t.Errorf("Expected CREATE2 deployed code 6004ff, got %x", code)
		}
		if !m.HistoryV3 { //AccountsDomain: has no "incarnation" concept
			incarnation, err := st.GetIncarnation(create2address)
			if err != nil {
				return err
			}
			if incarnation != 2 {
				t.Errorf("expected incarnation 2, got %d", incarnation)
			}
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 5
	if err = m.InsertChain(chain.Slice(4, 5)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(create2address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected create2address to exist at the block 5", create2address.String())
		}
		code, err := st.GetCode(create2address)
		if err != nil {
			return err
		}
		if !bytes.Equal(code, common.FromHex("6005ff")) {
			t.Errorf("Expected CREATE2 deployed code 6005ff, got %x", code)
		}

		if !m.HistoryV3 { //AccountsDomain: has no "incarnation" concept
			incarnation, err := st.GetIncarnation(create2address)
			if err != nil {
				return err
			}
			if incarnation != 4 {
				t.Errorf("expected incarnation 4 (two self-destructs and two-recreations within a block), got %d", incarnation)
			}
		}
		return nil
	})
	require.NoError(t, err)

}

func TestReorgOverSelfDestruct(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: types.GenesisAccount{Balance: funds},
			},
		}
	)

	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct

	// Here we generate 3 blocks, two of which (the one with "Change" invocation and "Destruct" invocation will be reverted during the reorg)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			contractAddress, txn, selfDestruct, err = contracts.DeploySelfdestruct(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 1:
			txn, err = selfDestruct.Change(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 2:
			txn, err = selfDestruct.Destruct(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOptsLonger, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOptsLonger.GasLimit = 1000000

	longerChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			_, txn, _, err = contracts.DeploySelfdestruct(transactOptsLonger, contractBackendLonger)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackendLonger.Commit()
	})
	if err != nil {
		t.Fatalf("generate long blocks")
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {

		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected account to exist")
		}
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if exist {
			t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
		}
		return nil
	})
	require.NoError(t, err)
	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	var key0 common.Hash
	var correctValueX uint256.Int
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {

			t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
		}

		// Remember value of field "x" (storage item 0) after the first block, to check after rewinding
		st.GetState(contractAddress, key0, &correctValueX)
		return nil
	})
	require.NoError(t, err)

	// BLOCKS 2 + 3
	if err = m.InsertChain(chain.Slice(1, chain.Length())); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if exist {
			t.Error("expected contractAddress to not exist at the block 3", contractAddress.String())
		}
		return nil
	})
	require.NoError(t, err)
	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if err = m.InsertChain(longerChain.Slice(1, 4)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected contractAddress to exist at the block 4", contractAddress.String())
		}
		var valueX uint256.Int
		st.GetState(contractAddress, key0, &valueX)
		if valueX != correctValueX {
			t.Fatalf("storage value has changed after reorg: %x, expected %x", valueX, correctValueX)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestReorgOverStateChange(t *testing.T) {
	t.Parallel()
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: {Balance: funds},
			},
		}
	)

	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct

	// Here we generate 3 blocks, two of which (the one with "Change" invocation and "Destruct" invocation will be reverted during the reorg)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			contractAddress, txn, selfDestruct, err = contracts.DeploySelfdestruct(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 1:
			txn, err = selfDestruct.Change(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOptsLonger, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOptsLonger.GasLimit = 1000000
	longerChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			_, txn, _, err = contracts.DeploySelfdestruct(transactOptsLonger, contractBackendLonger)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackendLonger.Commit()
	})
	if err != nil {
		t.Fatalf("generate longer blocks: %v", err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected account to exist")
		}
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if exist {

			t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	var key0 common.Hash
	var correctValueX uint256.Int
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {

			t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
		}

		// Remember value of field "x" (storage item 0) after the first block, to check after rewinding
		st.GetState(contractAddress, key0, &correctValueX)
		return nil
	})
	require.NoError(t, err)

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, chain.Length())); err != nil {
		t.Fatal(err)
	}

	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if err = m.InsertChain(longerChain.Slice(1, 3)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected contractAddress to exist at the block 4", contractAddress.String())
		}

		// Reload blockchain from the database
		var valueX uint256.Int
		st.GetState(contractAddress, key0, &valueX)
		if valueX != correctValueX {
			t.Fatalf("storage value has changed after reorg: %x, expected %x", valueX, correctValueX)
		}
		return nil
	})
	require.NoError(t, err)

}

type BucketsStats struct {
	Accounts     uint64
	Storage      uint64
	ChangeSetHAT uint64
	ChangeSetHST uint64
	HAT          uint64
	HST          uint64
}

func (b BucketsStats) Size() uint64 {
	return b.ChangeSetHST + b.ChangeSetHAT + b.HST + b.Storage + b.HAT + b.Accounts
}

func TestCreateOnExistingStorage(t *testing.T) {
	t.Skip("Alex Sharov: seems it's not useful property in reality")

	t.Parallel()
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		// Address of the contract that will be deployed
		contractAddr = common.HexToAddress("0x3a220f351252089d385b29beca14e27f204c296a")
		funds        = big.NewInt(1000000000)
		gspec        = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: {Balance: funds},
				// Pre-existing storage item in an account without code
				contractAddr: {Balance: funds, Storage: map[common.Hash]common.Hash{{}: common.HexToHash("0x42")}},
			},
		}
	)

	m := mock.MockWithGenesis(t, gspec, key, false)

	var err error
	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)

	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address

	// There is one block, and it ends up deploying Revive contract (could be any other contract, it does not really matter)
	// On the address contractAddr, where there is a storage item in the genesis, but no contract code
	// We expect the pre-existing storage items to be removed by the deployment
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			contractAddress, txn, _, err = contracts.DeployRevive(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected account to exist")
		}
		if contractAddress != contractAddr {
			t.Errorf("expected contract address to be %x, got: %x", contractAddr, contractAddress)
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	var key0 common.Hash
	var check0 uint256.Int
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
		}

		st.GetState(contractAddress, key0, &check0)
		if !check0.IsZero() {
			t.Errorf("expected 0x00 in position 0, got: %x", check0.Bytes())
		}
		return nil
	})
	require.NoError(t, err)
}

func TestReproduceCrash(t *testing.T) {
	t.Parallel()
	// This example was taken from Ropsten contract that used to cause a crash
	// it is created in the block 598915 and then there are 3 transactions modifying
	// its storage in the same block:
	// 1. Setting storageKey 1 to a non-zero value
	// 2. Setting storageKey 2 to a non-zero value
	// 3. Setting both storageKey1 and storageKey2 to zero values
	value0 := uint256.NewInt(0)
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	storageKey1 := common.HexToHash("0x0e4c0e7175f9d22279a4f63ff74f7fa28b7a954a6454debaa62ce43dd9132541")
	value1 := uint256.NewInt(0x016345785d8a0000)
	storageKey2 := common.HexToHash("0x0e4c0e7175f9d22279a4f63ff74f7fa28b7a954a6454debaa62ce43dd9132542")
	value2 := uint256.NewInt(0x58c00a51)

	_, tx, _ := state.NewTestTemporalDb(t)
	sd, err := dbstate.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	txNum := uint64(1)
	tsw := state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	tsr := state.NewReaderV3(sd.AsGetter(tx))

	intraBlockState := state.New(tsr)
	// Start the 1st transaction
	intraBlockState.CreateAccount(contract, true)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 2nd transaction
	intraBlockState.SetState(contract, storageKey1, *value1)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 3rd transaction
	intraBlockState.AddBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	intraBlockState.SetState(contract, storageKey2, *value2)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 4th transaction - clearing both storage cells
	intraBlockState.SubBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	intraBlockState.SetState(contract, storageKey1, *value0)
	intraBlockState.SetState(contract, storageKey2, *value0)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
}

func TestEip2200Gas(t *testing.T) {
	t.Parallel()
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				PetersburgBlock:       big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
				IstanbulBlock:         big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: {Balance: funds},
			},
		}
	)

	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct

	// Here we generate 1 block with 2 transactions, first creates a contract with some initial values in the
	// It activates the SSTORE pricing rules specific to EIP-2200 (istanbul)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			contractAddress, txn, selfDestruct, err = contracts.DeploySelfdestruct(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)

			transactOpts.GasPrice = big.NewInt(1)
			txn, err = selfDestruct.Change(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	var balanceBefore uint256.Int
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected account to exist")
		}
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if exist {
			t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
		}
		balanceBefore, err = st.GetBalance(address)
		return err
	})
	require.NoError(t, err)

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
		}
		balanceAfter, err := st.GetBalance(address)
		if err != nil {
			return err
		}
		gasSpent := big.NewInt(0).Sub(balanceBefore.ToBig(), balanceAfter.ToBig())
		expectedGasSpent := big.NewInt(190373) //(192245) // In the incorrect version, it is 179645
		if gasSpent.Cmp(expectedGasSpent) != 0 {
			t.Errorf("Expected gas spent: %d, got %d", expectedGasSpent, gasSpent)
		}
		return nil
	})
	require.NoError(t, err)
}

// Create contract, drop trie, reload trie from disk and add block with contract call
func TestWrongIncarnation(t *testing.T) {
	t.Parallel()
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: types.GenesisAccount{Balance: funds},
			},
		}
	)

	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var changer *contracts.Changer

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			contractAddress, txn, changer, err = contracts.DeployChanger(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 1:
			txn, err = changer.Change(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected account to exist")
		}
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if exist {
			t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		stateReader := m.NewStateReader(tx)
		acc, err := stateReader.ReadAccountData(contractAddress)
		if err != nil {
			t.Fatal(err)
		}
		if acc == nil {
			t.Fatal(errors.New("acc not found"))
		}

		if acc.Incarnation != state.FirstContractIncarnation {
			t.Fatal("Incorrect incarnation", acc.Incarnation)
		}

		st := state.New(stateReader)
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCKS 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		stateReader := m.NewStateReader(tx)
		acc, err := stateReader.ReadAccountData(contractAddress)
		if err != nil {
			t.Fatal(err)
		}
		if acc == nil {
			t.Fatal(errors.New("acc not found"))
		}
		if acc.Incarnation != state.FirstContractIncarnation {
			t.Fatal("Incorrect incarnation", acc.Incarnation)
		}
		return nil
	})
	require.NoError(t, err)
}

// create acc, deploy to it contract, reorg to state without contract
func TestWrongIncarnation2(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: types.GenesisAccount{Balance: funds},
			},
		}
		signer = types.LatestSignerForChainID(nil)
	)

	knownContractAddress := common.HexToAddress("0xdb7d6ab1f17c6b31909ae466702703daef9269cf")

	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), knownContractAddress, uint256.NewInt(1000), 1000000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), txn)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		case 1:
			contractAddress, txn, _, err = contracts.DeployChanger(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	if knownContractAddress != contractAddress {
		t.Errorf("Expected contractAddress: %x, got %x", knownContractAddress, contractAddress)
	}

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOptsLonger, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOptsLonger.GasLimit = 1000000
	longerChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), knownContractAddress, uint256.NewInt(1000), 1000000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackendLonger.SendTransaction(context.Background(), txn)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
		}
		contractBackendLonger.Commit()
	})
	if err != nil {
		t.Fatalf("generate longer blocks: %v", err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(address); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected account to exist")
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	// BLOCKS 2
	if err = m.InsertChain(chain.Slice(1, chain.Length())); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(contractAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
		}

		stateReader := m.NewStateReader(tx)
		acc, err := stateReader.ReadAccountData(contractAddress)
		if err != nil {
			t.Fatal(err)
		}
		if acc == nil {
			t.Fatal(errors.New("acc not found"))
		}
		if acc.Incarnation != state.FirstContractIncarnation {
			t.Fatal("wrong incarnation")
		}
		return nil
	})
	require.NoError(t, err)
	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if err = m.InsertChain(longerChain.Slice(1, longerChain.Length())); err != nil {
		t.Fatal(err)
	}

	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		stateReader := m.NewStateReader(tx)
		acc, err := stateReader.ReadAccountData(contractAddress)
		if err != nil {
			t.Fatal(err)
		}
		if acc == nil {
			t.Fatal(errors.New("acc not found"))
		}
		if acc.Incarnation != state.NonContractIncarnation {
			t.Fatal("wrong incarnation", acc.Incarnation)
		}
		return nil
	})
	require.NoError(t, err)

}

func TestChangeAccountCodeBetweenBlocks(t *testing.T) {
	t.Parallel()
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")

	_, tx, _ := state.NewTestTemporalDb(t)
	sd, err := dbstate.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)
	blockNum, txNum := uint64(1), uint64(3)
	_ = blockNum

	r, tsw := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	intraBlockState := state.New(r)
	// Start the 1st transaction
	intraBlockState.CreateAccount(contract, true)

	oldCode := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, oldCode)
	intraBlockState.AddBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	rh1, err := sd.ComputeCommitment(context.Background(), true, blockNum, txNum, "")
	require.NoError(t, err)
	//t.Logf("stateRoot %x", rh1)

	trieCode, tcErr := r.ReadAccountCode(contract)
	require.NoError(t, tcErr, "you can receive the new code")
	assert.Equal(t, oldCode, trieCode, "new code should be received")

	newCode := []byte{0x04, 0x04, 0x04, 0x04}
	intraBlockState.SetCode(contract, newCode)

	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}

	trieCode, tcErr = r.ReadAccountCode(contract)
	require.NoError(t, tcErr, "you can receive the new code")
	assert.Equal(t, newCode, trieCode, "new code should be received")

	rh2, err := sd.ComputeCommitment(context.Background(), true, blockNum, txNum, "")
	require.NoError(t, err)
	require.NotEqual(t, rh1, rh2)
}

// TestCacheCodeSizeSeparately makes sure that we don't store CodeNodes for code sizes
func TestCacheCodeSizeSeparately(t *testing.T) {
	t.Parallel()
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	//root := common.HexToHash("0xb939e5bcf5809adfb87ab07f0795b05b95a1d64a90f0eddd0c3123ac5b433854")

	_, tx, _ := state.NewTestTemporalDb(t)
	sd, err := dbstate.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)
	blockNum, txNum := uint64(1), uint64(3)
	_ = blockNum

	r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)

	intraBlockState := state.New(r)
	// Start the 1st transaction
	intraBlockState.CreateAccount(contract, true)

	code := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, code)
	intraBlockState.AddBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, w); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if err := intraBlockState.CommitBlock(&chain.Rules{}, w); err != nil {
		t.Errorf("error committing block: %v", err)
	}

	codeSize, err := r.ReadAccountCodeSize(contract)
	require.NoError(t, err, "you can receive the new code")
	assert.Equal(t, len(code), codeSize, "new code should be received")

	code2, err := r.ReadAccountCode(contract)
	require.NoError(t, err, "you can receive the new code")
	assert.Equal(t, code, code2, "new code should be received")
}

// TestCacheCodeSizeInTrie makes sure that we don't just read from the DB all the time
func TestCacheCodeSizeInTrie(t *testing.T) {
	t.Parallel()
	//t.Skip("switch to TG state readers/writers")
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	root := common.HexToHash("0xb939e5bcf5809adfb87ab07f0795b05b95a1d64a90f0eddd0c3123ac5b433854")

	_, tx, _ := state.NewTestTemporalDb(t)
	sd, err := dbstate.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)
	blockNum := uint64(1)
	txNum := uint64(3)

	r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)

	intraBlockState := state.New(r)
	// Start the 1st transaction
	intraBlockState.CreateAccount(contract, true)

	code := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, code)
	intraBlockState.AddBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, w); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if err := intraBlockState.CommitBlock(&chain.Rules{}, w); err != nil {
		t.Errorf("error committing block: %v", err)
	}

	r2, err := sd.ComputeCommitment(context.Background(), true, blockNum, txNum, "")
	require.NoError(t, err)
	require.Equal(t, root, common.CastToHash(r2))

	codeHash := common.BytesToHash(crypto.Keccak256(code))
	codeSize, err := r.ReadAccountCodeSize(contract)
	require.NoError(t, err, "you can receive the code size ")
	assert.Equal(t, len(code), codeSize, "you can receive the code size")

	require.NoError(t, tx.Delete(kv.Code, codeHash[:]), nil)

	codeSize2, err := r.ReadAccountCodeSize(contract)
	require.NoError(t, err, "you can still receive code size even with empty DB")
	assert.Equal(t, len(code), codeSize2, "code size should be received even with empty DB")

	r2, err = sd.ComputeCommitment(context.Background(), true, 1, 2, "")
	require.NoError(t, err)
	require.Equal(t, root, common.CastToHash(r2))
}

func TestRecreateAndRewind(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: chain.TestChainConfig,
			Alloc: types.GenesisAlloc{
				address: types.GenesisAccount{Balance: funds},
			},
		}
	)

	m := mock.MockWithGenesis(t, gspec, key, false)
	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOpts.GasLimit = 1000000
	var revive *contracts.Revive2
	var phoenix *contracts.Phoenix
	var reviveAddress common.Address
	var phoenixAddress common.Address

	chain, err1 := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			// Deploy phoenix factory
			reviveAddress, txn, revive, err = contracts.DeployRevive2(transactOpts, contractBackend)
			if err != nil {
				panic(err)
			}
			block.AddTx(txn)
		case 1:
			// Calculate the address of the Phoenix and create handle to phoenix contract
			var codeHash common.Hash
			if codeHash, err = common.HashData(common.FromHex(contracts.PhoenixBin)); err != nil {
				panic(err)
			}
			phoenixAddress = types.CreateAddress2(reviveAddress, [32]byte{}, codeHash.Bytes())
			if phoenix, err = contracts.NewPhoenix(phoenixAddress, contractBackend); err != nil {
				panic(err)
			}
			// Deploy phoenix
			if txn, err = revive.Deploy(transactOpts, [32]byte{}); err != nil {
				panic(err)
			}
			block.AddTx(txn)
			// Modify phoenix storage
			if txn, err = phoenix.Increment(transactOpts); err != nil {
				panic(err)
			}
			block.AddTx(txn)
			if txn, err = phoenix.Increment(transactOpts); err != nil {
				panic(err)
			}
			block.AddTx(txn)
		case 2:
			// Destruct the phoenix
			if txn, err = phoenix.Die(transactOpts); err != nil {
				panic(err)
			}
			block.AddTx(txn)
		case 3:
			// Recreate the phoenix, and change the storage
			if txn, err = revive.Deploy(transactOpts, [32]byte{}); err != nil {
				panic(err)
			}
			block.AddTx(txn)
			if txn, err = phoenix.Increment(transactOpts); err != nil {
				panic(err)
			}
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err1 != nil {
		t.Fatalf("generate blocks: %v", err1)
	}

	contractBackendLonger := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOptsLonger, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)
	transactOptsLonger.GasLimit = 1000000
	longerChain, err1 := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, block *core.BlockGen) {
		var txn types.Transaction

		switch i {
		case 0:
			// Deploy phoenix factory
			reviveAddress, txn, revive, err = contracts.DeployRevive2(transactOptsLonger, contractBackendLonger)
			if err != nil {
				panic(err)
			}
			block.AddTx(txn)
		case 1:
			// Calculate the address of the Phoenix and create handle to phoenix contract
			var codeHash common.Hash
			if codeHash, err = common.HashData(common.FromHex(contracts.PhoenixBin)); err != nil {
				panic(err)
			}
			phoenixAddress = types.CreateAddress2(reviveAddress, [32]byte{}, codeHash.Bytes())
			if phoenix, err = contracts.NewPhoenix(phoenixAddress, contractBackendLonger); err != nil {
				panic(err)
			}
			// Deploy phoenix
			if txn, err = revive.Deploy(transactOptsLonger, [32]byte{}); err != nil {
				panic(err)
			}
			block.AddTx(txn)
			// Modify phoenix storage
			if txn, err = phoenix.Increment(transactOptsLonger); err != nil {
				panic(err)
			}
			block.AddTx(txn)
			if txn, err = phoenix.Increment(transactOptsLonger); err != nil {
				panic(err)
			}
			block.AddTx(txn)
		case 2:
			// Destruct the phoenix
			if txn, err = phoenix.Die(transactOptsLonger); err != nil {
				panic(err)
			}
			block.AddTx(txn)
		case 3:
			// Recreate the phoenix, but now with the empty storage
			if txn, err = revive.Deploy(transactOptsLonger, [32]byte{}); err != nil {
				panic(err)
			}
			block.AddTx(txn)
		}
		contractBackendLonger.Commit()
	})
	if err1 != nil {
		t.Fatalf("generate longer blocks: %v", err1)
	}

	// BLOCKS 1 and 2
	if err = m.InsertChain(chain.Slice(0, 2)); err != nil {
		t.Fatal(err)
	}

	var key0 common.Hash
	var check0 uint256.Int
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(phoenixAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Errorf("expected phoenix %x to exist after first insert", phoenixAddress)
		}

		st.GetState(phoenixAddress, key0, &check0)
		if check0.Cmp(uint256.NewInt(2)) != 0 {
			t.Errorf("expected 0x02 in position 0, got: 0x%x", check0.Bytes())
		}
		return nil
	})
	require.NoError(t, err)

	// Block 3 and 4
	if err = m.InsertChain(chain.Slice(2, chain.Length())); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {

		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(phoenixAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Errorf("expected phoenix %x to exist after second insert", phoenixAddress)
		}

		st.GetState(phoenixAddress, key0, &check0)
		if check0.Cmp(uint256.NewInt(1)) != 0 {
			t.Errorf("expected 0x01 in position 0, got: 0x%x", check0.Bytes())
		}
		return nil
	})
	require.NoError(t, err)

	// Reorg
	if err = m.InsertChain(longerChain); err != nil {
		t.Fatal(err)
	}
	err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		if exist, err := st.Exist(phoenixAddress); err != nil {
			t.Error(err)
		} else if !exist {
			t.Errorf("expected phoenix %x to exist after second insert", phoenixAddress)
		}

		st.GetState(phoenixAddress, key0, &check0)
		if check0.Cmp(uint256.NewInt(0)) != 0 {
			t.Errorf("expected 0x00 in position 0, got: 0x%x", check0.Bytes())
		}
		return nil
	})
	require.NoError(t, err)

}
func TestTxLookupUnwind(t *testing.T) {
	t.Parallel()
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
			},
			Alloc: types.GenesisAlloc{
				address: types.GenesisAccount{Balance: funds},
			},
		}
		signer = types.LatestSignerForChainID(nil)
	)

	m := mock.MockWithGenesis(t, gspec, key, false)
	chain1, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, block *core.BlockGen) {
		var txn types.Transaction
		var e error
		switch i {
		case 1:
			txn, e = types.SignTx(types.NewTransaction(block.TxNonce(address), address, uint256.NewInt(0), 1000000, new(uint256.Int), nil), *signer, key)
			if e != nil {
				t.Fatal(e)
			}
			block.AddTx(txn)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	chain2, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.InsertChain(chain1); err != nil {
		t.Fatal(err)
	}
	if err = m.InsertChain(chain2); err != nil {
		t.Fatal(err)
	}
	var count uint64
	if err = m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		var e error
		if count, e = tx.Count(kv.TxLookup); e != nil {
			return e
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("txlookup record expected to be deleted, got %d", count)
	}
}

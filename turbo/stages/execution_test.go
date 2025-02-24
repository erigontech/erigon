// Copyright 2014 The go-ethereum Authors
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

package stages_test

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus/clique"
	"github.com/erigontech/erigon/consensus/ethash"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func TestBlockExecutionClique(t *testing.T) {
	// Initialize a Clique chain with a single signer
	var (
		cliqueDB = memdb.NewTestDB(t, kv.ConsensusDB)
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr     = crypto.PubkeyToAddress(key.PublicKey)
		engine   = clique.New(params.AllCliqueProtocolChanges, params.CliqueSnapshot, cliqueDB, log.New())
		signer   = types.LatestSignerForChainID(nil)
	)

	fmt.Println("addr  ", addr.String())

	genspec := &types.Genesis{
		ExtraData: make([]byte, clique.ExtraVanity+length.Addr+clique.ExtraSeal),
		Alloc: map[libcommon.Address]types.GenesisAccount{
			addr: {Balance: big.NewInt(10000000000000000)},
		},
		Config:     params.AllCliqueProtocolChanges,
		Difficulty: big.NewInt(123),
	}
	copy(genspec.ExtraData[clique.ExtraVanity:], addr[:])
	m := mock.MockWithGenesisEngine(t, genspec, engine, false /*withPosDownloader*/, true /*checkStateRoot*/, true /*useSilkworm*/)

	// Generate a batch of blocks, each properly signed
	getHeader := func(hash libcommon.Hash, number uint64) (h *types.Header) {
		if err := m.DB.View(m.Ctx, func(tx kv.Tx) (err error) {
			h, err = m.BlockReader.Header(m.Ctx, tx, hash, number)
			return err
		}); err != nil {
			panic(err)
		}
		return h
	}

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, block *core.BlockGen) {
		// The chain maker doesn't have access to a chain, so the difficulty will be
		// lets unset (nil). Set it here to the correct value.
		block.SetDifficulty(clique.DiffInTurn)

		// We want to simulate an empty middle block, having the same state as the
		// first one. The last is needs a state change again to force a reorg.
		// if i != 1 {
		baseFee, _ := uint256.FromBig(block.GetHeader().BaseFee)
		tx, err := types.SignTx(types.NewTransaction(block.TxNonce(addr), libcommon.Address{0x00}, uint256.NewInt(1), params.TxGas, baseFee, nil), *signer, key)
		if err != nil {
			panic(err)
		}
		block.AddTxWithChain(getHeader, engine, tx)
	})

	if err != nil {
		t.Fatalf("generate blocks  : %v", err)
	}
	for i, block := range chain.Blocks {
		header := block.Header()
		if i > 0 {
			header.ParentHash = chain.Blocks[i-1].Hash()
		}
		header.Extra = make([]byte, clique.ExtraVanity+clique.ExtraSeal)
		header.Difficulty = clique.DiffInTurn

		sig, _ := crypto.Sign(clique.SealHash(header).Bytes(), key)
		copy(header.Extra[len(header.Extra)-clique.ExtraSeal:], sig)
		chain.Headers[i] = header
		chain.Blocks[i] = block.WithSeal(header)
	}

	fmt.Printf("block %d: %s\n", m.Genesis.Number(), m.Genesis.Hash().String())
	for _, block := range chain.Blocks {
		fmt.Printf("block %d: %s\n", block.NumberU64(), block.Hash().String())
	}

	// Insert the first two blocks and make sure the chain is valid
	if err := m.InsertChain(chain.Slice(0, 3)); err != nil {
		t.Fatalf("failed to insert initial blocks : %v", err)
	}
	if err := m.DB.View(m.Ctx, func(tx kv.Tx) error {
		if head, err1 := m.BlockReader.BlockByHash(m.Ctx, tx, rawdb.ReadHeadHeaderHash(tx)); err1 != nil {
			t.Errorf("could not read chain head: %v", err1)
		} else if head.NumberU64() != 3 {
			t.Errorf("chain head mismatch: have %d, want %d", head.NumberU64(), 3)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	txn, err := m.DB.BeginRw(context.Background())
	defer txn.Rollback()
	require.NoError(t, err)
	stateReader := m.NewStateReader(txn)
	acc, err := stateReader.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, acc)

	// Check the balance of the account to ensure the transaction was applied
	expectedBalance := uint256.Int{0x235ac4090ade2d, 0x0, 0x0, 0x0} // Initial balance - 1 (transaction value)
	require.Equal(t, expectedBalance, acc.Balance)
}

func TestBlockExecutionEthash(t *testing.T) {
	var (
		signer      = types.LatestSignerForChainID(nil)
		bankKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		bankAddress = crypto.PubkeyToAddress(bankKey.PublicKey)
		bankFunds   = big.NewInt(1e9)
		contract    = hexutil.MustDecode("0x60606040526040516102eb3803806102eb8339016040526060805160600190602001505b33600060006101000a81548173ffffffffffffffffffffffffffffffffffffffff02191690830217905550806001600050908051906020019082805482825590600052602060002090601f01602090048101928215609c579182015b82811115609b578251826000505591602001919060010190607f565b5b50905060c3919060a7565b8082111560bf576000818150600090555060010160a7565b5090565b50505b50610215806100d66000396000f30060606040526000357c01000000000000000000000000000000000000000000000000000000009004806341c0e1b51461004f578063adbd84651461005c578063cfae32171461007d5761004d565b005b61005a6004506100f6565b005b610067600450610208565b6040518082815260200191505060405180910390f35b61008860045061018a565b60405180806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600302600f01f150905090810190601f1680156100e85780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141561018757600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16ff5b5b565b60206040519081016040528060008152602001506001600050805480601f016020809104026020016040519081016040528092919081815260200182805480156101f957820191906000526020600020905b8154815290600101906020018083116101dc57829003601f168201915b50505050509050610205565b90565b6000439050610212565b90560000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000d5468697320697320437972757300000000000000000000000000000000000000")
		input       = hexutil.MustDecode("0xadbd8465")
		kill        = hexutil.MustDecode("0x41c0e1b5")
		gspec       = &types.Genesis{
			Config:  params.TestChainConfig,
			Alloc:   types.GenesisAlloc{bankAddress: {Balance: bankFunds}},
			BaseFee: big.NewInt(1),
		}
	)

	m := mock.MockWithEverything(t, gspec, bankKey, prune.DefaultMode, ethash.NewFaker(), 128 /*blockBufferSize*/, false, /*withTxPool*/
		false /*withPosDownloader*/, true /*checkStateRoot*/, true /*useSilkworm*/)

	var theAddr libcommon.Address
	var gasFeeCap uint256.Int
	gasFeeCap.SetUint64(1)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
		nonce := block.TxNonce(bankAddress)
		switch i {
		case 0:
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 1e6, &gasFeeCap, contract), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
			theAddr = crypto.CreateAddress(bankAddress, nonce)
			fmt.Println("theAddr  ", theAddr.String())
		case 1:
			txn, err := types.SignTx(types.NewTransaction(nonce, theAddr, new(uint256.Int), 90000, &gasFeeCap, input), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)
		case 2:
			txn, err := types.SignTx(types.NewTransaction(nonce, theAddr, new(uint256.Int), 90000, &gasFeeCap, kill), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)

			// sending kill messsage to an already suicided account
			txn, err = types.SignTx(types.NewTransaction(nonce+1, theAddr, new(uint256.Int), 90000, &gasFeeCap, kill), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)
		}
	})
	assert.NoError(t, err)

	err = m.InsertChain(chain)
	assert.NoError(t, err)
	tx, err := m.DB.BeginTemporalRw(m.Ctx)
	assert.NoError(t, err)
	defer tx.Rollback()

	st := state.New(m.NewStateReader(tx))
	assert.NoError(t, err)
	exist, err := st.Exist(theAddr)
	assert.NoError(t, err)
	assert.False(t, exist, "Contract should've been removed")

	st = state.New(m.NewHistoryStateReader(1, tx))
	assert.NoError(t, err)
	exist, err = st.Exist(theAddr)
	assert.NoError(t, err)
	assert.False(t, exist, "Contract should not exist at block #0")

	st = state.New(m.NewHistoryStateReader(2, tx))
	assert.NoError(t, err)
	exist, err = st.Exist(theAddr)
	assert.NoError(t, err)
	assert.True(t, exist, "Contract should exist at block #1")

	st = state.New(m.NewHistoryStateReader(3, tx))
	assert.NoError(t, err)
	exist, err = st.Exist(theAddr)
	assert.NoError(t, err)
	assert.True(t, exist, "Contract should exist at block #2")
}

func createChainWithStorageContract(t testing.TB, useSilkworm bool, subAccountsCount int) (*mock.MockSentry, *types.Block, []*ecdsa.PrivateKey, libcommon.Address) {
	var (
		signer      = types.LatestSignerForChainID(big.NewInt(1337))
		bankKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		bankAddress = crypto.PubkeyToAddress(bankKey.PublicKey)
		bankFunds   = big.NewInt(1e9)
		contract    = hexutil.MustDecode("0x6c60003560005260206000203355600052600d6013f3")
		input       = hexutil.MustDecode("0x0000000000000000000000006a5f355f5260206000203355600052600b6015f3")
		gspec       = &types.Genesis{
			Config:  params.TestChainConfig,
			Alloc:   types.GenesisAlloc{bankAddress: {Balance: bankFunds}},
			BaseFee: big.NewInt(1),
		}
		gasFeeCap = uint256.NewInt(1)
	)

	subPrivateKeys := make([]*ecdsa.PrivateKey, subAccountsCount)
	subAddresses := make([]libcommon.Address, subAccountsCount)
	for i := 0; i < subAccountsCount; i++ {
		subPrivateKeys[i], _ = crypto.GenerateKey()
		subAddresses[i] = crypto.PubkeyToAddress(subPrivateKeys[i].PublicKey)
	}

	m := mock.MockWithEverything(t, gspec, bankKey, prune.DefaultMode, ethash.NewFaker(), 128 /*blockBufferSize*/, false, /*withTxPool*/
		false /*withPosDownloader*/, true /*checkStateRoot*/, useSilkworm)

	var storageContractAddress libcommon.Address

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, block *core.BlockGen) {
		nonce := block.TxNonce(bankAddress)
		switch i {
		case 0:
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 1e6, gasFeeCap, contract), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
			storageContractAddress = crypto.CreateAddress(bankAddress, nonce)
		case 1:
			txn, err := types.SignTx(types.NewTransaction(nonce, storageContractAddress, new(uint256.Int), 90000, gasFeeCap, input), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)
		case 2:
			txn, err := types.SignTx(types.NewTransaction(nonce, storageContractAddress, new(uint256.Int), 90000, gasFeeCap, input), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)
		case 3:
			// Send some funds to subAccounts
			for i := 0; i < subAccountsCount; i++ {
				txn, err := types.SignTx(types.NewTransaction(nonce+uint64(i), subAddresses[i], uint256.NewInt(1e5), 90000, gasFeeCap, nil), *signer, bankKey)
				assert.NoError(t, err)
				block.AddTx(txn)
			}
		}
	})
	assert.NoError(t, err)

	err = m.InsertChain(chain)
	assert.NoError(t, err)

	return m, chain.Blocks[3], subPrivateKeys, storageContractAddress
}

func TestBlockExecutionStorageContract(t *testing.T) {
	var (
		signer           = types.LatestSignerForChainID(big.NewInt(1337))
		gasFeeCap        = uint256.NewInt(1)
		subAccountsCount = 100
	)

	tests := []struct {
		name        string
		useSilkworm bool
	}{
		{"useSilkworm = false", false},
		{"useSilkworm = true", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, headBlock, subPrivateKeys, storageContractAddress := createChainWithStorageContract(t, tt.useSilkworm, subAccountsCount)

			chain, err := core.GenerateChain(m.ChainConfig, headBlock, m.Engine, m.DB, 1, func(_ int, block *core.BlockGen) {
				for j := uint64(0); j < uint64(subAccountsCount); j++ {
					txn, err := types.SignTx(types.NewTransaction(0, storageContractAddress, uint256.NewInt(0), 90000, gasFeeCap, hexutil.EncodeTs(j)), *signer, subPrivateKeys[j])
					assert.NoError(t, err)
					block.AddTx(txn)
				}
			})
			assert.NoError(t, err)

			newBlock := chain.Blocks[0]

			request := &execution.InsertBlocksRequest{
				Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{newBlock}),
			}
			result, err := m.Eth1ExecutionService.InsertBlocks(m.Ctx, request)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, result.Result, execution.ExecutionStatus_Success)

			fmt.Println("newBlock  ", newBlock.Hash().String())
			validationRequest := &execution.ValidationRequest{
				Hash:   gointerfaces.ConvertHashToH256(newBlock.Hash()),
				Number: newBlock.Number().Uint64(),
			}

			validationResult, err := m.Eth1ExecutionService.ValidateChain(m.Ctx, validationRequest)
			require.NoError(t, err)
			require.NotNil(t, validationResult)
			require.Equal(t, validationResult.ValidationStatus, execution.ExecutionStatus_Success)
			fmt.Println("validationResult  ", validationResult)
			m.Close()
		})
	}

	// ready := false
	// for !ready {
	// 	readyResponse, err := m.Eth1ExecutionService.Ready(m.Ctx, nil)
	// 	require.NoError(t, err)
	// 	ready = readyResponse.Ready
	// }

	// forkchoiceRequest := &execution.ForkChoice{
	// 	HeadBlockHash: gointerfaces.ConvertHashToH256(newBlock.Hash()),

	// 	Timeout:            0,
	// 	FinalizedBlockHash: gointerfaces.ConvertHashToH256(common.Hash{}),
	// 	SafeBlockHash:      gointerfaces.ConvertHashToH256(common.Hash{}),
	// }

	// fcuReceipt, err := m.Eth1ExecutionService.UpdateForkChoice(m.Ctx, forkchoiceRequest)
	// require.NoError(t, err)
	// require.NotNil(t, fcuReceipt)
	// require.Equal(t, execution.ExecutionStatus_Success, fcuReceipt.Status)
	// fmt.Println("fcuReceipt  ", fcuReceipt)

	// assert.Equal(t, -1, 0)
}

/*
Chain setup:

				Genesis Block
				      |
				      |
				Block 1 (Contract Creation)
				      |
				      |
				Block 2 (Contract Call)
				      |
				      |
				Block 3 (Contract Call)
				      |
				      |
				Block 4 (Funds Transfer)
				      |
				(fork validation benchmark)
					 /  \
                    /	 \
              Block A    Block B

Test setup:
- Blocks A and B contain N transactions each, where N is the number of subAccounts.
- Each transaction does Contract Call with different input data. The contract calculates Keccak256 of the input data and stores it in the storage.
- For each iteration:
  - validate and execute Block A
  - validate and execute Block B
  - clear validation cache
*/

func BenchmarkBlockExecution(b *testing.B) {
	var (
		signer           = types.LatestSignerForChainID(big.NewInt(1337))
		gasFeeCap        = uint256.NewInt(1)
		subAccountsCount = 100
	)

	tests := []struct {
		name        string
		useSilkworm bool
	}{
		{"useSilkworm = false", false},
		{"useSilkworm = true", true},
	}
	for _, bm := range tests {
		b.Run(bm.name, func(b *testing.B) {
			m, headBlock, subPrivateKeys, storageContractAddress := createChainWithStorageContract(b, bm.useSilkworm, subAccountsCount)

			chainA, err := core.GenerateChain(m.ChainConfig, headBlock, m.Engine, m.DB, 1, func(_ int, block *core.BlockGen) {
				for j := uint64(0); j < uint64(subAccountsCount); j++ {
					txn, err := types.SignTx(types.NewTransaction(0, storageContractAddress, uint256.NewInt(0), 90000, gasFeeCap, hexutil.EncodeTs(j)), *signer, subPrivateKeys[j])
					assert.NoError(b, err)
					block.AddTx(txn)
				}
			})
			assert.NoError(b, err)
			newBlockA := chainA.Blocks[0]
			request := &execution.InsertBlocksRequest{
				Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{newBlockA}),
			}
			result, err := m.Eth1ExecutionService.InsertBlocks(m.Ctx, request)
			require.NoError(b, err)
			require.NotNil(b, result)
			require.Equal(b, result.Result, execution.ExecutionStatus_Success)

			chainB, err := core.GenerateChain(m.ChainConfig, headBlock, m.Engine, m.DB, 1, func(_ int, block *core.BlockGen) {
				for j := uint64(0); j < uint64(subAccountsCount); j++ {
					txn, err := types.SignTx(types.NewTransaction(0, storageContractAddress, uint256.NewInt(0), 90000, gasFeeCap, hexutil.EncodeTs(j+1000)), *signer, subPrivateKeys[j])
					assert.NoError(b, err)
					block.AddTx(txn)
				}
			})
			assert.NoError(b, err)
			newBlockB := chainB.Blocks[0]
			request = &execution.InsertBlocksRequest{
				Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{newBlockB}),
			}
			result, err = m.Eth1ExecutionService.InsertBlocks(m.Ctx, request)
			require.NoError(b, err)
			require.NotNil(b, result)
			require.Equal(b, result.Result, execution.ExecutionStatus_Success)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				validationRequest := &execution.ValidationRequest{
					Hash:   gointerfaces.ConvertHashToH256(newBlockA.Hash()),
					Number: newBlockA.Number().Uint64(),
				}
				validationResult, err := m.Eth1ExecutionService.ValidateChain(m.Ctx, validationRequest)
				require.NoError(b, err)
				require.NotNil(b, validationResult)
				require.Equal(b, validationResult.ValidationStatus, execution.ExecutionStatus_Success)

				validationRequest = &execution.ValidationRequest{
					Hash:   gointerfaces.ConvertHashToH256(newBlockB.Hash()),
					Number: newBlockA.Number().Uint64(),
				}
				validationResult, err = m.Eth1ExecutionService.ValidateChain(m.Ctx, validationRequest)
				require.NoError(b, err)
				require.NotNil(b, validationResult)
				require.Equal(b, validationResult.ValidationStatus, execution.ExecutionStatus_Success)

				m.ForkValidator.ClearValidation()
			}

			m.Close()
		})
	}
}

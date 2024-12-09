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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus/clique"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/ethdb/privateapi"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/builder"
	"github.com/erigontech/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func TestBlockExecution1(t *testing.T) {
	// Initialize a Clique chain with a single signer
	var (
		cliqueDB = memdb.NewTestDB(t, kv.ConsensusDB)
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr     = crypto.PubkeyToAddress(key.PublicKey)
		engine   = clique.New(params.AllCliqueProtocolChanges, params.CliqueSnapshot, cliqueDB, log.New())
		signer   = types.LatestSignerForChainID(nil)
	)
	genspec := &types.Genesis{
		ExtraData: make([]byte, clique.ExtraVanity+length.Addr+clique.ExtraSeal),
		Alloc: map[libcommon.Address]types.GenesisAccount{
			addr: {Balance: big.NewInt(10000000000000000)},
		},
		Config: params.AllCliqueProtocolChanges,
	}
	copy(genspec.ExtraData[clique.ExtraVanity:], addr[:])
	checkStateRoot := true
	m := mock.MockWithGenesisEngine(t, genspec, engine, false, checkStateRoot)

	//**********************************
	// Start the private RPC server
	backend := eth.NewEthereum(
		m.Ctx,
		m.Cancel,
		m.EthConfig,
		m.DB,
		m.Log)
	latestBlockBuiltStore := builder.NewLatestBlockBuiltStore()
	ethBackendRPC := privateapi.NewEthBackendServer(context.Background(), backend, m.DB, m.Notifications, m.BlockReader, m.Log, latestBlockBuiltStore)
	privateRpcApi, err := privateapi.StartGrpc(
		m.RemoteKvServer,
		ethBackendRPC,
		nil,              //backend.txPoolGrpcServer,
		nil,              //miningRPC,
		nil,              //bridgeRPC,
		nil,              //heimdallRPC,
		"127.0.0.1:9090", //stack.Config().PrivateApiAddr,
		1,                //stack.Config().PrivateApiRateLimit,
		nil,              //creds,
		true,             //stack.Config().HealthCheck,
		m.Log)
	require.NoError(t, err)
	require.NotNil(t, privateRpcApi)
	//**********************************

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
		tx, err := types.SignTx(types.NewTransaction(block.TxNonce(addr), libcommon.Address{0x00}, new(uint256.Int), params.TxGas, baseFee, nil), *signer, key)
		if err != nil {
			panic(err)
		}
		block.AddTxWithChain(getHeader, engine, tx)
		// }
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
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

	// Insert the first two blocks and make sure the chain is valid
	if err := m.InsertChain(chain.Slice(0, 3)); err != nil {
		t.Fatalf("failed to insert initial blocks: %v", err)
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

	newBlock := chain.Blocks[1]

	validationRequest := &execution.ValidationRequest{
		Hash:   gointerfaces.ConvertHashToH256(newBlock.Hash()),
		Number: newBlock.Number().Uint64(),
	}

	validationResult, err := m.Eth1ExecutionService.ValidateChain(context.Background(), validationRequest)
	require.NoError(t, err)
	require.NotNil(t, validationResult)
	require.Equal(t, validationResult.ValidationStatus, execution.ExecutionStatus_Success)

	//update forkchoice
	forkchoiceRequest := &execution.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(newBlock.Hash()),
		Timeout:            10_000,
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(m.Genesis.Hash()),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(m.Genesis.Hash()),
	}

	fcuReceipt, err := m.Eth1ExecutionService.UpdateForkChoice(context.Background(), forkchoiceRequest)
	require.NoError(t, err)
	require.NotNil(t, fcuReceipt)
	require.Equal(t, execution.ExecutionStatus_Success, fcuReceipt.Status)

	// ****** HTTP Test
	// req, err := http.NewRequest(http.MethodGet, "http://localhost:9090", nil)
	// require.NoError(t, err)
	// // req.Header.Add("X-ERIGON-HEALTHCHECK", "synced")
	// response, err := http.DefaultClient.Do(req)
	// require.NoError(t, err)
	// require.NotNil(t, response)
	// require.Equal(t, http.StatusOK, response.StatusCode)
	// out, err := io.ReadAll(response.Body)
	// require.NoError(t, err)
	// fmt.Println(string(out))
	// ******
}

func TestBlockExecution2(t *testing.T) {
	// Initialize a Clique chain with a single signer
	logger := log.Root()
	logger.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StdoutHandler))
	var (
		cliqueDB = memdb.NewTestDB(t, kv.ConsensusDB)
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr     = crypto.PubkeyToAddress(key.PublicKey)
		engine   = clique.New(params.AllCliqueProtocolChanges, params.CliqueSnapshot, cliqueDB, logger)
		signer   = types.LatestSignerForChainID(nil)
	)
	genspec := &types.Genesis{
		ExtraData: make([]byte, clique.ExtraVanity+length.Addr+clique.ExtraSeal),
		Alloc: map[libcommon.Address]types.GenesisAccount{
			addr: {Balance: big.NewInt(10000000000000000)},
		},
		Config: params.AllCliqueProtocolChanges,
	}

	copy(genspec.ExtraData[clique.ExtraVanity:], addr[:])
	checkStateRoot := true

	m := mock.MockWithGenesisEngine(t, genspec, engine, false, checkStateRoot)

	// Generate a batch of blocks, each properly signed
	getHeader := func(hash libcommon.Hash, number uint64) (h *types.Header) {
		response, err := m.Eth1ExecutionService.GetHeader(m.Ctx,
			&execution.GetSegmentRequest{
				BlockHash:   gointerfaces.ConvertHashToH256(hash),
				BlockNumber: &number,
			})
		require.NoError(t, err)
		require.NotNil(t, response)
		require.NotNil(t, response.Header)

		h, err = eth1_utils.HeaderRpcToHeader(response.Header)
		require.NoError(t, err)
		return h
	}

	currentHeadBlock := m.Genesis

	chain, err := core.GenerateChain(m.ChainConfig, currentHeadBlock, m.Engine, m.DB, 1, func(i int, block *core.BlockGen) {
		block.SetDifficulty(clique.DiffInTurn)

		baseFee, _ := uint256.FromBig(block.GetHeader().BaseFee)
		tx, err := types.SignTx(types.NewTransaction(block.TxNonce(addr), libcommon.Address{0x00}, new(uint256.Int), params.TxGas, baseFee, nil), *signer, key)
		if err != nil {
			panic(err)
		}
		block.AddTxWithChain(getHeader, engine, tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
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

	insertBlocksRequest := &execution.InsertBlocksRequest{
		Blocks: eth1_utils.ConvertBlocksToRPC(chain.Blocks),
	}

	newBlock := chain.Blocks[0]

	result, err := m.Eth1ExecutionService.InsertBlocks(context.Background(), insertBlocksRequest)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, result.Result, execution.ExecutionStatus_Success)

	validationRequest := &execution.ValidationRequest{
		Hash:   gointerfaces.ConvertHashToH256(newBlock.Hash()),
		Number: newBlock.Number().Uint64(),
	}

	validationResult, err := m.Eth1ExecutionService.ValidateChain(context.Background(), validationRequest)
	require.NoError(t, err)
	require.NotNil(t, validationResult)
	require.Equal(t, validationResult.ValidationStatus, execution.ExecutionStatus_Success)

	forkchoiceRequest := &execution.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(newBlock.Hash()),
		Timeout:            0,
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(m.Genesis.Hash()),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(m.Genesis.Hash()),
	}

	fcuReceipt, err := m.Eth1ExecutionService.UpdateForkChoice(m.Ctx, forkchoiceRequest)
	require.NoError(t, err)
	require.NotNil(t, fcuReceipt)
	require.Equal(t, execution.ExecutionStatus_Success, fcuReceipt.Status)
}

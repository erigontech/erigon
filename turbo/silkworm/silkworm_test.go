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

package silkworm

import (
	"context"
	"math/big"
	"runtime"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"

	"github.com/erigontech/mdbx-go/mdbx"
	mdbx2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"

	"github.com/ledgerwatch/erigon/core/rawdb"
)

func skipOnUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOARCH != "amd64" {
		t.Skip("Silkworm is only supported on linux/amd64")
	}
}

func setup(t *testing.T) (*ForkValidatorService, *types.Block) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	dirs := datadir.New(t.TempDir())
	num_contexts := 1
	log_level := log.LvlError

	silkworm, err := New(dirs.DataDir, mdbx.Version(), uint32(num_contexts), log_level)
	require.NoError(t, err)

	db := mdbx2.NewMDBX(log.New()).Path(dirs.DataDir).Exclusive().InMem(dirs.DataDir).Label(kv.ChainDB).MustOpen()

	network := "mainnet"

	genesis := core.GenesisBlockByChainName(network)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, genesisBlock, err := core.WriteGenesisBlock(tx, genesis, nil, dirs.Chaindata, log.New())
	require.NoError(t, err)
	expect := params.GenesisHashByChainName(network)
	require.NotNil(t, expect, network)
	require.EqualValues(t, genesisBlock.Hash(), *expect, network)
	tx.Commit()

	silkwormService := NewForkValidatorService(silkworm, db, ForkValidatorTestSettings())

	return silkwormService, genesisBlock
}

func SampleBlock(parent *types.Header) *types.Block {
	return types.NewBlockWithHeader(&types.Header{
		Number:     new(big.Int).Add(parent.Number, big.NewInt(1)),
		Difficulty: new(big.Int).Add(parent.Number, big.NewInt(17000000000)),
		ParentHash: parent.Hash(),
		//Beneficiary: crypto.PubkeyToAddress(crypto.MustGenerateKey().PublicKey),
		TxHash:      types.EmptyRootHash,
		ReceiptHash: types.EmptyRootHash,
		GasLimit:    10000000,
		GasUsed:     0,
		Time:        parent.Time + 12,
	})
}

func ForkValidatorTestSettings() ForkValidatorSettings {
	return ForkValidatorSettings{
		BatchSize:               512 * 1024 * 1024,
		EtlBufferSize:           256 * 1024 * 1024,
		SyncLoopThrottleSeconds: 0,
		StopBeforeSendersStage:  true,
	}
}

func TestSilkwormInitialization(t *testing.T) {
	skipOnUnsupportedPlatform(t)
	forkValidatorService, _ := setup(t)
	require.NotNil(t, forkValidatorService)
	require.NotNil(t, forkValidatorService.silkworm)
	require.NotNil(t, forkValidatorService.db)

	forkValidatorService.silkworm.Close()
	forkValidatorService.silkworm = nil
	forkValidatorService.db.Close()
}

func TestSilkwormForkValidatorInitialization(t *testing.T) {
	skipOnUnsupportedPlatform(t)
	forkValidatorService, _ := setup(t)

	err := forkValidatorService.Start()
	require.NoError(t, err)

	forkValidatorService.silkworm.Close()
	forkValidatorService.silkworm = nil
	forkValidatorService.db.Close()
}

func TestSilkwormForkValidatorTermination(t *testing.T) {
	skipOnUnsupportedPlatform(t)
	forkValidatorService, _ := setup(t)

	err := forkValidatorService.Start()
	require.NoError(t, err)

	err = forkValidatorService.Stop()
	require.NoError(t, err)

	forkValidatorService.silkworm.Close()
	forkValidatorService.silkworm = nil
	forkValidatorService.db.Close()
}

func TestSilkwormVerifyChainSingleBlock(t *testing.T) {
	skipOnUnsupportedPlatform(t)
	forkValidatorService, genesisBlock := setup(t)

	tx, err := forkValidatorService.db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock := SampleBlock(genesisBlock.Header())

	err = rawdb.WriteBlock(tx, newBlock)
	require.NoError(t, err)
	tx.Commit()

	err = forkValidatorService.Start()
	require.NoError(t, err)

	validationResult, err := forkValidatorService.VerifyChain(newBlock.Header().Hash())
	require.NoError(t, err)
	require.True(t, validationResult.ExecutionStatus == 0)
	require.Equal(t, newBlock.Header().Hash(), validationResult.LastValidHash)

	forkValidatorService.silkworm.Close()
	forkValidatorService.silkworm = nil
	forkValidatorService.db.Close()
}

func TestSilkwormForkChoiceUpdateSingleBlock(t *testing.T) {
	skipOnUnsupportedPlatform(t)
	forkValidatorService, genesisBlock := setup(t)

	tx, err := forkValidatorService.db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock := SampleBlock(genesisBlock.Header())

	err = rawdb.WriteBlock(tx, newBlock)
	require.NoError(t, err)
	tx.Commit()

	err = forkValidatorService.Start()
	require.NoError(t, err)

	validationResult, err := forkValidatorService.VerifyChain(newBlock.Header().Hash())
	require.NoError(t, err)
	require.True(t, validationResult.ExecutionStatus == 0)
	require.Equal(t, newBlock.Header().Hash(), validationResult.LastValidHash)

	err = forkValidatorService.ForkChoiceUpdate(newBlock.Header().Hash(), common.Hash{}, common.Hash{})
	require.NoError(t, err)

	forkValidatorService.silkworm.Close()
	forkValidatorService.silkworm = nil
	forkValidatorService.db.Close()
}

func TestSilkwormVerifyChainTwoBlocks(t *testing.T) {
	skipOnUnsupportedPlatform(t)
	forkValidatorService, genesisBlock := setup(t)

	tx, err := forkValidatorService.db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock1 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock1)
	require.NoError(t, err)

	newBlock2 := SampleBlock(newBlock1.Header())
	err = rawdb.WriteBlock(tx, newBlock2)
	require.NoError(t, err)

	tx.Commit()

	err = forkValidatorService.Start()
	require.NoError(t, err)

	validationResult, err := forkValidatorService.VerifyChain(newBlock2.Header().Hash())
	require.NoError(t, err)
	require.True(t, validationResult.ExecutionStatus == 0)
	require.Equal(t, newBlock2.Header().Hash(), validationResult.LastValidHash)

	validationResult, err = forkValidatorService.VerifyChain(newBlock1.Header().Hash())
	require.NoError(t, err)
	require.True(t, validationResult.ExecutionStatus == 0)
	require.Equal(t, newBlock2.Header().Hash(), validationResult.LastValidHash)

	forkValidatorService.silkworm.Close()
	forkValidatorService.silkworm = nil
	forkValidatorService.db.Close()
}

func TestSilkwormVerifyTwoChains(t *testing.T) {
	skipOnUnsupportedPlatform(t)
	forkValidatorService, genesisBlock := setup(t)

	tx, err := forkValidatorService.db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock1 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock1)
	require.NoError(t, err)

	newBlock2 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock2)
	require.NoError(t, err)

	tx.Commit()

	err = forkValidatorService.Start()
	require.NoError(t, err)

	validationResult, err := forkValidatorService.VerifyChain(newBlock1.Header().Hash())
	require.NoError(t, err)
	require.True(t, validationResult.ExecutionStatus == 0)
	require.Equal(t, newBlock1.Header().Hash(), validationResult.LastValidHash)

	validationResult, err = forkValidatorService.VerifyChain(newBlock2.Header().Hash())
	require.NoError(t, err)
	require.True(t, validationResult.ExecutionStatus == 0)
	require.Equal(t, newBlock2.Header().Hash(), validationResult.LastValidHash)

	forkValidatorService.silkworm.Close()
	forkValidatorService.silkworm = nil
	forkValidatorService.db.Close()
}

func TestSilkwormForkChoiceUpdateTwoChains(t *testing.T) {
	skipOnUnsupportedPlatform(t)
	forkValidatorService, genesisBlock := setup(t)

	tx, err := forkValidatorService.db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()

	newBlock1 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock1)
	require.NoError(t, err)

	newBlock2 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock2)
	require.NoError(t, err)

	tx.Commit()

	err = forkValidatorService.Start()
	require.NoError(t, err)

	validationResult, err := forkValidatorService.VerifyChain(newBlock1.Header().Hash())
	require.NoError(t, err)
	require.True(t, validationResult.ExecutionStatus == 0)
	require.Equal(t, newBlock1.Header().Hash(), validationResult.LastValidHash)

	err = forkValidatorService.ForkChoiceUpdate(newBlock2.Header().Hash(), newBlock2.Header().Hash(), newBlock2.Header().Hash())
	require.NoError(t, err)

	forkValidatorService.silkworm.Close()
	forkValidatorService.silkworm = nil
	forkValidatorService.db.Close()
}

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

package jsonrpc

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func TestFillTransactionFillsDefaults(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Raw)
	require.NotNil(t, result.Tx)
	require.Greater(t, uint64(result.Tx.Gas), uint64(0))
	require.True(t, result.Tx.GasPrice != nil || result.Tx.MaxFeePerGas != nil)
}

func TestFillTransactionConflictingFees(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gasPrice := (*hexutil.Big)(big.NewInt(1e9))
	maxFeePerGas := (*hexutil.Big)(big.NewInt(2e9))

	_, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From:         &from,
		To:           &to,
		GasPrice:     gasPrice,
		MaxFeePerGas: maxFeePerGas,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "gasPrice")
}

func TestFillTransactionChainIDMismatch(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	wrongChainID := (*hexutil.Big)(big.NewInt(999999))

	_, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From:    &from,
		To:      &to,
		ChainID: wrongChainID,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "chainId")
}

func TestFillTransactionContractCreationNoData(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	_, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From: &from,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "contract creation")
}

func TestFillTransactionNoFrom(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		To: &to,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, hexutil.Uint64(0), result.Tx.Nonce, "nonce must default to 0 when From is absent")
}

func TestFillTransactionExplicitNoncePreserved(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	nonce := hexutil.Uint64(42)

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From:  &from,
		To:    &to,
		Nonce: &nonce,
	})
	require.NoError(t, err)
	require.Equal(t, nonce, result.Tx.Nonce, "explicit nonce must not be overwritten")
}

func TestFillTransactionExplicitGasPreserved(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(50000)

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
		Gas:  &gas,
	})
	require.NoError(t, err)
	require.Equal(t, gas, result.Tx.Gas, "explicit gas must not be overwritten by estimation")
}

func TestFillTransactionBlobPreCancun(t *testing.T) {
	// TestChainBerlinConfig has no Cancun (ExcessBlobGas == nil on head).
	// A blob tx request must return a clear error, not panic.
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	blobHash := common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000001")

	_, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From:                &from,
		To:                  &to,
		BlobVersionedHashes: []common.Hash{blobHash},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cancun")
}

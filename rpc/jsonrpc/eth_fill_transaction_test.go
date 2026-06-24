// Copyright 2026 The Erigon Authors
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
	"errors"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// errPoolClient simulates a txpool gRPC call failure.
type errPoolClient struct{ stubTxPoolClient }

func (errPoolClient) Nonce(_ context.Context, _ *txpoolproto.NonceRequest, _ ...grpc.CallOption) (*txpoolproto.NonceReply, error) {
	return nil, errors.New("pool unavailable")
}

// fixedNoncePoolClient always reports a specific pending nonce for the sender.
type fixedNoncePoolClient struct {
	stubTxPoolClient
	nonce uint64
}

func (c fixedNoncePoolClient) Nonce(_ context.Context, _ *txpoolproto.NonceRequest, _ ...grpc.CallOption) (*txpoolproto.NonceReply, error) {
	return &txpoolproto.NonceReply{Found: true, Nonce: c.nonce}, nil
}

// newLondonApiForTest returns an API backed by a fresh London-activated chain (genesis only).
// The genesis baseFee is params.InitialBaseFee (1_000_000_000 wei).
func newLondonApiForTest(t *testing.T) *APIImpl {
	londonCfg := &chain.Config{
		ChainID:               uint256.NewInt(1337),
		Rules:                 chain.EtHashRules,
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		MuirGlacierBlock:      common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		LondonBlock:           common.NewUint64(0),
		Ethash:                new(chain.EthashConfig),
	}
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(londonCfg))
	return newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)
}

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
	nonce := hexutil.Uint64(7)

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

func TestFillTransactionBlobPreCancunExplicitBlobFee(t *testing.T) {
	// Even with an explicit maxFeePerBlobGas, blob txs on a pre-Cancun chain must error.
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	blobHash := common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000001")
	gas := hexutil.Uint64(21000)

	_, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From:                &from,
		To:                  &to,
		Gas:                 &gas,
		BlobVersionedHashes: []common.Hash{blobHash},
		MaxFeePerBlobGas:    (*hexutil.Big)(big.NewInt(1e9)),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cancun")
}

func TestFillTransactionPoolErrorPropagates(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, errPoolClient{}, nil)

	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")

	_, err := api.FillTransaction(context.Background(), ethapi.CallArgs{From: &from, To: &to})
	require.Error(t, err, "pool gRPC error must propagate to the caller")
	require.Contains(t, err.Error(), "pool unavailable")
}

func TestFillTransactionGasPriceWithAccessListIsTypeOne(t *testing.T) {
	api := newLondonApiForTest(t)
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	al := types.AccessList{{Address: to, StorageKeys: nil}}

	// Erigon preserves an explicit accessList even when gasPrice is set (type 1),
	// unlike Geth which silently drops it to a LegacyTx (type 0).
	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		To:         &to,
		Gas:        &gas,
		GasPrice:   (*hexutil.Big)(big.NewInt(10_000_000_000)),
		AccessList: &al,
	})
	require.NoError(t, err)
	require.Equal(t, hexutil.Uint64(types.AccessListTxType), result.Tx.Type)
}

func TestFillTransactionUserGasAboveCapPreserved(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	// FillTransaction passes globalGasCap=0 to ToTransaction, so no capping occurs.
	userGas := hexutil.Uint64(10_000_000)

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
		Gas:  &userGas,
	})
	require.NoError(t, err)
	require.Equal(t, userGas, result.Tx.Gas, "user-provided gas must not be silently capped")
}

func TestFillTransactionPoolNonce(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	const pendingNonce = uint64(5)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, fixedNoncePoolClient{nonce: pendingNonce}, nil)

	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{From: &from, To: &to})
	require.NoError(t, err)
	require.Equal(t, hexutil.Uint64(pendingNonce+1), result.Tx.Nonce, "nonce must be pool nonce + 1")
}

func TestFillTransactionOnlyMaxFeePerGas(t *testing.T) {
	api := newLondonApiForTest(t)
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	maxFee := (*hexutil.Big)(new(big.Int).SetUint64(1_000_000_000_000_000_000)) // 1e18 wei

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		To:           &to,
		Gas:          &gas,
		MaxFeePerGas: maxFee,
	})
	require.NoError(t, err)
	require.NotNil(t, result.Tx.MaxPriorityFeePerGas, "oracle must fill maxPriorityFeePerGas")
	require.GreaterOrEqual(t, result.Tx.MaxFeePerGas.ToInt().Cmp(result.Tx.MaxPriorityFeePerGas.ToInt()), 0)
	require.Equal(t, hexutil.Uint64(types.DynamicFeeTxType), result.Tx.Type)
}

func TestFillTransactionOnlyMaxPriorityFeePerGas(t *testing.T) {
	api := newLondonApiForTest(t)
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	const userTip = int64(1_000_000_000) // 1 gwei
	// London genesis baseFee = params.InitialBaseFee = 1_000_000_000
	const initialBaseFee = int64(1_000_000_000)

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		To:                   &to,
		Gas:                  &gas,
		MaxPriorityFeePerGas: (*hexutil.Big)(big.NewInt(userTip)),
	})
	require.NoError(t, err)
	require.NotNil(t, result.Tx.MaxFeePerGas, "oracle must fill maxFeePerGas")
	require.Equal(t, userTip+2*initialBaseFee, result.Tx.MaxFeePerGas.ToInt().Int64())
	require.Equal(t, hexutil.Uint64(types.DynamicFeeTxType), result.Tx.Type)
}

func TestFillTransactionMaxFeePerGasTooLow(t *testing.T) {
	api := newLondonApiForTest(t)
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)

	_, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		To:                   &to,
		Gas:                  &gas,
		MaxFeePerGas:         (*hexutil.Big)(big.NewInt(1)),
		MaxPriorityFeePerGas: (*hexutil.Big)(big.NewInt(1_000_000_000)),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "maxFeePerGas")
}

func TestFillTransactionGasPricePostLondon(t *testing.T) {
	api := newLondonApiForTest(t)
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		To:       &to,
		Gas:      &gas,
		GasPrice: (*hexutil.Big)(big.NewInt(10_000_000_000)), // 10 gwei
	})
	require.NoError(t, err)
	require.Equal(t, hexutil.Uint64(types.LegacyTxType), result.Tx.Type, "explicit gasPrice must produce a legacy tx")
}

func TestFillTransactionBlobFeeUsesNextBlockExcess(t *testing.T) {
	// Genesis has a large ExcessBlobGas but zero BlobGasUsed.
	// CalcExcessBlobGas returns ExcessBlobGas - targetBlobGas, not ExcessBlobGas itself.
	// FillTransaction must use the former (next block's excess), not the latter.
	const headExcessBlobGas = uint64(10_000_000)

	cancunCfg := &chain.Config{
		ChainID:               uint256.NewInt(1337),
		Rules:                 chain.EtHashRules,
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		MuirGlacierBlock:      common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		LondonBlock:           common.NewUint64(0),
		ShanghaiTime:          common.NewUint64(0),
		CancunTime:            common.NewUint64(0),
		Ethash:                new(chain.EthashConfig),
	}

	excess := headExcessBlobGas
	blobUsed := uint64(0)
	gspec := &types.Genesis{
		Config:        cancunCfg,
		ExcessBlobGas: &excess,
		BlobGasUsed:   &blobUsed,
	}

	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec))
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	blobHash := common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000001")

	result, err := api.FillTransaction(context.Background(), ethapi.CallArgs{
		To:                   &to,
		Gas:                  &gas,
		BlobVersionedHashes:  []common.Hash{blobHash},
		MaxFeePerGas:         (*hexutil.Big)(big.NewInt(10_000_000_000)),
		MaxPriorityFeePerGas: (*hexutil.Big)(big.NewInt(1_000_000_000)),
	})
	require.NoError(t, err)
	require.NotNil(t, result.Tx.MaxFeePerBlobGas)

	head := m.Genesis.HeaderNoCopy()
	nextTime := head.Time + cancunCfg.SecondsPerSlot()
	nextExcess := misc.CalcExcessBlobGas(cancunCfg, head, nextTime)
	correctFee, err := misc.GetBlobGasPrice(cancunCfg, nextExcess, nextTime)
	require.NoError(t, err)
	expectedMaxFeePerBlobGas := new(big.Int).Lsh(correctFee.ToBig(), 1)
	require.Equal(t, expectedMaxFeePerBlobGas, result.Tx.MaxFeePerBlobGas.ToInt())

	// Sanity: the old approach (using head.ExcessBlobGas directly) gives a different, larger value.
	oldFee, err := misc.GetBlobGasPrice(cancunCfg, headExcessBlobGas, nextTime)
	require.NoError(t, err)
	oldMaxFeePerBlobGas := new(big.Int).Lsh(oldFee.ToBig(), 1)
	require.NotEqual(t, expectedMaxFeePerBlobGas, oldMaxFeePerBlobGas,
		"test scenario must differentiate CalcExcessBlobGas from head.ExcessBlobGas")
}

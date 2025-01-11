package opstack

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"

	//"github.com/erigontech/erigon-lib/fastlz"
	"github.com/holiman/uint256"

	"github.com/stretchr/testify/require"
)

// This file is based on op-geth
// https://github.com/ethereum-optimism/op-geth/commit/a290ca164a36c80a8d106d88bd482b6f82220bef

var (
	basefee  = uint256.NewInt(1000 * 1e6)
	overhead = uint256.NewInt(50)
	scalar   = uint256.NewInt(7 * 1e6)

	blobBasefee       = uint256.NewInt(10 * 1e6)
	basefeeScalar     = uint256.NewInt(2)
	blobBasefeeScalar = uint256.NewInt(3)

	// below are the expected cost func outcomes for the above parameter settings on the emptyTx
	// which is defined in transaction_test.go
	bedrockFee  = uint256.NewInt(11326000000000)
	regolithFee = uint256.NewInt(3710000000000)
	ecotoneFee  = uint256.NewInt(960900) // (480/16)*(2*16*1000 + 3*10) == 960900
	// the emptyTx is out of bounds for the linear regression so it uses the minimum size
	fjordFee = uint256.NewInt(3203000) // 100_000_000 * (2 * 1000 * 1e6 * 16 + 3 * 10 * 1e6) / 1e12

	bedrockGas      = uint256.NewInt(1618)
	regolithGas     = uint256.NewInt(530) // 530  = 1618 - (16*68)
	ecotoneGas      = uint256.NewInt(480)
	minimumFjordGas = uint256.NewInt(1600) // fastlz size of minimum txn, 100_000_000 * 16 / 1e6

	OptimismTestConfig = &chain.OptimismConfig{EIP1559Elasticity: 50, EIP1559Denominator: 10}

	// RollupCostData of emptyTx
	emptyTxRollupCostData = RollupCostData{Zeroes: 0, Ones: 30}
)

func TestBedrockL1CostFunc(t *testing.T) {
	costFunc0 := newL1CostFuncPreEcotoneHelper(basefee, overhead, scalar, false /*isRegolith*/)
	costFunc1 := newL1CostFuncPreEcotoneHelper(basefee, overhead, scalar, true)

	c0, g0 := costFunc0(emptyTxRollupCostData) // pre-Regolith
	c1, g1 := costFunc1(emptyTxRollupCostData)

	require.Equal(t, bedrockFee, c0)
	require.Equal(t, bedrockGas, g0) // gas-used

	require.Equal(t, regolithFee, c1)
	require.Equal(t, regolithGas, g1)
}

func TestEcotoneL1CostFunc(t *testing.T) {
	costFunc := newL1CostFuncEcotone(basefee, blobBasefee, basefeeScalar, blobBasefeeScalar)

	c0, g0 := costFunc(emptyTxRollupCostData)

	require.Equal(t, ecotoneGas, g0)
	require.Equal(t, ecotoneFee, c0)
}

func TestFjordL1CostFuncMinimumBounds(t *testing.T) {
	costFunc := NewL1CostFuncFjord(
		basefee,
		blobBasefee,
		basefeeScalar,
		blobBasefeeScalar,
	)

	// Minimum size transactions:
	// -42.5856 + 0.8365*110 = 49.4294
	// -42.5856 + 0.8365*150 = 82.8894
	// -42.5856 + 0.8365*170 = 99.6194
	for _, fastLzsize := range []uint64{100, 150, 170} {
		c, g := costFunc(RollupCostData{
			FastLzSize: fastLzsize,
		})

		require.Equal(t, minimumFjordGas, g)
		require.Equal(t, fjordFee, c)
	}

	// Larger size transactions:
	// -42.5856 + 0.8365*171 = 100.4559
	// -42.5856 + 0.8365*175 = 108.8019
	// -42.5856 + 0.8365*200 = 124.7144
	for _, fastLzsize := range []uint64{171, 175, 200} {
		c, g := costFunc(RollupCostData{
			FastLzSize: fastLzsize,
		})

		require.Greater(t, g.Uint64(), minimumFjordGas.Uint64())
		require.Greater(t, c.Uint64(), fjordFee.Uint64())
	}
}

// TestFjordL1CostSolidityParity tests that the cost function for the fjord upgrade matches a Solidity
// test to ensure the outputs are the same.
func TestFjordL1CostSolidityParity(t *testing.T) {
	costFunc := NewL1CostFuncFjord(
		uint256.NewInt(2*1e6),
		uint256.NewInt(3*1e6),
		uint256.NewInt(20),
		uint256.NewInt(15),
	)

	c0, g0 := costFunc(RollupCostData{
		FastLzSize: 235,
	})

	require.Equal(t, uint256.NewInt(2463), g0)
	require.Equal(t, uint256.NewInt(105484), c0)
}

func TestExtractBedrockGasParams(t *testing.T) {
	regolithTime := uint64(1)
	config := &chain.Config{
		Optimism:     OptimismTestConfig,
		RegolithTime: big.NewInt(1),
	}

	data := getBedrockL1Attributes(basefee, overhead, scalar)

	gasParamsPreRegolith, err := ExtractL1GasParams(config, regolithTime-1, data)
	require.NoError(t, err)
	costFuncPreRegolith := gasParamsPreRegolith.CostFunc

	// Function should continue to succeed even with extra data (that just gets ignored) since we
	// have been testing the data size is at least the expected number of bytes instead of exactly
	// the expected number of bytes. It's unclear if this flexibility was intentional, but since
	// it's been in production we shouldn't change this behavior.
	data = append(data, []byte{0xBE, 0xEE, 0xEE, 0xFF}...) // tack on garbage data
	gasParamsRegolith, err := ExtractL1GasParams(config, regolithTime, data)
	require.NoError(t, err)
	costFuncRegolith := gasParamsRegolith.CostFunc

	c, _ := costFuncPreRegolith(emptyTxRollupCostData)
	require.Equal(t, bedrockFee, c)

	c, _ = costFuncRegolith(emptyTxRollupCostData)
	require.Equal(t, regolithFee, c)

	// try to extract from data which has not enough params, should get error.
	data = data[:len(data)-4-32]
	_, err = ExtractL1GasParams(config, regolithTime, data)
	require.Error(t, err)
}

func TestExtractEcotoneGasParams(t *testing.T) {
	zeroTime := big.NewInt(0)
	// create a config where ecotone upgrade is active
	config := &chain.Config{
		Optimism:     OptimismTestConfig,
		RegolithTime: zeroTime,
		EcotoneTime:  zeroTime,
	}
	require.True(t, config.IsOptimismEcotone(zeroTime.Uint64()))

	data := getEcotoneL1Attributes(basefee, blobBasefee, basefeeScalar, blobBasefeeScalar)

	gasParams, err := ExtractL1GasParams(config, 0, data)
	require.NoError(t, err)
	costFunc := gasParams.CostFunc

	c, g := costFunc(emptyTxRollupCostData)

	require.Equal(t, ecotoneGas, g)
	require.Equal(t, ecotoneFee, c)

	// make sure wrong amont of data results in error
	data = append(data, 0x00) // tack on garbage byte
	_, err = extractL1GasParamsPostEcotone(data)
	require.Error(t, err)
}

func TestExtractFjordGasParams(t *testing.T) {
	zeroTime := big.NewInt(0)
	// create a config where fjord is active
	config := &chain.Config{
		Optimism:     OptimismTestConfig,
		RegolithTime: zeroTime,
		EcotoneTime:  zeroTime,
		FjordTime:    zeroTime,
	}
	require.True(t, config.IsOptimismFjord(zeroTime.Uint64()))

	data := getEcotoneL1Attributes(
		basefee, blobBasefee, basefeeScalar, blobBasefeeScalar,
	)

	gasparams, err := ExtractL1GasParams(config, zeroTime.Uint64(), data)
	require.NoError(t, err)
	costFunc := gasparams.CostFunc

	c, g := costFunc(emptyTxRollupCostData)

	require.Equal(t, minimumFjordGas, g)
	require.Equal(t, fjordFee, c)
}

// make sure the first block of the ecotone upgrade is properly detected, and invokes the bedrock
// cost function appropriately
func TestFirstBlockEcotoneGasParams(t *testing.T) {
	zeroTime := big.NewInt(0)
	// create a config where ecotone upgrade is active
	config := &chain.Config{
		Optimism:     OptimismTestConfig,
		RegolithTime: zeroTime,
		EcotoneTime:  zeroTime,
	}
	require.True(t, config.IsOptimismEcotone(0))

	data := getBedrockL1Attributes(basefee, overhead, scalar)

	oldGasParam, err := ExtractL1GasParams(config, 0, data)
	require.NoError(t, err)
	oldCostFunc := oldGasParam.CostFunc
	c, g := oldCostFunc(emptyTxRollupCostData)
	require.Equal(t, regolithGas, g)
	require.Equal(t, regolithFee, c)
}

func getBedrockL1Attributes(basefee, overhead, scalar *uint256.Int) []byte {
	uint256Bytes := make([]byte, 32)
	ignored := big.NewInt(1234)
	data := []byte{}
	data = append(data, BedrockL1AttributesSelector...)
	data = append(data, ignored.FillBytes(uint256Bytes)...)          // arg 0
	data = append(data, ignored.FillBytes(uint256Bytes)...)          // arg 1
	data = append(data, basefee.ToBig().FillBytes(uint256Bytes)...)  // arg 2
	data = append(data, ignored.FillBytes(uint256Bytes)...)          // arg 3
	data = append(data, ignored.FillBytes(uint256Bytes)...)          // arg 4
	data = append(data, ignored.FillBytes(uint256Bytes)...)          // arg 5
	data = append(data, overhead.ToBig().FillBytes(uint256Bytes)...) // arg 6
	data = append(data, scalar.ToBig().FillBytes(uint256Bytes)...)   // arg 7
	return data
}

func getEcotoneL1Attributes(basefee, blobBasefee, basefeeScalar, blobBasefeeScalar *uint256.Int) []byte {
	ignored := big.NewInt(1234)
	data := []byte{}
	uint256Bytes := make([]byte, 32)
	uint64Bytes := make([]byte, 8)
	uint32Bytes := make([]byte, 4)
	data = append(data, EcotoneL1AttributesSelector...)
	data = append(data, basefeeScalar.ToBig().FillBytes(uint32Bytes)...)
	data = append(data, blobBasefeeScalar.ToBig().FillBytes(uint32Bytes)...)
	data = append(data, ignored.FillBytes(uint64Bytes)...)
	data = append(data, ignored.FillBytes(uint64Bytes)...)
	data = append(data, ignored.FillBytes(uint64Bytes)...)
	data = append(data, basefee.ToBig().FillBytes(uint256Bytes)...)
	data = append(data, blobBasefee.ToBig().FillBytes(uint256Bytes)...)
	data = append(data, ignored.FillBytes(uint256Bytes)...)
	data = append(data, ignored.FillBytes(uint256Bytes)...)
	return data
}

type testStateGetter struct {
	basefee, blobBasefee, overhead, scalar *uint256.Int
	basefeeScalar, blobBasefeeScalar       uint32
}

func (sg *testStateGetter) GetState(addr common.Address, key *common.Hash, value *uint256.Int) {
	switch *key {
	case L1BaseFeeSlot:
		value.Set(sg.basefee)
	case OverheadSlot:
		value.Set(sg.overhead)
	case ScalarSlot:
		value.Set(sg.scalar)
	case L1BlobBaseFeeSlot:
		value.Set(sg.blobBasefee)
	case L1FeeScalarsSlot:
		// fetch Ecotone fee scalars
		offset := scalarSectionStart
		buf := common.Hash{}
		binary.BigEndian.PutUint32(buf[offset:offset+4], sg.basefeeScalar)
		binary.BigEndian.PutUint32(buf[offset+4:offset+8], sg.blobBasefeeScalar)
		value.SetBytes(buf.Bytes())
	default:
		panic("unknown slot")
	}
}

// TestNewL1CostFunc tests that the appropriate cost function is selected based on the
// configuration and statedb values.
func TestNewL1CostFunc(t *testing.T) {
	time := uint64(10)
	timeInFuture := uint64(20)
	config := &chain.Config{
		Optimism: OptimismTestConfig,
	}
	statedb := &testStateGetter{
		basefee:           basefee,
		overhead:          overhead,
		scalar:            scalar,
		blobBasefee:       blobBasefee,
		basefeeScalar:     uint32(basefeeScalar.Uint64()),
		blobBasefeeScalar: uint32(blobBasefeeScalar.Uint64()),
	}

	costFunc := NewL1CostFunc(config, statedb)
	require.NotNil(t, costFunc)

	// empty cost data should result in nil fee
	fee := costFunc(RollupCostData{}, time)
	require.Nil(t, fee)

	// emptyTx fee w/ bedrock config should be the bedrock fee
	fee = costFunc(emptyTxRollupCostData, time)
	require.NotNil(t, fee)
	require.Equal(t, bedrockFee, fee)

	// emptyTx fee w/ regolith config should be the regolith fee
	config.RegolithTime = new(big.Int).SetUint64(time)
	costFunc = NewL1CostFunc(config, statedb)
	require.NotNil(t, costFunc)
	fee = costFunc(emptyTxRollupCostData, time)
	require.NotNil(t, fee)
	require.Equal(t, regolithFee, fee)

	// emptyTx fee w/ ecotone config should be the ecotone fee
	config.EcotoneTime = new(big.Int).SetUint64(time)
	costFunc = NewL1CostFunc(config, statedb)
	fee = costFunc(emptyTxRollupCostData, time)
	require.NotNil(t, fee)
	require.Equal(t, ecotoneFee, fee)

	// emptyTx fee w/ fjord config should be the fjord fee
	config.FjordTime = new(big.Int).SetUint64(time)
	costFunc = NewL1CostFunc(config, statedb)
	fee = costFunc(emptyTxRollupCostData, time)
	require.NotNil(t, fee)
	require.Equal(t, fjordFee, fee)

	// emptyTx fee w/ ecotone config, but simulate first ecotone block by blowing away the ecotone
	// params. Should result in regolith fee.
	config.FjordTime = new(big.Int).SetUint64(timeInFuture)
	statedb.basefeeScalar = 0
	statedb.blobBasefeeScalar = 0
	statedb.blobBasefee = new(uint256.Int)
	costFunc = NewL1CostFunc(config, statedb)
	fee = costFunc(emptyTxRollupCostData, time)
	require.NotNil(t, fee)
	require.Equal(t, regolithFee, fee)

	// emptyTx fee w/ fjord config, but simulate first ecotone block by blowing away the ecotone
	// params. Should result in regolith fee.
	config.EcotoneTime = new(big.Int).SetUint64(time)
	config.FjordTime = new(big.Int).SetUint64(time)
	statedb.basefeeScalar = 0
	statedb.blobBasefeeScalar = 0
	statedb.blobBasefee = new(uint256.Int)
	costFunc = NewL1CostFunc(config, statedb)
	fee = costFunc(emptyTxRollupCostData, time)
	require.NotNil(t, fee)
	require.Equal(t, regolithFee, fee)
}

func TestFlzCompressLen(t *testing.T) {
	var (
		// We cannot import erigon librarys to erigon-lib. Temporarily hard code.
		/*
			var emptyTx = NewTransaction(
				0,
				libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"),
				uint256.NewInt(0), 0, uint256.NewInt(0),
				nil,
			)
		*/
		// marshalled upper legacy tx
		emptyTxBytes, _   = hex.DecodeString("dd80808094095e7baea6a6c7c4c2dfeb977efac326af552d878080808080")
		contractCallTxStr = "02f901550a758302df1483be21b88304743f94f8" +
			"0e51afb613d764fa61751affd3313c190a86bb870151bd62fd12adb8" +
			"e41ef24f3f0000000000000000000000000000000000000000000000" +
			"00000000000000006e000000000000000000000000af88d065e77c8c" +
			"c2239327c5edb3a432268e5831000000000000000000000000000000" +
			"000000000000000000000000000003c1e50000000000000000000000" +
			"00000000000000000000000000000000000000000000000000000000" +
			"000000000000000000000000000000000000000000000000a0000000" +
			"00000000000000000000000000000000000000000000000000000000" +
			"148c89ed219d02f1a5be012c689b4f5b731827bebe00000000000000" +
			"0000000000c001a033fd89cb37c31b2cba46b6466e040c61fc9b2a36" +
			"75a7f5f493ebd5ad77c497f8a07cdf65680e238392693019b4092f61" +
			"0222e71b7cec06449cb922b93b6a12744e"
		contractCallTx, _ = hex.DecodeString(contractCallTxStr)
	)

	testCases := []struct {
		input       []byte
		expectedLen uint32
	}{
		// empty input
		{[]byte{}, 0},
		// all 1 inputs
		{bytes.Repeat([]byte{1}, 1000), 21},
		// all 0 inputs
		{make([]byte, 1000), 21},
		// empty tx input
		{emptyTxBytes, 31},
		// contract call tx: https://optimistic.etherscan.io/tx/0x8eb9dd4eb6d33f4dc25fb015919e4b1e9f7542f9b0322bf6622e268cd116b594
		{contractCallTx, 202},
	}

	for _, tc := range testCases {
		output := FlzCompressLen(tc.input)
		require.Equal(t, tc.expectedLen, output)
	}
}

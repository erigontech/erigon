package taiko_test

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	params2 "github.com/erigontech/erigon-lib/chain/params"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/consensus/taiko"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testL2RollupAddress = common.HexToAddress("0x79fcdef22feed20eddacbb2587640e45491b757f")
	goldenTouchKey, _   = crypto.HexToECDSA("92954368afd3caa1f3ce3ead0069c1af414054aefe1ef9aeacc1bf426222ce38")
	testKey, _          = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr            = crypto.PubkeyToAddress(testKey.PublicKey)
	testContract        = common.HexToAddress("0xbeef")
	testEmpty           = common.HexToAddress("0xeeee")
	testSlot            = common.HexToHash("0xdeadbeef")
	testValue           = crypto.Keccak256Hash(testSlot[:])
	testBalance         = big.NewInt(2e15)
	nBlocks             = 1
	config              *chain.Config
	genesis             *types.Genesis
	txs                 []*types.Transaction
	testEngine          *taiko.Taiko
)

func init() {

	config = params.TestChainConfig
	config.GrayGlacierBlock = nil
	config.ArrowGlacierBlock = nil
	config.Ethash = nil
	config.Taiko = true

	// taikoL2AddressPrefix := strings.TrimPrefix(config.ChainID.String(), "0")

	// taikoL2Address := common.HexToAddress(
	// 	"0x" +
	// 		taikoL2AddressPrefix +
	// 		strings.Repeat("0", length.Addr*2-len(taikoL2AddressPrefix)-len(taiko.TaikoL2AddressSuffix)) +
	// 		taiko.TaikoL2AddressSuffix,
	// )

	genesis = &types.Genesis{
		Config: params.TaikoChainConfig,
		Alloc: types.GenesisAlloc{
			testAddr:     {Balance: testBalance, Storage: map[common.Hash]common.Hash{testSlot: testValue}},
			testContract: {Nonce: 1, Code: []byte{0x13, 0x37}, Balance: common.Big0},
			testEmpty:    {Balance: big.NewInt(1)},
		},
		ExtraData: []byte("test genesis"),
		Timestamp: 9000,
	}

	// txs = []*types.Transaction{
	// 	types.MustSignNewTx(goldenTouchKey, types.LatestSigner(genesis.Config), &types.DynamicFeeTransaction{
	// 		Nonce:     0,
	// 		GasTipCap: common.Big0,
	// 		GasFeeCap: new(big.Int).SetUint64(875_000_000),
	// 		Data:      taiko.AnchorSelector,
	// 		Gas:       taiko.AnchorGasLimit,
	// 		To:        &taikoL2Address,
	// 	}),
	// 	types.MustSignNewTx(testKey, types.LatestSigner(genesis.Config), &types.LegacyTx{
	// 		Nonce:    0,
	// 		Value:    big.NewInt(12),
	// 		GasPrice: big.NewInt(params.InitialBaseFee),
	// 		Gas:      params.TxGas,
	// 		To:       &common.Address{2},
	// 	}),
	// 	types.MustSignNewTx(testKey, types.LatestSigner(genesis.Config), &types.LegacyTx{
	// 		Nonce:    1,
	// 		Value:    big.NewInt(8),
	// 		GasPrice: big.NewInt(params.InitialBaseFee),
	// 		Gas:      params.TxGas,
	// 		To:       &common.Address{2},
	// 	}),
	// 	// prepareBlockTx
	// 	types.MustSignNewTx(testKey, types.LatestSigner(genesis.Config), &types.LegacyTx{
	// 		Nonce:    2,
	// 		Value:    big.NewInt(8),
	// 		GasPrice: big.NewInt(params.InitialBaseFee),
	// 		Gas:      params.TxGas,
	// 		To:       &testL2RollupAddress,
	// 	}),
	// }
}

func TestVerifyHeader(t *testing.T) {
	chainDB := memdb.NewTestDB(t, kv.ChainDB)
	testEngine = taiko.New(config, chainDB)
	checkStateRoot := true
	m := mock.MockWithGenesisEngine(t, genesis, testEngine, false, checkStateRoot)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, nBlocks, func(i int, block *core.BlockGen) {
		// The chain maker doesn't have access to a chain, so the difficulty will be
		// lets unset (nil). Set it here to the correct value.
		block.SetDifficulty(common.Big0)
		block.SetExtra([]byte("test_taiko"))
		block.SetWithdrawalsHash(&types.EmptyRootHash)
	})
	require.NoError(t, err)

	err = m.InsertChain(chain)
	require.NoError(t, err)

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	chainReader := stagedsync.ChainReader{
		Cfg:         *m.ChainConfig,
		Db:          tx,
		BlockReader: m.BlockReader,
		Logger:      m.Log,
	}

	for _, b := range chain.Blocks {
		err := testEngine.VerifyHeader(chainReader, b.Header(), false)
		assert.NoErrorf(t, err, "VerifyHeader error: %s", err)
	}

	err = testEngine.VerifyHeader(chainReader, &types.Header{
		Number:          common.Big1,
		Time:            uint64(time.Now().Unix()),
		BaseFee:         big.NewInt(params2.InitialBaseFee),
		WithdrawalsHash: &types.EmptyRootHash,
		UncleHash:       types.EmptyUncleHash,
	}, false)
	assert.ErrorIs(t, err, consensus.ErrUnknownAncestor, "VerifyHeader should throw ErrUnknownAncestor when parentHash is unknown")
	err = testEngine.VerifyHeader(chainReader, &types.Header{
		ParentHash:      m.Genesis.Hash(),
		Number:          common.Big0,
		Time:            uint64(time.Now().Unix()),
		BaseFee:         big.NewInt(params2.InitialBaseFee),
		WithdrawalsHash: &types.EmptyRequestsHash,
		UncleHash:       types.EmptyUncleHash,
	}, false)
	assert.ErrorIs(t, err, consensus.ErrInvalidNumber, "VerifyHeader should throw ErrInvalidNumber when the block number is wrong")

	err = testEngine.VerifyHeader(chainReader, &types.Header{
		ParentHash:      chain.TopBlock.Hash(),
		Number:          new(big.Int).SetInt64(int64(len(chain.Blocks) + 1)),
		Time:            uint64(time.Now().Unix()),
		Extra:           bytes.Repeat([]byte{1}, int(params2.MaximumExtraDataSize+1)),
		BaseFee:         big.NewInt(params2.InitialBaseFee),
		WithdrawalsHash: &types.EmptyRootHash,
		UncleHash:       types.EmptyUncleHash,
	}, false)
	assert.ErrorContains(t, err, "extra-data too long", "VerifyHeader should throw ErrExtraDataTooLong when the block has too much extra data")

	err = testEngine.VerifyHeader(chainReader, &types.Header{
		ParentHash:      chain.TopBlock.Hash(),
		Number:          new(big.Int).SetInt64(int64(len(chain.Blocks) + 1)),
		Time:            uint64(time.Now().Unix()),
		Difficulty:      common.Big1,
		BaseFee:         big.NewInt(params2.InitialBaseFee),
		WithdrawalsHash: &types.EmptyRootHash,
		UncleHash:       types.EmptyUncleHash,
	}, false)
	assert.ErrorContains(t, err, "invalid difficulty", "VerifyHeader should throw ErrInvalidDifficulty when difficulty is not 0")

	err = testEngine.VerifyHeader(chainReader, &types.Header{
		ParentHash:      chain.TopBlock.Hash(),
		Number:          new(big.Int).SetInt64(int64(len(chain.Blocks) + 1)),
		Time:            uint64(time.Now().Unix()),
		GasLimit:        params2.MaxGasLimit + 1,
		BaseFee:         big.NewInt(params2.InitialBaseFee),
		WithdrawalsHash: &types.EmptyRootHash,
		UncleHash:       types.EmptyUncleHash,
	}, false)
	assert.ErrorContains(t, err, "invalid gasLimit", "VerifyHeader should throw ErrInvalidGasLimit when gasLimit is higher than the limit")

	err = testEngine.VerifyHeader(chainReader, &types.Header{
		ParentHash: chain.TopBlock.Hash(),
		Number:     new(big.Int).SetInt64(int64(len(chain.Blocks) + 1)),
		Time:       uint64(time.Now().Unix()),
		GasLimit:   params2.MaxGasLimit,
		BaseFee:    big.NewInt(params2.InitialBaseFee),
		UncleHash:  types.EmptyUncleHash,
	}, false)
	assert.ErrorContains(t, err, "withdrawals hash missing", "VerifyHeader should throw ErrWithdrawalsHashMissing when withdrawalshash is nil")

	err = testEngine.VerifyHeader(chainReader, &types.Header{
		ParentHash:      chain.TopBlock.Hash(),
		Number:          new(big.Int).SetInt64(int64(len(chain.Blocks) + 1)),
		Time:            uint64(time.Now().Unix()),
		GasLimit:        params2.MaxGasLimit,
		BaseFee:         big.NewInt(params2.InitialBaseFee),
		WithdrawalsHash: &types.EmptyRootHash,
	}, false)
	assert.ErrorContains(t, err, "uncles not empty", "VerifyHeader should throw ErrUnclesNotEmpty if uncles is not the empty hash")
}

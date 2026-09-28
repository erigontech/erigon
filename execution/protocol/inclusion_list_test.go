package protocol

import (
	"crypto/ecdsa"
	"errors"
	"math"
	"testing"
	"unsafe"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const (
	ilBlockGasLimit = 30_000_000
	ilTxnGasLimit   = 100_000
	ilChainID       = 1337
)

var (
	ilSenderKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	ilOtherKey, _   = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	ilSenderCommon  = crypto.PubkeyToAddress(ilSenderKey.PublicKey)
	ilOtherCommon   = crypto.PubkeyToAddress(ilOtherKey.PublicKey)
	ilSenderAddress = accounts.InternAddress(ilSenderCommon)
	ilOtherAddress  = accounts.InternAddress(ilOtherCommon)
	ilRecipient     = common.HexToAddress("0x3333333333333333333333333333333333333333")
)

func ilSigner() types.Signer {
	return *types.LatestSignerForChainID(uint256.NewInt(ilChainID))
}

func ilSign(t *testing.T, key *ecdsa.PrivateKey, txn types.Transaction) types.Transaction {
	t.Helper()
	signed, err := types.SignTx(txn, ilSigner(), key)
	require.NoError(t, err)
	return signed
}

func ilLegacyTxn(t *testing.T, key *ecdsa.PrivateKey, nonce, gasLimit, gasPrice, value uint64, data []byte) types.Transaction {
	t.Helper()
	return ilSign(t, key, types.NewTransaction(nonce, ilRecipient, uint256.NewInt(value), gasLimit, uint256.NewInt(gasPrice), data))
}

func ilValidTxn(t *testing.T, nonce uint64) types.Transaction {
	t.Helper()
	return ilLegacyTxn(t, ilSenderKey, nonce, ilTxnGasLimit, 0, 0, nil)
}

func ilCreationTxn(t *testing.T, nonce, gasLimit uint64, data []byte) types.Transaction {
	t.Helper()
	return ilSign(t, ilSenderKey, types.NewContractCreation(nonce, uint256.NewInt(0), gasLimit, uint256.NewInt(0), data))
}

func ilDynamicFeeTxn(t *testing.T, nonce, gasLimit, tipCap, feeCap uint64) types.Transaction {
	t.Helper()
	to := ilRecipient
	return ilSign(t, ilSenderKey, &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    nonce,
			GasLimit: gasLimit,
			To:       &to,
			Value:    *uint256.NewInt(0),
		},
		ChainID: *uint256.NewInt(ilChainID),
		TipCap:  *uint256.NewInt(tipCap),
		FeeCap:  *uint256.NewInt(feeCap),
	})
}

func ilBlobTxn(t *testing.T, nonce uint64, blobs int) types.Transaction {
	t.Helper()
	to := ilRecipient
	hashes := make([]common.Hash, blobs)
	for i := range hashes {
		hashes[i][0] = kzg.BlobCommitmentVersionKZG
		hashes[i][31] = byte(i + 1)
	}
	return ilSign(t, ilSenderKey, &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    nonce,
				GasLimit: ilTxnGasLimit,
				To:       &to,
				Value:    *uint256.NewInt(0),
			},
			ChainID: *uint256.NewInt(ilChainID),
			TipCap:  *uint256.NewInt(0),
			FeeCap:  *uint256.NewInt(0),
		},
		MaxFeePerBlobGas:    *uint256.NewInt(0),
		BlobVersionedHashes: hashes,
	})
}

func ilUnsignedTxn() types.Transaction {
	return types.NewTransaction(0, ilRecipient, uint256.NewInt(0), ilTxnGasLimit, uint256.NewInt(0), nil)
}

func ilState(t *testing.T) *state.IntraBlockState {
	t.Helper()
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	return ibs
}

func ilEVM(t *testing.T, ibs *state.IntraBlockState, cfg *chain.Config) *vm.EVM {
	t.Helper()
	return newTestEVM(ibs, cfg, ilBlockGasLimit)
}

func ilEVMWithBaseFee(t *testing.T, ibs *state.IntraBlockState, cfg *chain.Config, baseFee uint64) *vm.EVM {
	t.Helper()
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    ilBlockGasLimit,
		BaseFee:     *uint256.NewInt(baseFee),
	}
	return vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, cfg, vm.Config{NoBaseFee: true})
}

func ilGasPool() *GasPool {
	return NewGasPool(ilBlockGasLimit, params.GasPerBlob*params.MaxBlobsPerTxn)
}

type ilStubTxn struct {
	types.Transaction
	hash common.Hash
	msg  *types.Message
	err  error
}

func (s *ilStubTxn) Hash() common.Hash { return s.hash }

func (s *ilStubTxn) AsMessage(types.Signer, *uint256.Int, *chain.Rules) (*types.Message, error) {
	return s.msg, s.err
}

func ilStorageKeys(n int) []common.Hash {
	var key common.Hash
	return unsafe.Slice(&key, n)
}

func ilAccessListMessage(storageKeys int) *types.Message {
	accessList := types.AccessList{{
		Address:     ilRecipient,
		StorageKeys: ilStorageKeys(storageKeys),
	}}
	return types.NewMessage(
		ilSenderAddress, accounts.InternAddress(ilRecipient), 0, uint256.NewInt(0), ilTxnGasLimit,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		nil, accessList,
		false, false, true, false, nil,
	)
}

func TestCheckInclusionListTransactions_EmptyInclusionList(t *testing.T) {
	t.Parallel()

	ibs := ilState(t)
	evm := ilEVM(t, ibs, chain.AllProtocolChanges)

	t.Run("nil block and nil inclusion list", func(t *testing.T) {
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, nil))
	})

	t.Run("non-empty block, empty inclusion list", func(t *testing.T) {
		blockTxns := types.Transactions{ilValidTxn(t, 0), ilValidTxn(t, 1)}
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), blockTxns, types.Transactions{}))
	})

	t.Run("empty block, empty inclusion list", func(t *testing.T) {
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), types.Transactions{}, types.Transactions{}))
	})
}

func TestCheckInclusionListTransactions_AllInclusionListTxnsInBlock(t *testing.T) {
	t.Parallel()

	ibs := ilState(t)
	evm := ilEVM(t, ibs, chain.AllProtocolChanges)

	first := ilValidTxn(t, 0)
	second := ilValidTxn(t, 1)
	third := ilValidTxn(t, 2)

	t.Run("block equals inclusion list", func(t *testing.T) {
		blockTxns := types.Transactions{first, second}
		inclusionListTxns := types.Transactions{first, second}
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), blockTxns, inclusionListTxns))
	})

	t.Run("block is a superset in different order", func(t *testing.T) {
		blockTxns := types.Transactions{third, first, second}
		inclusionListTxns := types.Transactions{second, first}
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), blockTxns, inclusionListTxns))
	})

	t.Run("duplicate inclusion list entries", func(t *testing.T) {
		blockTxns := types.Transactions{first}
		inclusionListTxns := types.Transactions{first, first, first}
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), blockTxns, inclusionListTxns))
	})
}

func TestCheckInclusionListTransactions_IncludedTxnIsNotValidated(t *testing.T) {
	t.Parallel()

	ibs := ilState(t)
	require.NoError(t, ibs.SetNonce(ilSenderAddress, 9, tracing.NonceChangeUnspecified))
	evm := ilEVM(t, ibs, chain.AllProtocolChanges)

	nonceTooLow := ilValidTxn(t, 0)
	belowIntrinsicGas := ilLegacyTxn(t, ilSenderKey, 9, 1_000, 0, 0, nil)

	blockTxns := types.Transactions{nonceTooLow, belowIntrinsicGas}
	inclusionListTxns := types.Transactions{nonceTooLow, belowIntrinsicGas}

	require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), blockTxns, inclusionListTxns))
}

func TestCheckInclusionListTransactions_MissingValidTxn(t *testing.T) {
	t.Parallel()

	ibs := ilState(t)
	evm := ilEVM(t, ibs, chain.AllProtocolChanges)

	missing := ilValidTxn(t, 0)

	t.Run("empty block", func(t *testing.T) {
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), types.Transactions{}, types.Transactions{missing}))
	})

	t.Run("block holds unrelated txns", func(t *testing.T) {
		blockTxns := types.Transactions{ilValidTxn(t, 7), ilLegacyTxn(t, ilOtherKey, 0, ilTxnGasLimit, 0, 0, nil)}
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), blockTxns, types.Transactions{missing}))
	})

	t.Run("same sender and nonce but different txn", func(t *testing.T) {
		sameNonceOtherPayload := ilLegacyTxn(t, ilSenderKey, 0, ilTxnGasLimit, 0, 0, []byte{0x01})
		require.NotEqual(t, missing.Hash(), sameNonceOtherPayload.Hash())
		blockTxns := types.Transactions{sameNonceOtherPayload}
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), blockTxns, types.Transactions{missing}))
	})

	t.Run("one of several inclusion list txns missing", func(t *testing.T) {
		included := ilValidTxn(t, 1)
		blockTxns := types.Transactions{included}
		inclusionListTxns := types.Transactions{included, missing}
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), blockTxns, inclusionListTxns))
	})

	t.Run("duplicate missing txn", func(t *testing.T) {
		inclusionListTxns := types.Transactions{missing, missing}
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), types.Transactions{}, inclusionListTxns))
	})

	t.Run("contract creation", func(t *testing.T) {
		creation := ilCreationTxn(t, 0, ilTxnGasLimit, []byte{byte(vm.STOP)})
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), types.Transactions{}, types.Transactions{creation}))
	})

	t.Run("blob txn within the blob budget", func(t *testing.T) {
		blobTxn := ilBlobTxn(t, 0, 1)
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), types.Transactions{}, types.Transactions{blobTxn}))
	})
}

func TestCheckInclusionListTransactions_MissingInvalidTxn(t *testing.T) {
	t.Parallel()

	t.Run("nonce too low", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.SetNonce(ilSenderAddress, 5, tracing.NonceChangeUnspecified))
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{ilValidTxn(t, 4)}))
	})

	t.Run("nonce too high", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{ilValidTxn(t, 5)}))
	})

	t.Run("nonce at uint64 max", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.SetNonce(ilSenderAddress, math.MaxUint64, tracing.NonceChangeUnspecified))
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{ilValidTxn(t, math.MaxUint64)}))
	})

	t.Run("sender is not an EOA", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.SetCode(ilSenderAddress, []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{ilValidTxn(t, 0)}))
	})

	t.Run("insufficient funds for value", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilLegacyTxn(t, ilSenderKey, 0, ilTxnGasLimit, 0, 1_000, nil)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("insufficient funds for gas", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.AddBalance(ilSenderAddress, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilLegacyTxn(t, ilSenderKey, 0, ilTxnGasLimit, 1_000_000_000, 0, nil)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("sufficient funds for gas", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.AddBalance(ilSenderAddress, *uint256.NewInt(ilTxnGasLimit * 1_000_000_000), tracing.BalanceChangeUnspecified))
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilLegacyTxn(t, ilSenderKey, 0, ilTxnGasLimit, 1_000_000_000, 0, nil)
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("gas limit below intrinsic gas", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilLegacyTxn(t, ilSenderKey, 0, 1_000, 0, 0, nil)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("gas limit below the calldata floor", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilLegacyTxn(t, ilSenderKey, 0, 30_000, 0, 0, make([]byte, 1_000))
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("gas limit at the calldata floor", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilLegacyTxn(t, ilSenderKey, 0, ilTxnGasLimit, 0, 0, make([]byte, 1_000))
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("init code above the size limit", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilCreationTxn(t, 0, params.MaxTxnGasLimit, make([]byte, params.MaxInitCodeSizeAmsterdam+1))
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("init code at the size limit", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilCreationTxn(t, 0, params.MaxTxnGasLimit, make([]byte, params.MaxInitCodeSizeAmsterdam))
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("tip cap above fee cap", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilDynamicFeeTxn(t, 0, ilTxnGasLimit, 2, 1)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("fee cap below base fee", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVMWithBaseFee(t, ibs, chain.AllProtocolChanges, 1_000)
		txn := ilDynamicFeeTxn(t, 0, ilTxnGasLimit, 1, 1)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("fee cap above base fee", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.AddBalance(ilSenderAddress, *uint256.NewInt(ilTxnGasLimit * 2_000), tracing.BalanceChangeUnspecified))
		evm := ilEVMWithBaseFee(t, ibs, chain.AllProtocolChanges, 1_000)
		txn := ilDynamicFeeTxn(t, 0, ilTxnGasLimit, 1, 2_000)
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("more blobs than a single txn may carry", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		txn := ilBlobTxn(t, 0, params.MaxBlobsPerTxn+1)
		gp := NewGasPool(ilBlockGasLimit, params.GasPerBlob*uint64(params.MaxBlobsPerTxn+1))
		require.True(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("state read failure", func(t *testing.T) {
		ibs := state.New(&accountErrorReader{StateReader: state.NewNoopReader(), err: errors.New("nonce read failed")})
		defer ibs.Close()
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{ilValidTxn(t, 0)}))
	})
}

func TestCheckInclusionListTransactions_UndecodableTxn(t *testing.T) {
	t.Parallel()

	t.Run("unsigned txn", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{ilUnsignedTxn()}))
	})

	t.Run("dynamic fee txn before london", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.TestChainBerlinConfig)
		txn := ilDynamicFeeTxn(t, 0, ilTxnGasLimit, 0, 0)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("blob txn with an invalid versioned hash", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		to := ilRecipient
		txn := ilSign(t, ilSenderKey, &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: 0, GasLimit: ilTxnGasLimit, To: &to, Value: *uint256.NewInt(0)},
				ChainID:  *uint256.NewInt(ilChainID),
				TipCap:   *uint256.NewInt(0),
				FeeCap:   *uint256.NewInt(0),
			},
			MaxFeePerBlobGas:    *uint256.NewInt(0),
			BlobVersionedHashes: []common.Hash{{0x02}},
		})
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("signed by a different chain id", func(t *testing.T) {
		ibs := ilState(t)
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		otherSigner := *types.LatestSignerForChainID(uint256.NewInt(ilChainID + 1))
		txn, err := types.SignTx(
			types.NewTransaction(0, ilRecipient, uint256.NewInt(0), ilTxnGasLimit, uint256.NewInt(0), nil),
			otherSigner, ilSenderKey)
		require.NoError(t, err)
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})
}

func TestCheckInclusionListTransactions_BlockGasExhausted(t *testing.T) {
	t.Parallel()

	ibs := ilState(t)
	evm := ilEVM(t, ibs, chain.AllProtocolChanges)
	txn := ilValidTxn(t, 0)

	t.Run("execution gas exhausted", func(t *testing.T) {
		gp := NewBlockGasPool(ilTxnGasLimit-1, ilBlockGasLimit, params.GasPerBlob)
		require.True(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("state gas exhausted", func(t *testing.T) {
		gp := NewBlockGasPool(ilBlockGasLimit, ilTxnGasLimit-1, params.GasPerBlob)
		require.True(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("both dimensions exactly sufficient", func(t *testing.T) {
		gp := NewBlockGasPool(ilTxnGasLimit, ilTxnGasLimit, params.GasPerBlob)
		require.False(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("empty gas pool", func(t *testing.T) {
		gp := NewGasPool(0, 0)
		require.True(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("nil gas pool skips the budget checks", func(t *testing.T) {
		require.False(t, CheckInclusionListTransactions(evm, nil, ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("blob gas exhausted", func(t *testing.T) {
		gp := NewGasPool(ilBlockGasLimit, 0)
		require.True(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{ilBlobTxn(t, 0, 1)}))
	})

	t.Run("blob gas exactly sufficient", func(t *testing.T) {
		gp := NewGasPool(ilBlockGasLimit, params.GasPerBlob)
		require.False(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{ilBlobTxn(t, 0, 1)}))
	})

	t.Run("gas limit above the per-txn cap before amsterdam", func(t *testing.T) {
		osakaIBS := ilState(t)
		osakaEVM := ilEVM(t, osakaIBS, chain.TestChainOsakaConfig)
		capped := ilLegacyTxn(t, ilSenderKey, 0, params.MaxTxnGasLimit+1, 0, 0, nil)
		gp := NewGasPool(params.MaxTxnGasLimit*4, 0)
		require.True(t, CheckInclusionListTransactions(osakaEVM, gp, ilSigner(), nil, types.Transactions{capped}))
	})

	t.Run("gas limit above the per-txn cap fits the execution dimension after amsterdam", func(t *testing.T) {
		gp := NewBlockGasPool(params.MaxTxnGasLimit, params.MaxTxnGasLimit*2, 0)
		capped := ilLegacyTxn(t, ilSenderKey, 0, params.MaxTxnGasLimit+1, 0, 0, nil)
		require.False(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{capped}))
	})
}

func TestCheckInclusionListTransactions_IntrinsicGasOverflow(t *testing.T) {
	t.Parallel()

	ibs := ilState(t)
	evm := ilEVM(t, ibs, chain.AllProtocolChanges)

	t.Run("overflowing access list is skipped", func(t *testing.T) {
		txn := &ilStubTxn{hash: common.HexToHash("0xaa"), msg: ilAccessListMessage(1 << 55)}
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("access list within range is validated", func(t *testing.T) {
		txn := &ilStubTxn{hash: common.HexToHash("0xbb"), msg: ilAccessListMessage(1)}
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, types.Transactions{txn}))
	})

	t.Run("overflowing access list is skipped even when included", func(t *testing.T) {
		txn := &ilStubTxn{hash: common.HexToHash("0xaa"), msg: ilAccessListMessage(1 << 55)}
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), types.Transactions{txn}, types.Transactions{txn}))
	})
}

func TestCheckInclusionListTransactions_ValidTxnAfterInvalidOnes(t *testing.T) {
	t.Parallel()

	ibs := ilState(t)
	require.NoError(t, ibs.SetNonce(ilOtherAddress, 5, tracing.NonceChangeUnspecified))
	evm := ilEVM(t, ibs, chain.AllProtocolChanges)

	nonceTooLow := ilLegacyTxn(t, ilOtherKey, 0, ilTxnGasLimit, 0, 0, nil)
	belowIntrinsicGas := ilLegacyTxn(t, ilOtherKey, 5, 1_000, 0, 0, nil)
	unsigned := ilUnsignedTxn()
	valid := ilValidTxn(t, 0)

	t.Run("all inclusion list txns invalid", func(t *testing.T) {
		inclusionListTxns := types.Transactions{nonceTooLow, belowIntrinsicGas, unsigned}
		require.True(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, inclusionListTxns))
	})

	t.Run("valid txn last", func(t *testing.T) {
		inclusionListTxns := types.Transactions{nonceTooLow, belowIntrinsicGas, unsigned, valid}
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, inclusionListTxns))
	})

	t.Run("valid txn first", func(t *testing.T) {
		inclusionListTxns := types.Transactions{valid, nonceTooLow, belowIntrinsicGas, unsigned}
		require.False(t, CheckInclusionListTransactions(evm, ilGasPool(), ilSigner(), nil, inclusionListTxns))
	})
}

func TestCheckInclusionListTransactions_LeavesGasPoolAndStateUntouched(t *testing.T) {
	t.Parallel()

	balance := uint256.NewInt(1_000_000_000_000_000_000)

	assertUntouched := func(t *testing.T, ibs *state.IntraBlockState, gp *GasPool) {
		t.Helper()
		require.Equal(t, uint64(ilBlockGasLimit), gp.ExecutionGasAvailable())
		require.Equal(t, uint64(ilBlockGasLimit), gp.StateGasAvailable())
		require.Equal(t, params.GasPerBlob*params.MaxBlobsPerTxn, gp.BlobGas())

		nonce, err := ibs.GetNonce(ilSenderAddress)
		require.NoError(t, err)
		require.Zero(t, nonce)

		got, err := ibs.GetBalance(ilSenderAddress)
		require.NoError(t, err)
		require.Equal(t, *balance, got)
	}

	t.Run("unsatisfied inclusion list", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.AddBalance(ilSenderAddress, *balance, tracing.BalanceChangeUnspecified))
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		gp := ilGasPool()

		require.False(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{ilValidTxn(t, 0)}))
		assertUntouched(t, ibs, gp)
	})

	t.Run("satisfied inclusion list", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.AddBalance(ilSenderAddress, *balance, tracing.BalanceChangeUnspecified))
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		gp := ilGasPool()

		txn := ilValidTxn(t, 0)
		require.True(t, CheckInclusionListTransactions(evm, gp, ilSigner(), types.Transactions{txn}, types.Transactions{txn}))
		assertUntouched(t, ibs, gp)
	})

	t.Run("inclusion list of invalid txns", func(t *testing.T) {
		ibs := ilState(t)
		require.NoError(t, ibs.AddBalance(ilSenderAddress, *balance, tracing.BalanceChangeUnspecified))
		evm := ilEVM(t, ibs, chain.AllProtocolChanges)
		gp := ilGasPool()

		invalid := ilLegacyTxn(t, ilSenderKey, 9, ilTxnGasLimit, 0, 0, nil)
		require.True(t, CheckInclusionListTransactions(evm, gp, ilSigner(), nil, types.Transactions{invalid}))
		assertUntouched(t, ibs, gp)
	})
}

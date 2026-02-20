package freezeblocks_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

func TestTransactionSnapshotEncodeDecode(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	addr := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.TestChainConfig
	signer := types.LatestSigner(chainConfig)

	txs := createTestTransactions(t, key, addr, *signer)

	segmentFile := filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, 0, 1000, snaptype2.Transactions.Enum()))
	idxFile := filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 0, 1000, snaptype2.Transactions.Name()))

	encodeTransactions(t, txs, segmentFile, idxFile, dir, logger)

	decodedTxs := decodeTransactions(t, segmentFile, len(txs))

	verifyTransactions(t, txs, decodedTxs)
}

func createTestTransactions(t *testing.T, key *ecdsa.PrivateKey, addr common.Address, signer types.Signer) []types.Transaction {
	txs := make([]types.Transaction, 0)

	legacyTo := common.HexToAddress("0xdeadbeef")
	timeboostedTrue := true
	timeboostedFalse := false

	legacyTx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    0,
			GasLimit: 21000,
			To:       &legacyTo,
			Value:    uint256.NewInt(100),
			Data:     nil,
		},
		GasPrice:    uint256.NewInt(1000000000),
		Timeboosted: &timeboostedTrue,
	}
	signedLegacy, err := types.SignTx(legacyTx, signer, key)
	require.NoError(t, err)
	txs = append(txs, signedLegacy)

	dynamicTo := common.HexToAddress("0xcafebabe")
	chainID := signer.ChainID()
	dynamicTx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &dynamicTo,
			Value:    uint256.NewInt(200),
			Data:     nil,
		},
		ChainID:     chainID,
		TipCap:      uint256.NewInt(1000000000),
		FeeCap:      uint256.NewInt(2000000000),
		Timeboosted: &timeboostedFalse,
	}
	signedDynamic, err := types.SignTx(dynamicTx, signer, key)
	require.NoError(t, err)
	txs = append(txs, signedDynamic)

	arbRetryTo := common.HexToAddress("0xbeefcafe")
	arbRetryTx := &types.ArbitrumRetryTx{
		ChainId:             signer.ChainID().ToBig(),
		Nonce:               2,
		GasFeeCap:           big.NewInt(3000000000),
		Gas:                 50000,
		To:                  &arbRetryTo,
		Value:               big.NewInt(300),
		Data:                []byte{0x01, 0x02, 0x03},
		TicketId:            common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
		RefundTo:            addr,
		MaxRefund:           big.NewInt(1000),
		SubmissionFeeRefund: big.NewInt(500),
		From:                addr,
		Timeboosted:         &timeboostedTrue,
	}
	txs = append(txs, arbRetryTx)

	retryTo := common.HexToAddress("0xf00dbabe")
	arbSubmitRetryable := &types.ArbitrumSubmitRetryableTx{
		ChainId:          signer.ChainID().ToBig(),
		RequestId:        common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"),
		From:             addr,
		L1BaseFee:        big.NewInt(1000000000),
		DepositValue:     big.NewInt(5000),
		GasFeeCap:        big.NewInt(4000000000),
		Gas:              100000,
		RetryTo:          &retryTo,
		RetryValue:       big.NewInt(400),
		Beneficiary:      common.HexToAddress("0xdeadcafe"),
		MaxSubmissionFee: big.NewInt(2000),
		FeeRefundAddr:    addr,
		RetryData:        []byte("retry-data"),
		EffectiveGasUsed: 75000,
	}
	txs = append(txs, arbSubmitRetryable)

	return txs
}

func encodeTransactions(t *testing.T, txs []types.Transaction, segmentFile, idxFile, tmpDir string, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100

	c, err := seg.NewCompressor(context.Background(), "test-txs", segmentFile, tmpDir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()

	for i, tx := range txs {
		var txBuf bytes.Buffer
		err := tx.EncodeRLP(&txBuf)
		require.NoError(t, err, "failed to encode tx %d", i)

		sender, hasSender := tx.GetSender()
		var senderBytes [20]byte
		if hasSender {
			senderBytes = sender
		}

		hash := tx.Hash()
		hashByte := hash[:1]

		txBytes := txBuf.Bytes()
		buf := make([]byte, 0, len(hashByte)+len(senderBytes)+len(txBytes))
		buf = append(buf, hashByte...)
		buf = append(buf, senderBytes[:]...)
		buf = append(buf, txBytes...)

		err = c.AddWord(buf)
		require.NoError(t, err, "failed to add tx %d", i)
	}

	err = c.Compress()
	require.NoError(t, err)

	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   len(txs),
		BucketSize: 10,
		TmpDir:     tmpDir,
		IndexFile:  idxFile,
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()

	for i := uint64(0); i < uint64(len(txs)); i++ {
		err = idx.AddKey([]byte{byte(i)}, i)
		require.NoError(t, err)
	}

	err = idx.Build(context.Background())
	require.NoError(t, err)
}

func decodeTransactions(t *testing.T, segmentFile string, expectedCount int) []types.Transaction {
	d, err := seg.NewDecompressor(segmentFile)
	require.NoError(t, err)
	defer d.Close()

	require.Equal(t, expectedCount, d.Count(), "decompressor count mismatch")

	txs := make([]types.Transaction, 0, expectedCount)
	g := d.MakeGetter()

	for g.HasNext() {
		buf, _ := g.Next(nil)
		require.True(t, len(buf) > 1+20, "buffer too short")

		tx, err := types.DecodeTransaction(buf[1+20:])
		require.NoError(t, err)

		txs = append(txs, tx)
	}

	require.Equal(t, expectedCount, len(txs), "decoded transaction count mismatch")
	return txs
}

func verifyTransactions(t *testing.T, original, decoded []types.Transaction) {
	require.Equal(t, len(original), len(decoded), "transaction count mismatch")

	for i := range original {
		origTx := original[i]
		decodedTx := decoded[i]

		require.Equal(t, origTx.Type(), decodedTx.Type(), "tx %d: type mismatch", i)
		require.Equal(t, origTx.Hash(), decodedTx.Hash(), "tx %d: hash mismatch", i)
		require.Equal(t, origTx.GetNonce(), decodedTx.GetNonce(), "tx %d: nonce mismatch", i)
		require.Equal(t, origTx.GetGasLimit(), decodedTx.GetGasLimit(), "tx %d: gas limit mismatch", i)

		origTimeboosted := origTx.IsTimeBoosted()
		decodedTimeboosted := decodedTx.IsTimeBoosted()
		if origTimeboosted != nil {
			require.NotNil(t, decodedTimeboosted, "tx %d: timeboosted should not be nil", i)
			require.Equal(t, *origTimeboosted, *decodedTimeboosted, "tx %d: timeboosted value mismatch", i)
		}

		if origTx.Type() == types.ArbitrumSubmitRetryableTxType {
			origRetryable, ok1 := origTx.Unwrap().(*types.ArbitrumSubmitRetryableTx)
			decodedRetryable, ok2 := decodedTx.Unwrap().(*types.ArbitrumSubmitRetryableTx)

			require.True(t, ok1 && ok2, "tx %d: failed to unwrap ArbitrumSubmitRetryableTx", i)
			require.Equal(t, origRetryable.EffectiveGasUsed, decodedRetryable.EffectiveGasUsed,
				"tx %d: EffectiveGasUsed mismatch", i)

			t.Logf("Tx %d (ArbitrumSubmitRetryableTx): EffectiveGasUsed preserved correctly: %d",
				i, decodedRetryable.EffectiveGasUsed)
		}

		if origTx.Type() == types.ArbitrumRetryTxType {
			origRetry, ok1 := origTx.Unwrap().(*types.ArbitrumRetryTx)
			decodedRetry, ok2 := decodedTx.Unwrap().(*types.ArbitrumRetryTx)

			require.True(t, ok1 && ok2, "tx %d: failed to unwrap ArbitrumRetryTx", i)

			if origRetry.Timeboosted != nil {
				require.NotNil(t, decodedRetry.Timeboosted, "tx %d: ArbitrumRetryTx Timeboosted should not be nil", i)
				require.Equal(t, *origRetry.Timeboosted, *decodedRetry.Timeboosted,
					"tx %d: ArbitrumRetryTx Timeboosted mismatch", i)

				t.Logf("Tx %d (ArbitrumRetryTx): Timeboosted preserved correctly: %v",
					i, *decodedRetry.Timeboosted)
			}
		}

		t.Logf("Tx %d verified: type=0x%x, hash=%s, timeboosted=%v",
			i, decodedTx.Type(), decodedTx.Hash().Hex(), decodedTimeboosted)
	}
}

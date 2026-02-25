package types

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/stretchr/testify/require"
)

func TestArbitrumInternalTx(t *testing.T) {
	rawInitial := [][]byte{
		common.FromHex("6af88a83066eeeb8846bf6a42d000000000000000000000000000000000000000000000000000000005bd57bd900000000000000000000000000000000000000000000000000000000003f28db00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000064e4f6d4"),
		common.FromHex("0x6af88a83066eeeb8846bf6a42d00000000000000000000000000000000000000000000000000000000064cb523000000000000000000000000000000000000000000000000000000000049996b00000000000000000000000000000000000000000000000000000000001f09350000000000000000000000000000000000000000000000000000000000000000"),
	}

	expectedHashes := []common.Hash{
		common.HexToHash("0x1ac8d67d5c4be184b3822f9ef97102789394f4bc75a0f528d5e14debef6e184c"),
		common.HexToHash("0x3d78fd6ddbac46955b91777c1fc698b011b7c4a2a84d07a0b0b1a11f34ccf817"),
	}

	for ri, raw := range rawInitial {
		var tx ArbitrumInternalTx
		if err := rlp.DecodeBytes(raw[1:], &tx); err != nil {
			t.Fatal(err)
		}

		var b bytes.Buffer
		require.Equal(t, tx.Hash(), expectedHashes[ri])
		// now encode and decode again
		require.NoError(t, tx.MarshalBinary(&b))
		require.Equal(t, raw, b.Bytes())
	}
}

func TestArbitrumUnsignedTx(t *testing.T) {
	rawInitial := [][]byte{
		common.FromHex("0x65f85e83066eee9462182981bf35cdf00dbecdb9bbc00be33138a4dc0184a0eebb008301bdd494000000000000000000000000000000000000006401a425e1606300000000000000000000000051072981bf35cdf00dbecdb9bbc00be3313893cb"),
	}

	expectedHashes := []common.Hash{
		common.HexToHash("0x8b7e4e0a2a31d2889200dc6c91c12833208d2f7847eabf0c21e9b15f86a8a8aa"),
	}

	for ri, raw := range rawInitial {
		var tx ArbitrumUnsignedTx
		if err := rlp.DecodeBytes(raw[1:], &tx); err != nil {
			t.Fatal(err)
		}

		var b bytes.Buffer
		require.Equal(t, tx.Hash(), expectedHashes[ri])
		// now encode and decode again
		require.NoError(t, tx.MarshalBinary(&b))
		require.Equal(t, raw, b.Bytes())
	}
}

func TestArbitrumSubmitRetryableTx(t *testing.T) {
	rawInitial := common.FromHex("0x69f89f83066eeea0000000000000000000000000000000000000000000000000000000000000000194b8787d8f23e176a5d32135d746b69886e03313be845bd57bd98723e3dbb7b88ab8843b9aca00830186a0943fab184622dc19b6109349b94811493bf2a45362872386f26fc100009411155ca9bbf7be58e27f3309e629c847996b43c88601f6377d4ab89411155ca9bbf7be58e27f3309e629c847996b43c880")
	var tx ArbitrumSubmitRetryableTx
	if err := rlp.DecodeBytes(rawInitial[1:], &tx); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, tx.Hash(), common.HexToHash("0x13cb79b086a427f3db7ebe6ec2bb90a806a3b0368ecee6020144f352e37dbdf6"))

	var b bytes.Buffer

	// now encode and decode again, use ForHashing method since normal method encodes EffectiveGasUsed, which is not present in OG encoding
	require.NoError(t, tx.MarshalBinaryForHashing(&b))

	require.Equal(t, rawInitial, b.Bytes())
}

func TestArbitrumSubmitRetryTx(t *testing.T) {
	rawInitial := common.FromHex("0x68f88583066eee8094b8787d8f23e176a5d32135d746b69886e03313be8405f5e100830186a0943fab184622dc19b6109349b94811493bf2a45362872386f26fc1000080a013cb79b086a427f3db7ebe6ec2bb90a806a3b0368ecee6020144f352e37dbdf69411155ca9bbf7be58e27f3309e629c847996b43c8860b0e85efeab88601f6377d4ab8")
	var tx ArbitrumRetryTx
	if err := rlp.DecodeBytes(rawInitial[1:], &tx); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, tx.Hash(), common.HexToHash("0x873c5ee3092c40336006808e249293bf5f4cb3235077a74cac9cafa7cf73cb8b"))

	var b bytes.Buffer

	// now encode and decode again
	require.NoError(t, tx.MarshalBinary(&b))

	require.Equal(t, rawInitial, b.Bytes())
}

func TestArbitrumDepsitTx(t *testing.T) {
	rawInitial := common.FromHex("0x64f85883066eeea0000000000000000000000000000000000000000000000000000000000000000f9499998aa374dbde60d26433e275ad700b658731749488888aa374dbde60d26433e275ad700b65872063880de0b6b3a7640000")
	var tx ArbitrumDepositTx

	if err := rlp.DecodeBytes(rawInitial[1:], &tx); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, tx.Hash(), common.HexToHash("0x733c1300c06ac4ced959e68f16f565ee8918a4e75c9f9e3913bc7a7e939c60db"))

	var b bytes.Buffer

	// now encode and decode again
	require.NoError(t, tx.MarshalBinary(&b))

	require.Equal(t, rawInitial, b.Bytes())
}

func TestArbitrumSubmitRetryableTxGasUsed(t *testing.T) {
	gasUsedVals := []uint64{0, 32000}

	for _, gasUsed := range gasUsedVals {
		two := big.NewInt(2)
		chainID := big.NewInt(1)

		requestId := common.HexToHash("0x0123")
		from := common.HexToAddress("0x0000000000000000000000000000000000000001")
		retryTo := common.HexToAddress("0x0000000000000000000000000000000000000002")
		beneficiary := common.HexToAddress("0x00000000000000000000000000000000000000B5")
		feeRefund := common.HexToAddress("0x0000000000000000000000000000000000000003")

		tx := &ArbitrumSubmitRetryableTx{
			ChainId:          chainID,
			RequestId:        requestId,
			From:             accounts.InternAddress(from),
			L1BaseFee:        big.NewInt(0),
			DepositValue:     big.NewInt(1000),
			GasFeeCap:        two,
			Gas:              60000,
			RetryTo:          &retryTo,
			RetryValue:       two,
			Beneficiary:      accounts.InternAddress(beneficiary),
			MaxSubmissionFee: big.NewInt(7),
			FeeRefundAddr:    accounts.InternAddress(feeRefund),
			RetryData:        []byte("data"),
			EffectiveGasUsed: gasUsed,
		}

		var buf bytes.Buffer
		require.NoError(t, tx.EncodeRLP(&buf))

		// Decode using your generic RLP transaction decoder
		stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		decoded, err := DecodeRLPTransaction(stream, false)
		require.NoError(t, err)

		tx2, ok := decoded.(*ArbitrumSubmitRetryableTx)
		require.True(t, ok, "decoded type should be *ArbitrumSubmitRetryableTx")

		// Field-by-field equality
		require.EqualValues(t, tx.ChainId, tx2.ChainId)
		require.EqualValues(t, tx.RequestId, tx2.RequestId)
		require.EqualValues(t, tx.From, tx2.From)
		require.EqualValues(t, tx.L1BaseFee, tx2.L1BaseFee)
		require.EqualValues(t, tx.DepositValue, tx2.DepositValue)
		require.EqualValues(t, tx.GasFeeCap, tx2.GasFeeCap)
		require.EqualValues(t, tx.Gas, tx2.Gas)
		require.EqualValues(t, tx.RetryTo, tx2.RetryTo)
		require.EqualValues(t, tx.RetryValue, tx2.RetryValue)
		require.EqualValues(t, tx.Beneficiary, tx2.Beneficiary)
		require.EqualValues(t, tx.MaxSubmissionFee, tx2.MaxSubmissionFee)
		require.EqualValues(t, tx.FeeRefundAddr, tx2.FeeRefundAddr)
		require.EqualValues(t, tx.RetryData, tx2.RetryData)
		require.EqualValues(t, tx.EffectiveGasUsed, tx2.EffectiveGasUsed)

		// With NoTimeBoosted embedded, this should be false.
		require.Nil(t, tx2.IsTimeBoosted())
	}
}

func boolPtr(b bool) *bool { return &b }

package types

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/stretchr/testify/require"
)

func TestArbitrumInternalTx(t *testing.T) {
	rawInitial := common.FromHex("6af88a83066eeeb8846bf6a42d000000000000000000000000000000000000000000000000000000005bd57bd900000000000000000000000000000000000000000000000000000000003f28db00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000064e4f6d4")
	var tx ArbitrumInternalTx
	if err := rlp.DecodeBytes(rawInitial[1:], &tx); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, tx.Hash(), common.HexToHash("0x1ac8d67d5c4be184b3822f9ef97102789394f4bc75a0f528d5e14debef6e184c"))

	var b bytes.Buffer

	// now encode and decode again
	require.NoError(t, tx.MarshalBinary(&b))

	require.Equal(t, rawInitial, b.Bytes())
}

func TestArbitrumSubmitRetryableTx(t *testing.T) {
	rawInitial := common.FromHex("0x69f89f83066eeea0000000000000000000000000000000000000000000000000000000000000000194b8787d8f23e176a5d32135d746b69886e03313be845bd57bd98723e3dbb7b88ab8843b9aca00830186a0943fab184622dc19b6109349b94811493bf2a45362872386f26fc100009411155ca9bbf7be58e27f3309e629c847996b43c88601f6377d4ab89411155ca9bbf7be58e27f3309e629c847996b43c880")
	var tx ArbitrumSubmitRetryableTx
	if err := rlp.DecodeBytes(rawInitial[1:], &tx); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, tx.Hash(), common.HexToHash("0x13cb79b086a427f3db7ebe6ec2bb90a806a3b0368ecee6020144f352e37dbdf6"))

	var b bytes.Buffer

	// now encode and decode again
	require.NoError(t, tx.MarshalBinary(&b))

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

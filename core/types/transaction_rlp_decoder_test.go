package types

import (
	"bytes"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
	"testing"
)

const (
	privateKey = "b9a8b19ff082a7f4b943fcbe0da6cce6ce2c860090f05d031f463412ab534e95"
	address    = "0x67b1d87101671b127f5f8714789c7192f7ad340e"
)

func TestDecodeList(t *testing.T) {
	require := require.New(t)

	privateKey, _ := crypto.HexToECDSA(privateKey)
	address := common.HexToAddress(address)
	chainConfig := params.AllEthashProtocolChanges

	signature, _ := crypto.Sign(sha3.New256().Sum(nil), privateKey)
	signer := MakeSigner(chainConfig, 1)

	t.Run("DynamicFeeTransaction", func(t *testing.T) {
		tx := DynamicFeeTransaction{
			CommonTx: CommonTx{
				ChainID: uint256.NewInt(chainConfig.ChainID.Uint64()),
				Nonce:   1,
				Value:   uint256.NewInt(20),
				Gas:     1,
				To:      &address,
				Data:    []byte("{\"abi\": []}"),
			},
			Tip:    uint256.NewInt(1),
			FeeCap: uint256.NewInt(1),
		}

		signedTx, err := tx.WithSignature(*signer, signature)
		require.NoError(err)

		buf := bytes.NewBuffer(nil)

		err = signedTx.MarshalBinary(buf)
		require.NoError(err)
		buf.Next(1)

		encodedTx := buf.Bytes()

		decoder := TransactionRLPDecoder{
			rlp.NewStream(bytes.NewReader(encodedTx), uint64(len(encodedTx))),
		}

		err = decoder.Start()
		require.NoError(err)

		chainID, err := decoder.ChainID()
		require.NoError(err)
		require.Equal(signedTx.GetChainID(), chainID)

		nonce, err := decoder.Nonce()
		require.NoError(err)
		require.Equal(signedTx.GetNonce(), nonce)

		tip, err := decoder.Tip()
		require.NoError(err)
		require.Equal(signedTx.GetTip(), tip)

		feeCap, err := decoder.FeeCap()
		require.NoError(err)
		require.Equal(signedTx.GetFeeCap(), feeCap)

		gas, err := decoder.Gas()
		require.NoError(err)
		require.Equal(signedTx.GetGas(), gas)

		to, err := decoder.To()
		require.NoError(err)
		require.Equal(signedTx.GetTo(), to)

		value, err := decoder.Value()
		require.NoError(err)
		require.Equal(signedTx.GetValue(), value)

		data := &[]byte{}
		err = decoder.Data(data)
		require.NoError(err)
		require.Equal(signedTx.GetData(), *data)

		accessList := &AccessList{}
		err = decoder.AccessList(accessList)
		require.NoError(err)

		V, R, S := signedTx.RawSignatureValues()

		v, err := decoder.V()
		require.NoError(err)
		require.Equal(V, v)

		r, err := decoder.R()
		require.NoError(err)
		require.Equal(R, r)

		s, err := decoder.S()
		require.NoError(err)
		require.Equal(S, s)
	})
}

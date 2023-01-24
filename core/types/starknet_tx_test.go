package types

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

var (
	chainConfig = params.AllProtocolChanges
	address     = libcommon.HexToAddress("b94f5374fce5edbc8e2a8697c15331677e6ebf0b")
)

func TestStarknetTxDecodeRLP(t *testing.T) {
	require := require.New(t)

	privateKey, _ := crypto.HexToECDSA(generatePrivateKey(t))

	signature, _ := crypto.Sign(sha3.New256().Sum(nil), privateKey)
	signer := MakeSigner(chainConfig, 1)

	cases := []struct {
		name string
		tx   *StarknetTransaction
	}{
		{name: "with data, without salt", tx: &StarknetTransaction{
			CommonTx: CommonTx{
				ChainID: uint256.NewInt(chainConfig.ChainID.Uint64()),
				Nonce:   1,
				Value:   uint256.NewInt(20),
				Gas:     1,
				To:      &address,
				Data:    []byte("{\"abi\": []}"),
			},
			Salt:   []byte("contract_salt"),
			Tip:    uint256.NewInt(1),
			FeeCap: uint256.NewInt(1),
		}},
		{name: "with data and salt", tx: &StarknetTransaction{
			CommonTx: CommonTx{
				ChainID: uint256.NewInt(chainConfig.ChainID.Uint64()),
				Nonce:   1,
				Value:   uint256.NewInt(20),
				Gas:     1,
				To:      &address,
				Data:    []byte("{\"abi\": []}"),
			},
			Salt:   []byte{},
			Tip:    uint256.NewInt(1),
			FeeCap: uint256.NewInt(1),
		}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tx := tt.tx

			st, err := tx.WithSignature(*signer, signature)
			require.NoError(err)

			signedTx, ok := st.(*StarknetTransaction)
			require.True(ok)

			buf := bytes.NewBuffer(nil)

			err = signedTx.MarshalBinary(buf)
			require.NoError(err)

			encodedTx := buf.Bytes()

			txnObj, err := DecodeTransaction(rlp.NewStream(bytes.NewReader(encodedTx), uint64(len(encodedTx))))
			require.NoError(err)

			txn, ok := txnObj.(*StarknetTransaction)
			require.True(ok)

			require.Equal(signedTx.GetChainID(), txn.GetChainID())
			require.Equal(signedTx.GetNonce(), txn.GetNonce())
			require.Equal(signedTx.GetTip(), txn.GetTip())
			require.Equal(signedTx.GetFeeCap(), txn.GetFeeCap())
			require.Equal(signedTx.GetGas(), txn.GetGas())
			require.Equal(signedTx.GetTo(), txn.GetTo())
			require.Equal(signedTx.GetValue(), txn.GetValue())
			require.Equal(signedTx.GetData(), txn.GetData())
			require.Equal(signedTx.GetSalt(), txn.GetSalt())

			txV, txR, txS := signedTx.RawSignatureValues()
			txnV, txnR, txnS := txn.RawSignatureValues()

			require.Equal(txV, txnV)
			require.Equal(txR, txnR)
			require.Equal(txS, txnS)
		})
	}
}

func generatePrivateKey(t testing.TB) string {
	t.Helper()

	privateKey, err := crypto.GenerateKey()
	if err != nil {
		t.Error(err)
	}

	return hex.EncodeToString(crypto.FromECDSA(privateKey))
}

func starknetTransaction(chainId *big.Int, address libcommon.Address) *StarknetTransaction {
	return &StarknetTransaction{
		CommonTx: CommonTx{
			ChainID: uint256.NewInt(chainId.Uint64()),
			Nonce:   1,
			Value:   uint256.NewInt(20),
			Gas:     1,
			To:      &address,
			Data:    []byte("{\"abi\": []}"),
		},
		Salt:   []byte("contract_salt"),
		Tip:    uint256.NewInt(1),
		FeeCap: uint256.NewInt(1),
	}
}

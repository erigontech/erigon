package tests

import (
	// "math/big"
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	// "github.com/holiman/uint256"
	// "github.com/stretchr/testify/require"

	// "github.com/erigontech/erigon-lib/common"
	// "github.com/erigontech/erigon-lib/common/hexutil"
	// "github.com/consensys/gnark-crypto/ecc/bw6-756/ecdsa"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	// "github.com/stretchr/testify/require"
	// "github.com/erigontech/erigon-lib/log/v3"
	// "github.com/erigontech/erigon-lib/rlp"
	// "github.com/erigontech/erigon-lib/types"
	// "github.com/erigontech/erigon/core"
)

// TestEIP7702DelegateResetRPC demonstrates the RPC caching issue with EIP-7702 delegate reset.
// It shows that direct state access correctly reflects the delegate reset, but the RPC call
// still returns the old delegate due to caching issues.
func TestEIP7702GenTxn(t *testing.T) {
	// This test is expected to fail due to RPC caching issues
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// Create a new private key for the authority
	// authorityKey, _ := crypto.GenerateKey()
	var authorityKey *ecdsa.PrivateKey
	keyD := uint256.NewInt(0)
	err := keyD.SetFromDecimal("78647155152608782651318855194358693339845063559497252549133048081435191698778")
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}
	fmt.Println("Num to hex for key", keyD.Hex())
	authorityKey, err = crypto.HexToECDSA("ade0b65c42e0e6a11e3473c272457ee6a5f5d946ce274a7399d887ec88f8655a")
	if err != nil {
		fmt.Println(err.Error())
	}
	if authorityKey == nil {
		fmt.Println("authorityKey is null")
	} else {
		fmt.Println("authorityKey D: " + authorityKey.D.String())
	}

	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
	fmt.Printf("\n Wallet address %s \n", authority)

	// Choose a delegate address
	chainIdForTest := uint256.NewInt(0x301824)
	// // Magic byte for EIP-7702 authorizations
	 
	magicByte := []byte{0x05}
	offset := uint64(0)
	for n := offset; n < 20; n += 2 {
		fmt.Printf("\n\n======== nonce = %d , %d ===========\n", n, n+1)
		delegateAddr := common.HexToAddress("0xa1A4c60a9f688c7dcD09A38867d45b3fa9d6ce51")
		delegateAddr[0] =  uint8(n)
		if (n+offset)%4 != 0 {
			delegateAddr = common.Address{}
			fmt.Println("Prev Auth revocaction")
		}
		payloadItems := []interface{}{
			chainIdForTest, // Use CORRECT chain ID for signing
			delegateAddr,
			n+1,	// next nonce for authorizatoin
		}
		rlpEncodedPayload, err := rlp.EncodeToBytes(payloadItems)
		require.NoError(t, err)
		message := append(magicByte, rlpEncodedPayload...)
		signingHash := crypto.Keccak256Hash(message)
		signatureBytes, err := crypto.Sign(signingHash[:], authorityKey)
		require.NoError(t, err)
		sigR := new(big.Int).SetBytes(signatureBytes[:32])
		sigS := new(big.Int).SetBytes(signatureBytes[32:64])
		sigV := signatureBytes[64]

		// Ensure signature is in lower-S value range
		secp256k1N, _ := new(big.Int).SetString("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141", 16)
		secp256k1HalfN := new(big.Int).Div(secp256k1N, big.NewInt(2))
		if sigS.Cmp(secp256k1HalfN) > 0 {
			sigS.Sub(secp256k1N, sigS)
			sigV = 1 - sigV
		}

		// Create the authorization
		auth1 := &types.Authorization{
			ChainID: *chainIdForTest,
			Address: delegateAddr,
			Nonce:   n+1,
			R:       *uint256.MustFromBig(sigR),
			S:       *uint256.MustFromBig(sigS),
			YParity: sigV,
		}

		t4txn := types.SetCodeTransaction{
			Authorizations: []types.Authorization{*auth1}}
		t4txn.ChainID = uint256.NewInt(0x301824)
		t4txn.Nonce = n
		t4txn.GasLimit = 100000
		t4txn.FeeCap = uint256.NewInt(0x12a05f200)
		t4txn.TipCap = uint256.NewInt(0x3b9aca00)
		t4txn.Value = uint256.NewInt(0)
		t4txn.To = &common.Address{}
		t4txn.To.SetBytes(common.Hex2Bytes("0x9eee80d19b8c6969ea0cd4855f74ecd13e912ebe"))
		buf := bytes.NewBuffer(nil)
		txn, err := types.SignTx(&t4txn, *types.LatestSignerForChainID(big.NewInt(0x301824)), authorityKey)
		err = txn.MarshalBinary(buf)
		require.NoError(t, err)
		fmt.Printf("\nDelegate Addr: %x", delegateAddr)
		fmt.Printf("\nTxn marshalled 0x%x \n", buf.Bytes())
	}

	// // Apply the message
	// gp := new(core.GasPool).AddGas(block.GasLimit())
	// result1, err := core.ApplyMessage(evm, msg1, gp, true, false, nil)
	// require.NoError(t, err)
	// require.Nil(t, result1.Err)

	// // Verify that the delegate is set correctly
	// code, err := statedb.GetCode(authority)
	// require.NoError(t, err)
	// require.NotEmpty(t, code, "Code at authority address should not be empty after delegate is set")

	// // Get code directly from the state
	// codeDirect, err := getCodeDirect(authority)
	// require.NoError(t, err)

	// // This assertion should pass since we're using direct state access
	// expectedCode := append([]byte{0xef, 0x01, 0x00}, delegateAddr.Bytes()...)
	// require.Equal(t, expectedCode, codeDirect, "Code at authority address should contain delegate address (direct access)")

	// // Get code via our simulated RPC
	// codeRPC, err := getCodeRPC(authority)
	// require.NoError(t, err)

	// // This assertion should pass for the first call
	// t.Logf("RPC code (first call): %x, Expected: %x", []byte(codeRPC), expectedCode)
	// require.Equal(t, hexutil.Bytes(expectedCode), codeRPC, "Code at authority address should contain delegate address (RPC call)")

	// // Create the second authorization to reset the delegate to zero address
	// // Create the payload for the second authorization (resetting delegate)
	// payloadItems = []interface{}{
	// 	correctAuthChainID, // Use CORRECT chain ID for signing
	// 	common.Address{},   // Zero address to reset delegation
	// 	uint64(3),          // Use uint64 instead of int for RLP encoding - nonce gap!
	// }
	// rlpEncodedPayload, err = rlp.EncodeToBytes(payloadItems)
	// require.NoError(t, err)
	// message = append(magicByte, rlpEncodedPayload...)
	// signingHash = crypto.Keccak256Hash(message)
	// signatureBytes, err = crypto.Sign(signingHash[:], authorityKey)
	// require.NoError(t, err)
	// sigR = new(big.Int).SetBytes(signatureBytes[:32])
	// sigS = new(big.Int).SetBytes(signatureBytes[32:64])
	// sigV = signatureBytes[64]

	// // Ensure signature is in lower-S value range
	// if sigS.Cmp(secp256k1HalfN) > 0 {
	// 	sigS.Sub(secp256k1N, sigS)
	// 	sigV = 1 - sigV
	// }

	// auth2 := &types.Authorization{
	// 	ChainID: *correctAuthChainID,
	// 	Address: common.Address{}, // Zero address to reset delegation
	// 	Nonce:   3,
	// 	R:       *uint256.MustFromBig(sigR),
	// 	S:       *uint256.MustFromBig(sigS),
	// 	YParity: sigV,
	// }

	// // Create a message with the authorization
	// msg2 := types.NewMessage(
	// 	authority,                  // sender (use the same authority address that has funds)
	// 	&common.Address{},          // to (not important for this test)
	// 	2,                          // nonce
	// 	uint256.NewInt(0),          // value
	// 	1000000,                    // gas
	// 	uint256.NewInt(2000000000), // gas price (2 Gwei)
	// 	uint256.NewInt(2000000000), // fee cap (2 Gwei)
	// 	uint256.NewInt(1000000000), // tip cap (1 Gwei)
	// 	nil,                        // data
	// 	nil,                        // access list
	// 	false,                      // check nonce
	// 	false,                      // is free
	// 	nil,                        // blob fee cap
	// )
	// msg2.SetAuthorizations([]types.Authorization{*auth2})

	// // Apply the message
	// gp = new(core.GasPool).AddGas(block.GasLimit())
	// result2, err := core.ApplyMessage(evm, msg2, gp, true, false, nil)
	// require.NoError(t, err)
	// require.Nil(t, result2.Err)

	// // Verify that the delegate is reset to zero using direct state access
	// code, err = statedb.GetCode(authority)
	// require.NoError(t, err)
	// require.Empty(t, code, "Code at authority address should be empty after delegate reset (direct state access)")

	// // Get code directly from the state
	// codeDirect, err = getCodeDirect(authority)
	// require.NoError(t, err)
	// require.Empty(t, codeDirect, "Code at authority address should be empty after delegate reset (direct access)")

	// // Get code via our simulated RPC
	// codeRPC, err = getCodeRPC(authority)
	// require.NoError(t, err)

	// // This assertion should fail due to our simulated caching
	// // The RPC call should return the old delegate address from the cache
	// t.Logf("RPC code after reset (second call): %x, Expected: empty", []byte(codeRPC))

	// // This test is expected to fail - we're demonstrating the caching issue
	// // The direct state access shows empty code, but the RPC call still returns the old delegate
	// if len(codeRPC) == 0 {
	// 	t.Error("Expected RPC call to return cached delegate address, but got empty code")
	// } else {
	// 	t.Logf("Successfully demonstrated caching issue: direct state shows empty code, but RPC returns cached delegate")
	// }
}

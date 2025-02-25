package bls_test

import (
	"encoding/hex"
	"testing"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/stretchr/testify/require"
)

func convertHexToPublicKey(h string) []byte {
	b, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	pk := make([]byte, 48)
	copy(pk, b)
	return pk
}

func convertHexToSignature(h string) []byte {
	b, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	pk := make([]byte, 96)
	copy(pk, b)
	return pk
}

func convertHexToPrivateKey(h string) []byte {
	b, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	pk := make([]byte, 32)
	copy(pk, b)
	return pk
}

func convertHexToMessage(h string) []byte {
	b, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	return b
}

func TestAggregateSignature(t *testing.T) {
	valid, err := bls.VerifyAggregate(
		convertHexToSignature("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930"),
		convertHexToMessage("abababababababababababababababababababababababababababababababab"),
		[][]byte{
			convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
			convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
			convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
		})
	require.NoError(t, err)
	require.True(t, valid)
}

func TestSigningAndVerifying(t *testing.T) {
	// Create privatekey
	privateKey, err := bls.NewPrivateKeyFromBytes(convertHexToPrivateKey("328388aff0d4a5b7dc9205abd374e7e98f3cd9f3418edb4eafda5fb16473d216"))
	require.NoError(t, err)
	msg := convertHexToMessage("5656565656565656565656565656565656565656565656565656565656565656")
	signature := privateKey.Sign(msg)
	require.True(t, signature.Verify(msg, privateKey.PublicKey()))
	require.NoError(t, err)
}

func TestAggregateVerifyInfinity(t *testing.T) {
	valid, err := bls.VerifyMultipleSignatures(
		[][]byte{
			convertHexToSignature("9104e74b9dfd3ad502f25d6a5ef57db0ed7d9a0e00f3500586d8ce44231212542fcfaf87840539b398bf07626705cf1105d246ca1062c6c2e1a53029a0f790ed5e3cb1f52f8234dc5144c45fc847c0cd37a92d68e7c5ba7c648a8a339f171244"),
			convertHexToSignature("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930"),
			convertHexToSignature("9104e74b9dfd3ad502f25d6a5ef57db0ed7d9a0e00f3500586d8ce44231212542fcfaf87840539b398bf07626705cf1105d246ca1062c6c2e1a53029a0f790ed5e3cb1f52f8234dc5144c45fc847c0cd37a92d68e7c5ba7c648a8a339f171244"),
		}, [][]byte{
			convertHexToMessage("0000000000000000000000000000000000000000000000000000000000000000"),
			convertHexToMessage("5656565656565656565656565656565656565656565656565656565656565656"),
			convertHexToMessage("abababababababababababababababababababababababababababababababab"),
		},
		[][]byte{
			convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
			convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
			convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
		})
	require.NoError(t, err)
	require.False(t, valid)
}

func TestRandomPrivateKey(t *testing.T) {
	// Create privatekey
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	msg := convertHexToMessage("5656565656565656565656565656565656565656565656565656565656565656")
	signature := privateKey.Sign(msg)
	require.True(t, signature.Verify(msg, privateKey.PublicKey()))
	require.NoError(t, err)
}

func TestVerify(t *testing.T) {
	// Create privatekey
	valid, err := bls.Verify(convertHexToSignature("af1390c3c47acdb37131a51216da683c509fce0e954328a59f93aebda7e4ff974ba208d9a4a2a2389f892a9d418d618418dd7f7a6bc7aa0da999a9d3a5b815bc085e14fd001f6a1948768a3f4afefc8b8240dda329f984cb345c6363272ba4fe"),
		convertHexToMessage("5656565656565656565656565656565656565656565656565656565656565656"),
		convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"))
	require.NoError(t, err)
	require.True(t, valid)
}

func TestAggregateSignatures(t *testing.T) {
	msg := convertHexToMessage("5656565656565656565656565656565656565656565656565656565656565656")

	privateKey1, err := bls.GenerateKey()
	require.NoError(t, err)
	privateKey2, err := bls.GenerateKey()
	require.NoError(t, err)

	signature1 := privateKey1.Sign(msg)
	signature2 := privateKey2.Sign(msg)

	publicKey1 := privateKey1.PublicKey()
	publicKey2 := privateKey2.PublicKey()

	aggregatedPublicKey, err := bls.AggregatePublickKeys([][]byte{bls.CompressPublicKey(publicKey1), bls.CompressPublicKey(publicKey2)})
	require.NoError(t, err)

	aggregatedSignature, err := bls.AggregateSignatures([][]byte{signature1.Bytes(), signature2.Bytes()})
	require.NoError(t, err)

	valid, err := bls.Verify(aggregatedSignature, msg, aggregatedPublicKey)
	require.NoError(t, err)
	require.True(t, valid)
}

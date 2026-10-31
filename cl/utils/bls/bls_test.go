package bls_test

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/stretchr/testify/require"
	blst "github.com/supranational/blst/bindings/go"
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

func generateAggregateInputs(t testing.TB, count int, message []byte) ([]bls.PublicKey, [][]byte, [][]byte) {
	t.Helper()
	publicKeys := make([]bls.PublicKey, count)
	publicKeyBytes := make([][]byte, count)
	signatures := make([][]byte, count)
	for i := range count {
		var ikm [32]byte
		binary.LittleEndian.PutUint64(ikm[:], uint64(i+1))
		privateKey, err := bls.NewPrivateKeyFromIKM(ikm[:])
		require.NoError(t, err)
		publicKeys[i] = privateKey.PublicKey()
		publicKeyBytes[i] = bls.CompressPublicKey(publicKeys[i])
		signatures[i] = privateKey.Sign(message).Bytes()
	}
	return publicKeys, publicKeyBytes, signatures
}

func aggregateSignature(t testing.TB, signatures [][]byte) *bls.Signature {
	t.Helper()
	aggregated, err := bls.AggregateSignatures(signatures)
	require.NoError(t, err)
	signature, err := bls.NewSignatureFromBytes(aggregated)
	require.NoError(t, err)
	return signature
}

func negativeSignature(t testing.TB, signature []byte) []byte {
	t.Helper()
	affine := new(blst.P2Affine).Uncompress(signature)
	require.NotNil(t, affine)
	return new(blst.P2).Sub(affine).ToAffine().Compress()
}

func TestAggregateSignature(t *testing.T) {
	valid, err := bls.VerifyAggregate(
		convertHexToSignature("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930"),
		convertHexToMessage("abababababababababababababababababababababababababababababababab"),
		[][]byte{
			convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
			convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
			convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
		},
	)
	require.NoError(t, err)
	require.True(t, valid)
}

func TestSignatureVerifyAggregate(t *testing.T) {
	const (
		smallCount     = 16
		beforeChunk    = 511
		fullChunk      = 512
		afterChunk     = 513
		twoChunks      = 1024
		afterTwoChunks = 1025
		largeAggregate = 4096
	)
	message := []byte("aggregate verification message")
	wrongMessage := []byte("wrong aggregate verification message")
	publicKeys, _, signatures := generateAggregateInputs(t, largeAggregate, message)
	singleSignature := aggregateSignature(t, signatures[:1])
	smallSignature := aggregateSignature(t, signatures[:smallCount])
	beforeChunkSignature := aggregateSignature(t, signatures[:beforeChunk])
	fullChunkSignature := aggregateSignature(t, signatures[:fullChunk])
	afterChunkSignature := aggregateSignature(t, signatures[:afterChunk])
	twoChunksSignature := aggregateSignature(t, signatures[:twoChunks])
	afterTwoChunksSignature := aggregateSignature(t, signatures[:afterTwoChunks])
	largeSignature := aggregateSignature(t, signatures[:largeAggregate])
	publicKeysWithNil := append([]bls.PublicKey(nil), publicKeys[:afterTwoChunks]...)
	publicKeysWithNil[afterTwoChunks/2] = nil

	tests := []struct {
		name       string
		signature  *bls.Signature
		message    []byte
		publicKeys []bls.PublicKey
		want       bool
	}{
		{name: "single key", signature: singleSignature, message: message, publicKeys: publicKeys[:1], want: true},
		{name: "small aggregate", signature: smallSignature, message: message, publicKeys: publicKeys[:smallCount], want: true},
		{name: "before chunk boundary", signature: beforeChunkSignature, message: message, publicKeys: publicKeys[:beforeChunk], want: true},
		{name: "at chunk boundary", signature: fullChunkSignature, message: message, publicKeys: publicKeys[:fullChunk], want: true},
		{name: "after chunk boundary", signature: afterChunkSignature, message: message, publicKeys: publicKeys[:afterChunk], want: true},
		{name: "two chunks", signature: twoChunksSignature, message: message, publicKeys: publicKeys[:twoChunks], want: true},
		{name: "after two chunks", signature: afterTwoChunksSignature, message: message, publicKeys: publicKeys[:afterTwoChunks], want: true},
		{name: "large aggregate", signature: largeSignature, message: message, publicKeys: publicKeys[:largeAggregate], want: true},
		{name: "wrong message", signature: afterTwoChunksSignature, message: wrongMessage, publicKeys: publicKeys[:afterTwoChunks]},
		{name: "missing key", signature: afterTwoChunksSignature, message: message, publicKeys: publicKeys[:afterTwoChunks-1]},
		{name: "extra key", signature: afterTwoChunksSignature, message: message, publicKeys: publicKeys[:afterTwoChunks+1]},
		{name: "nil key", signature: afterTwoChunksSignature, message: message, publicKeys: publicKeysWithNil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.signature.VerifyAggregate(test.message, test.publicKeys))
		})
	}
}

func TestSignatureVerifyAggregateWorkerBufferReuse(t *testing.T) {
	previousMaxProcs := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousMaxProcs) })

	message := []byte("aggregate verification message")
	wrongMessage := []byte("wrong aggregate verification message")
	publicKeys, _, signatures := generateAggregateInputs(t, 2049, message)
	for _, count := range []int{1025, 2049} {
		t.Run(fmt.Sprintf("keys=%d", count), func(t *testing.T) {
			signature := aggregateSignature(t, signatures[:count])
			require.True(t, signature.VerifyAggregate(message, publicKeys[:count]))
			require.False(t, signature.VerifyAggregate(wrongMessage, publicKeys[:count]))
			require.False(t, signature.VerifyAggregate(message, publicKeys[:count-1]))
		})
	}
}

func TestSignatureVerifyAggregateBatchInputs(t *testing.T) {
	message := []byte("aggregate verification message")
	publicKeys, _, signatures := generateAggregateInputs(t, 513, message)
	negativePublicKey := bls.PublicKey(new(blst.P1).Sub((*blst.P1Affine)(publicKeys[0])).ToAffine())
	negatedSignature := negativeSignature(t, signatures[0])

	duplicateKeys := append([]bls.PublicKey{publicKeys[0], publicKeys[0]}, publicKeys[1:16]...)
	duplicateSignatures := append([][]byte{signatures[0], signatures[0]}, signatures[1:16]...)
	adjacentCancellingKeys := append([]bls.PublicKey{publicKeys[0], negativePublicKey}, publicKeys[1:17]...)
	adjacentCancellingSignatures := append([][]byte{signatures[0], negatedSignature}, signatures[1:17]...)
	boundaryCancellingKeys := append([]bls.PublicKey(nil), publicKeys[1:512]...)
	boundaryCancellingKeys = append(boundaryCancellingKeys, publicKeys[0], negativePublicKey)
	boundaryCancellingSignatures := append([][]byte(nil), signatures[1:512]...)
	boundaryCancellingSignatures = append(boundaryCancellingSignatures, signatures[0], negatedSignature)

	tests := []struct {
		name       string
		publicKeys []bls.PublicKey
		signatures [][]byte
	}{
		{name: "adjacent duplicate", publicKeys: duplicateKeys, signatures: duplicateSignatures},
		{name: "adjacent cancelling pair", publicKeys: adjacentCancellingKeys, signatures: adjacentCancellingSignatures},
		{name: "cancelling pair across chunk boundary", publicKeys: boundaryCancellingKeys, signatures: boundaryCancellingSignatures},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			signature := aggregateSignature(t, test.signatures)
			require.True(t, signature.VerifyAggregate(message, test.publicKeys))
		})
	}
}

func TestSignatureVerifyAggregatePreservesKeyMultiplicityAndOrder(t *testing.T) {
	message := []byte("aggregate verification message")
	publicKeys, _, signatures := generateAggregateInputs(t, 2, message)
	aggregated := aggregateSignature(t, [][]byte{signatures[0], signatures[0], signatures[1]})

	repeatedKeys := []bls.PublicKey{publicKeys[0], publicKeys[0], publicKeys[1]}
	reorderedKeys := []bls.PublicKey{publicKeys[1], publicKeys[0], publicKeys[0]}
	require.True(t, aggregated.VerifyAggregate(message, repeatedKeys))
	require.True(t, aggregated.VerifyAggregate(message, reorderedKeys))
}

func TestVerifyAggregateRejectsCancellingPublicKeys(t *testing.T) {
	message := []byte("aggregate verification message")
	publicKeys, publicKeyBytes, _ := generateAggregateInputs(t, 1, message)
	negativePublicKey := new(blst.P1).Sub((*blst.P1Affine)(publicKeys[0])).ToAffine().Compress()

	valid, err := bls.VerifyAggregate(
		bls.InfiniteSignature[:],
		message,
		[][]byte{publicKeyBytes[0], negativePublicKey},
	)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestVerifyAggregateEmptyPublicKeys(t *testing.T) {
	message := []byte("aggregate verification message")
	_, publicKeyBytes, signatures := generateAggregateInputs(t, 1, message)

	valid, err := bls.VerifyAggregate(signatures[0], message, nil)
	require.NoError(t, err)
	require.False(t, valid)

	valid, err = bls.VerifyAggregate(bls.InfiniteSignature[:], message, nil)
	require.NoError(t, err)
	require.True(t, valid)

	valid, err = bls.VerifyAggregate(signatures[0], message, publicKeyBytes[:0])
	require.NoError(t, err)
	require.False(t, valid)
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
		},
	)
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

func TestSignatureAggregatorMatchesAggregateSignatures(t *testing.T) {
	var sigs [][]byte
	var aggregator bls.SignatureAggregator
	for i := range 5 {
		key, err := bls.GenerateKey()
		require.NoError(t, err)
		sig := key.Sign([]byte{byte(i)}).Bytes()
		sigs = append(sigs, sig)
		require.NoError(t, aggregator.AddValidated(sig))
	}
	expected, err := bls.AggregateSignatures(sigs)
	require.NoError(t, err)

	got := aggregator.Compressed()
	require.Equal(t, expected, got[:])
}

func TestSignatureAggregatorRejectsMalformedSignature(t *testing.T) {
	var aggregator bls.SignatureAggregator
	require.ErrorIs(t, aggregator.AddValidated(make([]byte, 96)), bls.ErrDeserializeSignature)
	require.Error(t, aggregator.AddValidated(make([]byte, 95)))
}

package bls_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/stretchr/testify/require"
)

func TestAggregateSignatureCached(t *testing.T) {
	bls.SetEnabledCaching(true)
	bls.LoadPublicKeyIntoCache(convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"), true)
	bls.LoadPublicKeyIntoCache(convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"), false)

	for i := 0; i < 10; i++ {
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
	bls.SetEnabledCaching(false)
}

/*
BenchmarkAggregateSigCached-12    	    1779	    669063 ns/op	    6834 B/op	      47 allocs/op
PASS
ok  	github.com/erigontech/erigon/cl/utils/bls	2.445s
*/
func BenchmarkAggregateSigCached(b *testing.B) {
	bls.SetEnabledCaching(true)
	bls.LoadPublicKeyIntoCache(convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"), false)
	bls.LoadPublicKeyIntoCache(convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"), false)
	bls.LoadPublicKeyIntoCache(convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"), false)

	for i := 0; i < b.N; i++ {
		bls.VerifyAggregate(
			convertHexToSignature("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930"),
			convertHexToMessage("abababababababababababababababababababababababababababababababab"),
			[][]byte{
				convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
				convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
				convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
				convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
				convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
				convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
				convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
				convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
				convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
				convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
				convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
				convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
			})
	}
	bls.SetEnabledCaching(false)
}

/*
BenchmarkAggregateSigNonCached-12    	     900	   1247389 ns/op	   12602 B/op	     108 allocs/op
PASS
*/
func BenchmarkAggregateSigNonCached(b *testing.B) {
	bls.SetEnabledCaching(false)
	for i := 0; i < b.N; i++ {
		bls.VerifyAggregate(
			convertHexToSignature("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930"),
			convertHexToMessage("abababababababababababababababababababababababababababababababab"),
			[][]byte{
				convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
				convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
				convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
				convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
				convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
				convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
				convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
				convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
				convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
				convertHexToPublicKey("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a"),
				convertHexToPublicKey("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81"),
				convertHexToPublicKey("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f"),
			})
	}
}

package kzg

import (
	"errors"
	"fmt"
	"math/big"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"

	libkzg "github.com/ledgerwatch/erigon-lib/crypto/kzg"
)

const (
	BlobCommitmentVersionKZG uint8 = 0x01
	PrecompileInputLength    int   = 192
)

var (
	errInvalidInputLength = errors.New("invalid input length")
)

// The value that gets returned when the `verify_kzg_proof“ precompile is called
var precompileReturnValue [64]byte

// InitializeCrypytoCtx initializes the global context object returned via CryptoCtx
func init() {
	new(big.Int).SetUint64(gokzg4844.ScalarsPerBlob).FillBytes(precompileReturnValue[:32])
	copy(precompileReturnValue[32:], gokzg4844.BlsModulus[:])
}

// PointEvaluationPrecompile implements point_evaluation_precompile from EIP-4844
func PointEvaluationPrecompile(input []byte) ([]byte, error) {
	if len(input) != PrecompileInputLength {
		return nil, errInvalidInputLength
	}
	// versioned hash: first 32 bytes
	var versionedHash [32]byte
	copy(versionedHash[:], input[:32])

	var x, y [32]byte
	// Evaluation point: next 32 bytes
	copy(x[:], input[32:64])
	// Expected output: next 32 bytes
	copy(y[:], input[64:96])

	// input kzg point: next 48 bytes
	var dataKZG [48]byte
	copy(dataKZG[:], input[96:144])
	if libkzg.KZGToVersionedHash(dataKZG) != versionedHash {
		return nil, errors.New("mismatched versioned hash")
	}

	// Quotient kzg: next 48 bytes
	var quotientKZG [48]byte
	copy(quotientKZG[:], input[144:PrecompileInputLength])

	cryptoCtx := libkzg.Ctx()
	err := cryptoCtx.VerifyKZGProof(dataKZG, x, y, quotientKZG)
	if err != nil {
		return nil, fmt.Errorf("verify_kzg_proof error: %v", err)
	}

	result := precompileReturnValue // copy the value

	return result[:], nil
}

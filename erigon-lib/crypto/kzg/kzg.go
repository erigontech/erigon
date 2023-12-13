package kzg

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
)

const (
	BlobCommitmentVersionKZG uint8 = 0x01
	PrecompileInputLength    int   = 192
)

type VersionedHash [32]byte

var (
	errInvalidInputLength = errors.New("invalid input length")

	// The value that gets returned when the `verify_kzg_proof“ precompile is called
	precompileReturnValue [64]byte

	trustedSetupFile string

	gokzgCtx      *gokzg4844.Context
	initCryptoCtx sync.Once
)

func init() {
	new(big.Int).SetUint64(gokzg4844.ScalarsPerBlob).FillBytes(precompileReturnValue[:32])
	copy(precompileReturnValue[32:], gokzg4844.BlsModulus[:])
}

func SetTrustedSetupFilePath(path string) {
	trustedSetupFile = path
}

// InitKZGCtx initializes the global context object returned via CryptoCtx
func InitKZGCtx() {
	initCryptoCtx.Do(func() {
		if trustedSetupFile != "" {
			file, err := os.ReadFile(trustedSetupFile)
			if err != nil {
				panic(fmt.Sprintf("could not read file, err: %v", err))
			}

			setup := new(gokzg4844.JSONTrustedSetup)
			if err = json.Unmarshal(file, setup); err != nil {
				panic(fmt.Sprintf("could not unmarshal, err: %v", err))
			}

			gokzgCtx, err = gokzg4844.NewContext4096(setup)
			if err != nil {
				panic(fmt.Sprintf("could not create KZG context, err: %v", err))
			}
		} else {
			var err error
			// Initialize context to match the configurations that the
			// specs are using.
			gokzgCtx, err = gokzg4844.NewContext4096Secure()
			if err != nil {
				panic(fmt.Sprintf("could not create context, err : %v", err))
			}
		}
	})
}

// Ctx returns a context object that stores all of the necessary configurations to allow one to
// create and verify blob proofs.  This function is expensive to run if the crypto context isn't
// initialized, so production services should pre-initialize by calling InitKZGCtx.
func Ctx() *gokzg4844.Context {
	InitKZGCtx()
	return gokzgCtx
}

// KZGToVersionedHash implements kzg_to_versioned_hash from EIP-4844
func KZGToVersionedHash(kzg gokzg4844.KZGCommitment) VersionedHash {
	h := sha256.Sum256(kzg[:])
	h[0] = BlobCommitmentVersionKZG

	return VersionedHash(h)
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
	if KZGToVersionedHash(dataKZG) != versionedHash {
		return nil, errors.New("mismatched versioned hash")
	}

	// Quotient kzg: next 48 bytes
	var quotientKZG [48]byte
	copy(quotientKZG[:], input[144:PrecompileInputLength])

	cryptoCtx := Ctx()
	err := cryptoCtx.VerifyKZGProof(dataKZG, x, y, quotientKZG)
	if err != nil {
		return nil, fmt.Errorf("verify_kzg_proof error: %w", err)
	}

	result := precompileReturnValue // copy the value

	return result[:], nil
}

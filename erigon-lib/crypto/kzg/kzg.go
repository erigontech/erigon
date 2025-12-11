// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package kzg

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
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

	gokzgCtx      *goethkzg.Context
	initCryptoCtx sync.Once
)

func init() {
	new(big.Int).SetUint64(goethkzg.ScalarsPerBlob).FillBytes(precompileReturnValue[:32])
	copy(precompileReturnValue[32:], goethkzg.BlsModulus[:])
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

			setup := new(goethkzg.JSONTrustedSetup)
			if err = json.Unmarshal(file, setup); err != nil {
				panic(fmt.Sprintf("could not unmarshal, err: %v", err))
			}

			gokzgCtx, err = goethkzg.NewContext4096(setup)
			if err != nil {
				panic(fmt.Sprintf("could not create KZG context, err: %v", err))
			}
		} else {
			var err error
			// Initialize context to match the configurations that the
			// specs are using.
			gokzgCtx, err = goethkzg.NewContext4096Secure()
			if err != nil {
				panic(fmt.Sprintf("could not create context, err : %v", err))
			}
		}
	})
}

// Ctx returns a context object that stores all of the necessary configurations to allow one to
// create and verify blob proofs.  This function is expensive to run if the crypto context isn't
// initialized, so production services should pre-initialize by calling InitKZGCtx.
func Ctx() *goethkzg.Context {
	InitKZGCtx()
	return gokzgCtx
}

// KZGToVersionedHash implements kzg_to_versioned_hash from EIP-4844
func KZGToVersionedHash(kzg goethkzg.KZGCommitment) VersionedHash {
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

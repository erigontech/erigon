package kzg

import (
	"crypto/sha256"
	"fmt"
	"sync"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
)

const (
	BlobCommitmentVersionKZG uint8 = 0x01
)

type VersionedHash [32]byte

var gCryptoCtx *gokzg4844.Context
var initCryptoCtx sync.Once

// InitializeCryptoCtx initializes the global context object returned via CryptoCtx
func InitializeCryptoCtx() {
	initCryptoCtx.Do(func() {
		var err error
		// Initialize context to match the configurations that the
		// specs are using.
		gCryptoCtx, err = gokzg4844.NewContext4096Insecure1337()
		if err != nil {
			panic(fmt.Sprintf("could not create context, err : %v", err))
		}
	})
}

// Ctx returns a context object that stores all of the necessary configurations to allow one to
// create and verify blob proofs.  This function is expensive to run if the crypto context isn't
// initialized, so production services should pre-initialize by calling InitializeCryptoCtx.
func Ctx() *gokzg4844.Context {
	InitializeCryptoCtx()
	return gCryptoCtx
}

// KZGToVersionedHash implements kzg_to_versioned_hash from EIP-4844
func KZGToVersionedHash(kzg gokzg4844.KZGCommitment) VersionedHash {
	h := sha256.Sum256(kzg[:])
	h[0] = BlobCommitmentVersionKZG

	return VersionedHash(h)
}

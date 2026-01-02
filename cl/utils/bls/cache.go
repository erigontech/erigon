package bls

import (
	blst "github.com/supranational/blst/bindings/go"

	"github.com/erigontech/erigon/common/maphash"
)

var (
	pkCache      *maphash.Map[*blst.P1Affine]
	enabledCache bool
)

// init is used to initialize cache
func init() {
	pkCache = maphash.NewMap[*blst.P1Affine]()
}

func SetEnabledCaching(caching bool) {
	enabledCache = caching
}

func ClearCache() {
	pkCache = maphash.NewMap[*blst.P1Affine]()
}

func loadPublicKeyIntoCache(publicKey []byte, validate bool) error {
	if len(publicKey) != publicKeyLength {
		return ErrDeserializePublicKey
	}
	if _, ok := pkCache.Get(publicKey); ok {
		return nil
	}
	// Subgroup check NOT done when decompressing pubkey.
	publicKeyDecompressed := new(blst.P1Affine).Uncompress(publicKey)
	if publicKeyDecompressed == nil {
		return ErrDeserializePublicKey
	}
	// Subgroup and infinity check
	if validate && !publicKeyDecompressed.KeyValidate() {
		return ErrInfinitePublicKey
	}
	pkCache.Set(publicKey, publicKeyDecompressed)
	return nil
}

func LoadPublicKeyIntoCache(publicKey []byte, validate bool) error {
	if !enabledCache {
		return ErrCacheNotEnabled
	}
	return loadPublicKeyIntoCache(publicKey, validate)
}

func getAffineFromCache(key []byte) *blst.P1Affine {
	if !enabledCache {
		return nil
	}
	if len(key) != publicKeyLength {
		return nil
	}
	affine, _ := pkCache.Get(key)
	return affine
}

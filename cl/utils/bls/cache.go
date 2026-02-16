package bls

import (
	blst "github.com/supranational/blst/bindings/go"

	"github.com/erigontech/erigon/common/maphash"
)

var (
	pkCache      *maphash.Map[*blst.P1Affine]
	enabledCache bool
)

func init() {
	pkCache = maphash.NewMap[*blst.P1Affine]()
}

func SetEnabledCaching(caching bool) {
	enabledCache = caching
}

func ClearCache() {
	pkCache = maphash.NewMap[*blst.P1Affine]()
}

func LoadPublicKeyIntoCache(publicKey []byte, validate bool) error {
	if !enabledCache {
		return ErrCacheNotEnabled
	}
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

func loadAffineIntoCache(key []byte, affine *blst.P1Affine) {
	if !enabledCache {
		return
	}
	pkCache.Set(key, affine)
}

func getAffineFromCache(key []byte) *blst.P1Affine {
	if !enabledCache {
		return nil
	}
	if len(key) != publicKeyLength {
		return nil
	}
	v, _ := pkCache.Get(key)
	return v
}

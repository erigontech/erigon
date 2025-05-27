package bls

import (
	"bytes"
	"sort"
	"sync"

	blst "github.com/supranational/blst/bindings/go"
)

var (
	pkCache      *publicKeysCache
	enabledCache bool
)

// We build a basic cache to avoid allocs with sync.Map.
type kvCache struct {
	key   []byte
	value *blst.P1Affine
}

type publicKeysCache struct {
	cache [][]kvCache

	mu sync.RWMutex
}

const baseCacheLayer = 16384

// init is used to initialize cache
func init() {
	pkCache = &publicKeysCache{}
	pkCache.cache = make([][]kvCache, baseCacheLayer)
}

func SetEnabledCaching(caching bool) {
	enabledCache = caching
}

func ClearCache() {
	pkCache.cache = make([][]kvCache, baseCacheLayer)
}

func (p *publicKeysCache) loadPublicKeyIntoCache(publicKey []byte, validate bool) error {
	if len(publicKey) != publicKeyLength {
		return ErrDeserializePublicKey
	}
	if affine := p.getAffineFromCache(publicKey); affine != nil {
		return nil
	}
	// Subgroup check NOT done when decompressing pubkey.
	publicKeyDecompressed := new(blst.P1Affine).Uncompress(publicKey)
	if p == nil {
		return ErrDeserializePublicKey
	}
	// Subgroup and infinity check
	if validate && !publicKeyDecompressed.KeyValidate() {
		return ErrInfinitePublicKey
	}
	p.loadAffineIntoCache(publicKey, publicKeyDecompressed)
	return nil
}

func copyBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func (p *publicKeysCache) loadAffineIntoCache(key []byte, affine *blst.P1Affine) {
	if !enabledCache {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var idx int
	for i := 0; i < publicKeyLength; i++ {
		idx += int(key[i])
	}
	baseIdx := idx % baseCacheLayer
	p.cache[baseIdx] = append(p.cache[baseIdx], kvCache{key: copyBytes(key), value: affine})
	sort.Slice(p.cache[baseIdx], func(i, j int) bool {
		return bytes.Compare(p.cache[baseIdx][i].key, p.cache[baseIdx][j].key) < 0
	})
}

func LoadPublicKeyIntoCache(publicKey []byte, validate bool) error {
	if !enabledCache {
		return ErrCacheNotEnabled
	}
	return pkCache.loadPublicKeyIntoCache(publicKey, validate)
}

func (p *publicKeysCache) getAffineFromCache(key []byte) *blst.P1Affine {
	if !enabledCache {
		return nil
	}
	if len(key) != publicKeyLength {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	var idx int
	for i := 0; i < publicKeyLength; i++ {
		idx += int(key[i])
	}

	candidates := p.cache[idx%baseCacheLayer]

	i := sort.Search(len(candidates), func(i int) bool {
		return bytes.Compare(candidates[i].key, key) >= 0
	})
	if i < len(candidates) && bytes.Equal(candidates[i].key, key) {
		return candidates[i].value
	}

	return nil
}

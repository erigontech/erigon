package bls

import (
	"errors"
	"fmt"

	blst "github.com/supranational/blst/bindings/go"
)

// Length of a BLS public key
const publicKeyLength = 48

// PublicKey wraps the CGO object of the BLST library and give us easy access to its methods.
type PublicKey *blst.P1Affine

// NewPublicKey makes new empty Public Key.
func NewPublicKey() PublicKey {
	return new(blst.P1Affine)
}

func CompressPublicKey(p PublicKey) []byte {
	return (*blst.P1Affine)(p).Compress()
}

// NewPublicKeyFromBytes Derive new public key from a 48 long byte slice.
func NewPublicKeyFromBytes(b []byte) (PublicKey, error) {
	return newPublicKeyFromBytes(b, true)
}

func newPublicKeyFromBytes(b []byte, loadInCache bool) (PublicKey, error) {
	if len(b) != publicKeyLength {
		return nil, fmt.Errorf("bls(public): invalid key length. should be %d", publicKeyLength)
	}

	cachedAffine := pkCache.getAffineFromCache(b)
	if cachedAffine != nil {
		return cachedAffine, nil
	}

	// Subgroup check NOT done when decompressing pubkey.
	p := new(blst.P1Affine).Uncompress(b)
	if p == nil {
		return nil, ErrDeserializePublicKey
	}
	// Subgroup and infinity check
	if !p.KeyValidate() {
		return nil, ErrInfinitePublicKey
	}
	if loadInCache {
		pkCache.loadAffineIntoCache(b, p)
	}

	return p, nil
}

func AggregatePublickKeys(pubs [][]byte) ([]byte, error) {
	if len(pubs) == 0 {
		return nil, errors.New("nil or empty public keys")
	}
	agg := new(blst.P1Aggregate)
	mulP1 := make([]*blst.P1Affine, 0, len(pubs))
	for _, pubkey := range pubs {
		pubKeyObj, err := NewPublicKeyFromBytes(pubkey)
		if err != nil {
			return nil, err
		}
		mulP1 = append(mulP1, pubKeyObj)
	}
	// No group check needed here since it is done in PublicKeyFromBytes
	// Note the checks could be moved from PublicKeyFromBytes into Aggregate
	// and take advantage of multi-threading.
	agg.Aggregate(mulP1, false)
	return agg.ToAffine().Compress(), nil
}

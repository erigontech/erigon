package devvalidator

import (
	"github.com/erigontech/erigon/cl/clparams/devgenesis"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
)

// ValidatorKey holds a BLS keypair and its on-chain validator index.
type ValidatorKey struct {
	PrivKey        *bls.PrivateKey
	PubKey         bls.PublicKey
	PubKeyBytes    [48]byte
	ValidatorIndex uint64 // set after resolving against beacon state
}

// LoadKeys derives deterministic BLS keys from a seed and prepares them
// for signing. Validator indices are resolved later via the Beacon API.
func LoadKeys(seed string, count int) ([]*ValidatorKey, error) {
	privKeys, err := devgenesis.DeriveKeys(seed, count)
	if err != nil {
		return nil, err
	}

	keys := make([]*ValidatorKey, count)
	for i, priv := range privKeys {
		pub := priv.PublicKey()
		compressed := bls.CompressPublicKey(pub)
		var pubBytes [48]byte
		copy(pubBytes[:], compressed)

		keys[i] = &ValidatorKey{
			PrivKey:     priv,
			PubKey:      pub,
			PubKeyBytes: pubBytes,
		}
	}
	return keys, nil
}

// PubKeyToKey returns a map from compressed pubkey to ValidatorKey for fast lookup.
func PubKeyToKey(keys []*ValidatorKey) map[common.Bytes48]*ValidatorKey {
	m := make(map[common.Bytes48]*ValidatorKey, len(keys))
	for _, k := range keys {
		m[common.Bytes48(k.PubKeyBytes)] = k
	}
	return m
}

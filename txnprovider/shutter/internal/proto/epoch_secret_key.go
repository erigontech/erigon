package proto

import (
	"github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
)

func (k *Key) EpochSecretKey() (crypto.EpochSecretKey, error) {
	epochSecretKey := new(crypto.EpochSecretKey)
	if err := epochSecretKey.Unmarshal(k.GetKey()); err != nil {
		return nil, err
	}

	return epochSecretKey, nil
}

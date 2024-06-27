package state

import (
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
)

type PreimageWriter struct {
	db            kv.Getter
	savePreimages bool
}

func (pw *PreimageWriter) SetSavePreimages(save bool) {
	pw.savePreimages = save
}

func (pw *PreimageWriter) HashAddress(address common.Address, save bool) (common.Hash, error) {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return common.Hash{}, err
	}
	return addrHash, pw.savePreimage(save, addrHash[:], address[:])
}

func (pw *PreimageWriter) HashKey(key *common.Hash, save bool) (common.Hash, error) {
	keyHash, err := common.HashData(key[:])
	if err != nil {
		return common.Hash{}, err
	}
	return keyHash, pw.savePreimage(save, keyHash[:], key[:])
}

func (pw *PreimageWriter) savePreimage(save bool, hash []byte, preimage []byte) error {
	if !save || !pw.savePreimages {
		return nil
	}
	// Following check is to minimise the overwriting the same value of preimage
	// in the database, which would cause extra write churn
	if p, _ := pw.db.GetOne(kv.PreimagePrefix, hash); p != nil {
		return nil
	}

	putter, ok := pw.db.(kv.Putter)

	if !ok {
		return fmt.Errorf("db does not support putter interface")
	}

	return putter.Put(kv.PreimagePrefix, hash, preimage)
}

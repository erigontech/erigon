package state

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
)

type PreimageWriter struct {
	db            ethdb.GetterPutter
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
	if p, _ := pw.db.Get(dbutils.PreimagePrefix, hash); p != nil {
		return nil
	}
	return pw.db.Put(dbutils.PreimagePrefix, hash, preimage)
}

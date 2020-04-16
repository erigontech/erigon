package stateless

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func RegenerateIndex(chaindata string, indexBucket []byte, csBucket []byte) error {
	db, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		return err
	}
	var walker func([]byte) core.ChangesetWalker
	if bytes.Equal(dbutils.AccountChangeSetBucket, indexBucket) {
		walker = func(cs []byte) core.ChangesetWalker {
			return changeset.AccountChangeSetBytes(cs)
		}
	}

	if bytes.Equal(dbutils.StorageChangeSetBucket, indexBucket) {
		walker = func(cs []byte) core.ChangesetWalker {
			return changeset.StorageChangeSetBytes(cs)
		}
	}

	ig := core.NewIndexGenerator(db, csBucket, indexBucket, walker)
	err = ig.GenerateIndex()
	if err != nil {
		return err
	}
	fmt.Println("Index is successfully regenerated")
	return nil
}

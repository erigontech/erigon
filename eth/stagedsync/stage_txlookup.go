package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func spawnTxLookup(db ethdb.Database, dataDir string, quitCh chan struct{}) error {
	err:= etl.Transform(db,
		dbutils.HeaderPrefix,
		dbutils.ContractCodeBucket,
		dataDir,
		extractTx,
		etl.IdentityLoadFunc,
		etl.TransformArgs{Quit: quitCh},
	)
	if err!=nil {
		return err
	}
	return nil
}
func extractTx(k []byte, v []byte, next etl.ExtractNextFunc) error {
	return nil
}

func unwindTxLookup(unwindPoint uint64, db ethdb.Database, quitCh chan struct{}) error {
	return nil
}

package utils

import (
	"errors"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"os"
)

const (
	TypeLMDB = "lmdb"
	TypeMDBX = "mdbx"
)
var ErrUnsupported error = errors.New("unsupported KV type")
func RmTmpFiles(dbType string, snapshotPath string) error  {
	switch dbType {
	case TypeLMDB:
		return rmLmdbLock(snapshotPath)
	case TypeMDBX:
		return rmMdbxLock(snapshotPath)
	default:
		return ErrUnsupported
	}
}
func rmLmdbLock(snapshotPath string) error  {
	err := os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		return err
	}
	return os.Remove(snapshotPath + "/LOCK")
}
func rmMdbxLock(path string) error  {
	return os.Remove(path + "/mdbx.lck")
}


func OpenSnapshotKV(dbType string, configsFunc ethdb.BucketConfigsFunc, path string) ethdb.KV {
	if dbType==TypeLMDB {
		return ethdb.NewLMDB().WithBucketsConfig(configsFunc).Path(path).MustOpen()
	} else if dbType==TypeMDBX {
		return ethdb.NewMDBX().WithBucketsConfig(configsFunc).Path(path).MustOpen()
	}
	panic(ErrUnsupported.Error())
}
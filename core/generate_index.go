package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"os"
	"time"
)

func NewIndexGenerator(db ethdb.Database, quitCh chan struct{}) *IndexGenerator {
	return &IndexGenerator{
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
	}
}

type IndexGenerator struct {
	db               ethdb.Database
	ChangeSetBufSize int
	TempDir          string
	quitCh           chan struct{}
}

var CSMapper = map[string]struct {
	IndexBucket   []byte
	WalkerAdapter func(v []byte) changeset.Walker
	KeySize       int
	Template      string
	New           func() *changeset.ChangeSet
	Encode        func(*changeset.ChangeSet) ([]byte, error)
}{
	string(dbutils.AccountChangeSetBucket): {
		IndexBucket: dbutils.AccountsHistoryBucket,
		WalkerAdapter: func(v []byte) changeset.Walker {
			return changeset.AccountChangeSetBytes(v)
		},
		KeySize:  common.HashLength,
		Template: "acc-ind-",
		New:      changeset.NewAccountChangeSet,
		Encode:   changeset.EncodeAccounts,
	},
	string(dbutils.StorageChangeSetBucket): {
		IndexBucket: dbutils.StorageHistoryBucket,
		WalkerAdapter: func(v []byte) changeset.Walker {
			return changeset.StorageChangeSetBytes(v)
		},
		KeySize:  common.HashLength*2 + common.IncarnationLength,
		Template: "st-ind-",
		New:      changeset.NewStorageChangeSet,
		Encode:   changeset.EncodeStorage,
	},
	string(dbutils.PlainAccountChangeSetBucket): {
		IndexBucket: dbutils.AccountsHistoryBucket,
		WalkerAdapter: func(v []byte) changeset.Walker {
			return changeset.AccountChangeSetPlainBytes(v)
		},
		KeySize:  common.AddressLength,
		Template: "acc-ind-",
		New:      changeset.NewAccountChangeSetPlain,
		Encode:   changeset.EncodeAccountsPlain,
	},
	string(dbutils.PlainStorageChangeSetBucket): {
		IndexBucket: dbutils.StorageHistoryBucket,
		WalkerAdapter: func(v []byte) changeset.Walker {
			return changeset.StorageChangeSetPlainBytes(v)
		},
		KeySize:  common.AddressLength + common.IncarnationLength + common.HashLength,
		Template: "st-ind-",
		New:      changeset.NewStorageChangeSetPlain,
		Encode:   changeset.EncodeStoragePlain,
	},
}

func (ig *IndexGenerator) GenerateIndex(blockNum uint64, changeSetBucket []byte) error {
	v, ok := CSMapper[string(changeSetBucket)]
	if !ok {
		return errors.New("unknown bucket type")
	}
	log.Info("Index generation started", "from", blockNum, "csbucket", string(changeSetBucket))

	t:=time.Now()
	err:=etl.Transform(ig.db,changeSetBucket,
		v.IndexBucket,
		os.TempDir(),
		getExtractFunc(v.WalkerAdapter),
		loadFunc2,
		etl.TransformArgs{
			ExtractStartKey:  dbutils.EncodeTimestamp(blockNum),
			FixedBits:       0,
			BufferType:      etl.SortableAppendBuffer,
			BufferSize: 	ig.ChangeSetBufSize,
			Quit:            ig.quitCh,
		},
	)
	if err!=nil {
		return err
	}

	log.Info("Index generation successfully finished",  "csbucket", string(changeSetBucket),"it took", time.Since(t))

	return nil
}

func (ig *IndexGenerator) Truncate(timestampTo uint64, changeSetBucket []byte) error {
	vv, ok := CSMapper[string(changeSetBucket)]
	if !ok {
		return errors.New("unknown bucket type")
	}

	currentKey := dbutils.EncodeTimestamp(timestampTo)
	keys := make(map[string]struct{})
	err := ig.db.Walk(changeSetBucket, currentKey, 0, func(k, v []byte) (b bool, e error) {
		if err := common.Stopped(ig.quitCh); err != nil {
			return false, err
		}

		currentKey = common.CopyBytes(k)
		err := vv.WalkerAdapter(v).Walk(func(kk []byte, _ []byte) error {
			keys[string(kk)] = struct{}{}
			return nil
		})
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	historyEffects := make(map[string][]byte)
	keySize := vv.KeySize
	if bytes.Equal(dbutils.StorageChangeSetBucket, changeSetBucket) || bytes.Equal(dbutils.PlainStorageChangeSetBucket, changeSetBucket) {
		keySize -= 8
	}

	var startKey = make([]byte, keySize+8)

	for key := range keys {
		key := common.CopyBytes([]byte(key))
		copy(startKey[:keySize], dbutils.CompositeKeyWithoutIncarnation(key))

		binary.BigEndian.PutUint64(startKey[keySize:], timestampTo)
		if err := ig.db.Walk(vv.IndexBucket, startKey, 8*keySize, func(k, v []byte) (bool, error) {
			timestamp := binary.BigEndian.Uint64(k[keySize:]) // the last timestamp in the chunk
			kStr := string(common.CopyBytes(k))
			if timestamp > timestampTo {
				historyEffects[kStr] = nil
				// truncate the chunk
				index := dbutils.WrapHistoryIndex(v)
				index = index.TruncateGreater(timestampTo)
				if len(index) > 8 { // If the chunk is empty after truncation, it gets simply deleted
					// Truncated chunk becomes "the last chunk" with the timestamp 0xffff....ffff
					lastK := make([]byte, len(k))
					copy(lastK, k[:keySize])
					binary.BigEndian.PutUint64(lastK[keySize:], ^uint64(0))
					historyEffects[string(lastK)] = common.CopyBytes(index)
				}
			}
			return true, nil
		}); err != nil {
			return err
		}
	}

	for key, value := range historyEffects {
		if value == nil {
			if err := ig.db.Delete(vv.IndexBucket, []byte(key)); err != nil {
				return err
			}
		} else {
			if err := ig.db.Put(vv.IndexBucket, []byte(key), value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ig *IndexGenerator) DropIndex(bucket []byte) error {
	//todo add truncate to all db
	if bolt, ok := ig.db.(*ethdb.BoltDatabase); ok {
		log.Warn("Remove bucket", "bucket", string(bucket))
		err := bolt.DeleteBucket(bucket)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("imposible to drop")
}



func loadFunc2(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error { //nolint
	if len(value)%9 != 0 {
		log.Error("Strange value", "ln", len(value), "k", common.Bytes2Hex(k))
		return nil
	}

	currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
	indexBytes, err1 := state.Get(currentChunkKey)
	if err1 != nil && err1 != ethdb.ErrKeyNotFound {
		return fmt.Errorf("find chunk failed: %w", err1)
	}

	currentIndex:=dbutils.WrapHistoryIndex(indexBytes)


	for i:=0;i<len(value); i+=9 {
		b:=binary.BigEndian.Uint64(value[i:])
		vzero := value[i+8]==1
		blockNr := b
		if err1 != nil && err1 != ethdb.ErrKeyNotFound {
			return fmt.Errorf("find chunk failed: %w", err1)
		}

		if dbutils.CheckNewIndexChunk(currentIndex, blockNr) {
			// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
			indexKey, err3 := currentIndex.Key(k)
			if err3 != nil {
				return err3
			}
			// Flush the old chunk
			if err4 := next(indexKey, currentIndex); err4 != nil {
				return err4
			}
			// Start a new chunk
			currentIndex = dbutils.NewHistoryIndex()
		}
		currentIndex = currentIndex.Append(blockNr, vzero)
	}
	err := next(currentChunkKey, currentIndex)
	if err != nil {
		return err
	}

	return nil
}

func getExtractFunc(bytes2walker func([]byte) changeset.Walker) etl.ExtractFunc { //nolint
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		bufferMap := make(map[string][][]byte)
		blockNum, _ := dbutils.DecodeTimestamp(dbKey)
		err := bytes2walker(dbValue).Walk(func(changesetKey, changesetValue []byte) error {
			sKey := string(changesetKey)
			list := bufferMap[sKey]
			b := blockNum
			v:=make([]byte, 9)
			binary.BigEndian.PutUint64(v,b)
			if len(changesetValue) == 0 {
				v[8] = 1
			}
			list = append(list, v)

			bufferMap[sKey] = list
			return nil
		})
		if err != nil {
			return err
		}

		for k, v := range bufferMap {
			for i:=range v {
				err = next(dbKey, []byte(k), v[i])
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}
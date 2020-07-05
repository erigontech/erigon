package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func NewIndexGenerator(db ethdb.Database, quitCh chan struct{}) *IndexGenerator {
	return &IndexGenerator{
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
		quitCh:           quitCh,
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

func (ig *IndexGenerator) GenerateIndex(startBlock, endBlock uint64, changeSetBucket []byte) error {
	v, ok := CSMapper[string(changeSetBucket)]
	if !ok {
		return errors.New("unknown bucket type")
	}
	log.Info("Index generation started", "from", startBlock, "to", endBlock, "csbucket", string(changeSetBucket))
	if endBlock < startBlock && endBlock != 0 {
		return fmt.Errorf("generateIndex %s: endBlock %d smaller than startBlock %d", changeSetBucket, endBlock, startBlock)
	}
	t := time.Now()
	err := etl.Transform(ig.db, changeSetBucket,
		v.IndexBucket,
		os.TempDir(),
		getExtractFunc(v.WalkerAdapter),
		loadFunc,
		etl.TransformArgs{
			ExtractStartKey: dbutils.EncodeTimestamp(startBlock),
			ExtractEndKey:   dbutils.EncodeTimestamp(endBlock),
			FixedBits:       0,
			BufferType:      etl.SortableAppendBuffer,
			BufferSize:      ig.ChangeSetBufSize,
			Quit:            ig.quitCh,
		},
	)
	if err != nil {
		return err
	}

	log.Info("Index generation successfully finished", "csbucket", string(changeSetBucket), "it took", time.Since(t))
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
			keys[string(common.CopyBytes(kk))] = struct{}{}
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
		copy(startKey[:keySize], dbutils.CompositeKeyWithoutIncarnation([]byte(key)))

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
		err := bolt.ClearBuckets(bucket)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("imposible to drop")
}

func loadFunc(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
	trace := bytes.HasPrefix(k, common.FromHex("0x6ffedc1562918c07ae49b0ba210e6d80c7d61eab"))
	if len(value)%9 != 0 {
		log.Error("Value must be a multiple of 9", "ln", len(value), "k", common.Bytes2Hex(k))
		return errors.New("incorrect value")
	}
	k = common.CopyBytes(k)
	if len(k) >= 28 {
		binary.BigEndian.PutUint64(k[common.AddressLength:], ^binary.BigEndian.Uint64(k[common.AddressLength:]))
	}
	currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
	indexBytes, err1 := state.Get(currentChunkKey)
	if err1 != nil && !errors.Is(err1, ethdb.ErrKeyNotFound) {
		return fmt.Errorf("find chunk failed: %w", err1)
	}
	if trace {
		fmt.Printf("Loading %x: %x (%d), replacing %x\n", k, value, binary.BigEndian.Uint64(value), indexBytes)
	}
	currentIndex := dbutils.WrapHistoryIndex(indexBytes)

	for i := 0; i < len(value); i += 9 {
		b := binary.BigEndian.Uint64(value[i:])
		vzero := value[i+8] == 1
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
			if err4 := next(k, indexKey, currentIndex); err4 != nil {
				return err4
			}
			// Start a new chunk
			currentIndex = dbutils.NewHistoryIndex()
		}
		currentIndex = currentIndex.Append(blockNr, vzero)
	}
	err := next(k, currentChunkKey, currentIndex)
	if err != nil {
		return err
	}

	return nil
}

func getExtractFunc(bytes2walker func([]byte) changeset.Walker) etl.ExtractFunc { //nolint
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		blockNum, _ := dbutils.DecodeTimestamp(dbKey)
		return bytes2walker(dbValue).Walk(func(changesetKey, changesetValue []byte) error {
			trace := bytes.HasPrefix(changesetKey, common.FromHex("0x6ffedc1562918c07ae49b0ba210e6d80c7d61eab"))
			if trace {
				fmt.Printf("Extracting %x for block %d\n", changesetKey, blockNum)
			}
			key := common.CopyBytes(changesetKey)
			if len(key) >= 28 {
				binary.BigEndian.PutUint64(key[common.AddressLength:], ^binary.BigEndian.Uint64(key[common.AddressLength:]))
			}
			v := make([]byte, 9)
			binary.BigEndian.PutUint64(v, blockNum)
			if len(changesetValue) == 0 {
				v[8] = 1
			}
			return next(dbKey, key, v)
		})
	}
}

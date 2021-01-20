package core

import (
	"context"
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

func NewIndexGenerator(logPrefix string, db ethdb.Database, quitCh <-chan struct{}) *IndexGenerator {
	return &IndexGenerator{
		logPrefix:        logPrefix,
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
		quitCh:           quitCh,
	}
}

type IndexGenerator struct {
	logPrefix        string
	db               ethdb.Database
	ChangeSetBufSize int
	TempDir          string
	quitCh           <-chan struct{}
}

func (ig *IndexGenerator) GenerateIndex(startBlock, endBlock uint64, changeSetBucket string, tmpdir string) error {
	v, ok := changeset.Mapper[changeSetBucket]
	if !ok {
		return errors.New("unknown bucket type")
	}
	log.Debug(fmt.Sprintf("[%s] Index generation", ig.logPrefix), "from", startBlock, "to", endBlock, "csbucket", changeSetBucket)
	if endBlock < startBlock && endBlock != 0 {
		return fmt.Errorf("generateIndex %s: endBlock %d smaller than startBlock %d", changeSetBucket, endBlock, startBlock)
	}
	t := time.Now()
	err := etl.Transform(ig.logPrefix, ig.db, changeSetBucket,
		v.IndexBucket,
		tmpdir,
		getExtractFunc(changeSetBucket),
		loadFunc,
		etl.TransformArgs{
			ExtractStartKey: dbutils.EncodeBlockNumber(startBlock),
			ExtractEndKey:   dbutils.EncodeBlockNumber(endBlock),
			FixedBits:       0,
			BufferType:      etl.SortableAppendBuffer,
			BufferSize:      ig.ChangeSetBufSize,
			Quit:            ig.quitCh,
			LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"block", binary.BigEndian.Uint64(k)}
			},
		},
	)
	if err != nil {
		return err
	}

	log.Debug(fmt.Sprintf("[%s] Index generation successfully finished", ig.logPrefix), "csbucket", changeSetBucket, "it took", time.Since(t))
	return nil
}

func (ig *IndexGenerator) Truncate(timestampTo uint64, changeSetBucket string) error {
	vv, ok := changeset.Mapper[changeSetBucket]
	if !ok {
		return errors.New("unknown bucket type")
	}

	currentKey := dbutils.EncodeBlockNumber(timestampTo)
	keys := make(map[string]struct{})
	if err := changeset.Walk(ig.db, changeSetBucket, currentKey, 0, func(blockN uint64, k, _ []byte) (bool, error) {
		if err := common.Stopped(ig.quitCh); err != nil {
			return false, err
		}
		keys[string(common.CopyBytes(k))] = struct{}{}
		return true, nil
	}); err != nil {
		return err
	}

	historyEffects := make(map[string][]byte)
	keySize := vv.KeySize
	if dbutils.StorageChangeSetBucket == changeSetBucket || dbutils.PlainStorageChangeSetBucket == changeSetBucket {
		keySize += common.HashLength
	}

	var startKey = make([]byte, keySize+8)

	for key := range keys {
		copy(startKey[:keySize], dbutils.CompositeKeyWithoutIncarnation([]byte(key)))

		binary.BigEndian.PutUint64(startKey[keySize:], timestampTo)
		if err := ig.db.Walk(vv.IndexBucket, startKey, 8*keySize, func(k, v []byte) (bool, error) {
			timestamp := binary.BigEndian.Uint64(k[keySize:]) // the last timestamp in the chunk
			if timestamp <= timestampTo {
				return true, nil
			}
			kStr := string(common.CopyBytes(k))

			// Truncated chunk becomes "the last chunk" with the timestamp 0xffff....ffff
			// - "last but one chunk" will become "the last chunk" only if it exists.
			// - and "the last chunk" need to be deleted only if "last but one chunk" was not converted to "the last"
			if _, ok := historyEffects[kStr]; !ok {
				historyEffects[kStr] = nil
			}
			// truncate the chunk
			index := dbutils.WrapHistoryIndex(v)
			index = index.TruncateGreater(timestampTo)
			if len(index) > 8 { // If the chunk is empty after truncation, it gets simply deleted
				lastK := make([]byte, len(k))
				copy(lastK, k[:keySize])
				binary.BigEndian.PutUint64(lastK[keySize:], ^uint64(0))
				historyEffects[string(lastK)] = common.CopyBytes(index)
			}
			return true, nil
		}); err != nil {
			return err
		}
	}
	mutation := ig.db.NewBatch()
	defer mutation.Rollback()

	for key, value := range historyEffects {
		if value == nil {
			if err := mutation.Delete(vv.IndexBucket, []byte(key), nil); err != nil {
				return err
			}
		} else {
			if err := mutation.Put(vv.IndexBucket, []byte(key), value); err != nil {
				return err
			}
		}
		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if err := mutation.CommitAndBegin(context.Background()); err != nil {
				return err
			}
		}
	}
	_, err := mutation.Commit()
	return err
}

func (ig *IndexGenerator) DropIndex(bucket string) error {
	casted, ok := ig.db.(ethdb.BucketsMigrator)
	if !ok {
		return errors.New("imposible to drop")
	}
	log.Warn(fmt.Sprintf("[%s] Remove bucket", ig.logPrefix), "bucket", bucket)
	return casted.ClearBuckets(bucket)
}

func loadFunc(k []byte, value []byte, state etl.CurrentTableReader, next etl.LoadNextFunc) error {
	if len(value)%9 != 0 {
		log.Error("Value must be a multiple of 9", "ln", len(value), "k", common.Bytes2Hex(k))
		return errors.New("incorrect value")
	}
	k = common.CopyBytes(k)
	currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
	indexBytes, err1 := state.Get(currentChunkKey)
	if err1 != nil && !errors.Is(err1, ethdb.ErrKeyNotFound) {
		return fmt.Errorf("find chunk failed: %w", err1)
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

	if err := next(k, currentChunkKey, currentIndex); err != nil {
		return err
	}

	return nil
}

func getExtractFunc(changeSetBucket string) etl.ExtractFunc { //nolint
	decode := changeset.Mapper[changeSetBucket].Decode
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		blockNum, k, v := decode(dbKey, dbValue)

		newV := make([]byte, 9)
		binary.BigEndian.PutUint64(newV, blockNum)
		if len(v) == 0 {
			newV[8] = 1
		}
		return next(dbKey, k, newV)
	}
}

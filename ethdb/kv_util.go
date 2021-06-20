package ethdb

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/log"
)

func Get(tx KVGetter, bucket string, key []byte) ([]byte, error) {
	v, err := tx.GetOne(bucket, key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

func ForEach(c Cursor, walker func(k, v []byte) (bool, error)) error {
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		ok, err := walker(k, v)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return nil
}

func Walk(c Cursor, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	k, v, err := c.Seek(startkey)
	if err != nil {
		return err
	}
	for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
		goOn, err := walker(k, v)
		if err != nil {
			return err
		}
		if !goOn {
			break
		}
		k, v, err = c.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func MultiPut(tx RwTx, tuples ...[]byte) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	count := 0
	total := float64(len(tuples)) / 3
	for bucketStart := 0; bucketStart < len(tuples); {
		bucketEnd := bucketStart
		for ; bucketEnd < len(tuples) && bytes.Equal(tuples[bucketEnd], tuples[bucketStart]); bucketEnd += 3 {
		}
		bucketName := string(tuples[bucketStart])
		c, err := tx.RwCursor(bucketName)
		if err != nil {
			return err
		}

		// move cursor to a first element in batch
		// if it's nil, it means all keys in batch gonna be inserted after end of bucket (batch is sorted and has no duplicates here)
		// can apply optimisations for this case
		firstKey, _, err := c.Seek(tuples[bucketStart+1])
		if err != nil {
			return err
		}
		isEndOfBucket := firstKey == nil

		l := (bucketEnd - bucketStart) / 3
		for i := 0; i < l; i++ {
			k := tuples[bucketStart+3*i+1]
			v := tuples[bucketStart+3*i+2]
			if isEndOfBucket {
				if v == nil {
					// nothing to delete after end of bucket
				} else {
					if err := c.Append(k, v); err != nil {
						return err
					}
				}
			} else {
				if v == nil {
					if err := c.Delete(k, nil); err != nil {
						return err
					}
				} else {
					if err := c.Put(k, v); err != nil {
						return err
					}
				}
			}

			count++

			select {
			default:
			case <-logEvery.C:
				progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, total/1_000_000)
				log.Info("Write to db", "progress", progress, "current table", bucketName)
			}
		}

		bucketStart = bucketEnd
	}
	return nil
}

// todo: return TEVM code and use it
func GetCheckTEVM(db KVGetter) func(contractHash common.Hash) (bool, error) {
	checked := map[common.Hash]struct{}{}
	var ok bool

	return func(contractHash common.Hash) (bool, error) {
		if contractHash == (common.Hash{}) {
			return true, nil
		}

		if _, ok = checked[contractHash]; ok {
			return true, nil
		}

		ok, err := db.Has(dbutils.ContractTEVMCodeBucket, contractHash.Bytes())
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return false, fmt.Errorf("can't check TEVM bucket by contract %q hash: %w",
				contractHash.String(), err)
		}

		if !ok {
			checked[contractHash] = struct{}{}
		}

		return ok, nil
	}
}

func Bytesmask(fixedbits int) (fixedbytes int, mask byte) {
	fixedbytes = (fixedbits + 7) / 8
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}

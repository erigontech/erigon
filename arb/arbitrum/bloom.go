package arbitrum

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/bitutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/bloombits"
	"github.com/erigontech/erigon/erigon-db/rawdb"
)

const (
	// bloomServiceThreads is the number of goroutines used globally by an Ethereum
	// instance to service bloombits lookups for all running filters.
	bloomServiceThreads = 16

	// bloomFilterThreads is the number of goroutines used locally per filter to
	// multiplex requests onto the global servicing goroutines.
	bloomFilterThreads = 3

	// bloomRetrievalBatch is the maximum number of bloom bit retrievals to service
	// in a single batch.
	bloomRetrievalBatch = 16

	// bloomRetrievalWait is the maximum time to wait for enough bloom bit requests
	// to accumulate request an entire batch (avoiding hysteresis).
	bloomRetrievalWait = time.Duration(0)
)

// startBloomHandlers starts a batch of goroutines to accept bloom bit database
// retrievals from possibly a range of filters and serving the data to satisfy.
func (b *Backend) startBloomHandlers(sectionSize uint64) {
	for i := 0; i < bloomServiceThreads; i++ {
		go func() {
			for {
				select {
				case _, more := <-b.chanClose:
					if !more {
						return
					}
				case request := <-b.bloomRequests:
					task := <-request
					ServeBloombitRetrieval(task, b.chainDb, sectionSize)
					request <- task
				}
			}
		}()
	}
}

func ServeBloombitRetrieval(task *bloombits.Retrieval, chainDb kv.TemporalRwDB, sectionSize uint64) {
	task.Bitsets = make([][]byte, len(task.Sections))
	tx, err := chainDb.BeginRo(context.Background())
	if err != nil {
		task.Error = err
		return
	}
	defer tx.Rollback()

	for i, section := range task.Sections {
		head, err := rawdb.ReadCanonicalHash(tx, (section+1)*sectionSize-1)
		if err != nil {
			task.Error = err
			return
		}
		if compVector, err := ReadBloomBits(tx, task.Bit, section, head); err == nil {
			if blob, err := bitutil.DecompressBytes(compVector, int(sectionSize/8)); err == nil {
				task.Bitsets[i] = blob
			} else {
				task.Error = err
			}
		} else {
			task.Error = err
		}
	}
}

// ReadBloomBits retrieves the compressed bloom bit vector belonging to the given
// section and bit index from the.
func ReadBloomBits(tx kv.Tx, bit uint, section uint64, head common.Hash) ([]byte, error) {
	return tx.GetOne(ArbBloomBitsBucket, bloomBitsKey(bit, section, head))
}

// WriteBloomBits stores the compressed bloom bits vector belonging to the given
// section and bit index.
func WriteBloomBits(db kv.RwDB, bit uint, section uint64, head common.Hash, bits []byte) {
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(ArbBloomBitsBucket, bloomBitsKey(bit, section, head), bits)
	})
	if err != nil {
		log.Crit("Failed to store bloom bits", "err", err)
	}
}

var bloomBitsPrefix = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits

const (
	ArbBloomBitsBucket = "arb_bloomBits"
)

// DeleteBloombits removes all compressed bloom bits vector belonging to the
// given section range and bit index.
func DeleteBloombits(db kv.RwDB, bit uint, from uint64, to uint64) {
	start, end := bloomBitsKey(bit, from, common.Hash{}), bloomBitsKey(bit, to, common.Hash{})

	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		it, err := tx.RwCursor(ArbBloomBitsBucket)
		if err != nil {
			return err
		}
		defer it.Close()

		for k, _, err := it.Seek(start); err == nil && k != nil; k, _, err = it.Next() {
			if bytes.Compare(k, end) >= 0 {
				break
			}
			if len(k) != len(bloomBitsPrefix)+2+8+32 {
				continue
			}
			if err = tx.Delete(ArbBloomBitsBucket, k); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		log.Crit("Failed to delete bloom bits", "err", err)
	}
}

func bloomBitsKey(bit uint, section uint64, hash common.Hash) []byte {
	key := append(append(bloomBitsPrefix, make([]byte, 10)...), hash.Bytes()...)

	binary.BigEndian.PutUint16(key[1:], uint16(bit))
	binary.BigEndian.PutUint64(key[3:], section)

	return key
}

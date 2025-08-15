package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"strconv"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/trie"
)

type WitnessDBWriter struct {
	storage     kv.RwDB
	statsWriter *csv.Writer
}

func NewWitnessDBWriter(storage kv.RwDB, statsWriter *csv.Writer) (*WitnessDBWriter, error) {
	err := statsWriter.Write([]string{
		"blockNum", "maxTrieSize", "witnessesSize",
	})
	if err != nil {
		return nil, err
	}
	return &WitnessDBWriter{storage, statsWriter}, nil
}

const chunkSize = 100000 // 100KB

func WriteChunks(tx kv.RwTx, tableName string, key []byte, valueBytes []byte) error {
	// Split the valueBytes into chunks and write each chunk
	for i := 0; i < len(valueBytes); i += chunkSize {
		end := i + chunkSize
		if end > len(valueBytes) {
			end = len(valueBytes)
		}
		chunk := valueBytes[i:end]
		chunkKey := append(key, []byte("_chunk_"+strconv.Itoa(i/chunkSize))...)

		// Write each chunk to the KV store
		if err := tx.Put(tableName, chunkKey, chunk); err != nil {
			return err
		}
	}

	return nil
}

func ReadChunks(tx kv.Tx, tableName string, key []byte) ([]byte, error) {
	// Initialize a buffer to store the concatenated chunks
	var result []byte

	// Retrieve and concatenate each chunk
	for i := 0; ; i++ {
		chunkKey := append(key, []byte("_chunk_"+strconv.Itoa(i))...)
		chunk, err := tx.GetOne(tableName, chunkKey)
		if err != nil {
			return nil, err
		}

		// Check if this is the last chunk
		if len(chunk) == 0 {
			break
		}

		// Append the chunk to the result
		result = append(result, chunk...)
	}

	return result, nil
}

// HasWitness returns whether a witness exists for the given key or not
func HasWitness(tx kv.Tx, tableName string, key []byte) (bool, error) {
	firstChunkKey := append(key, []byte("_chunk_0")...)
	chunk, err := tx.GetOne(tableName, firstChunkKey)
	if err != nil {
		return false, err
	}

	if len(chunk) == 0 {
		return false, nil
	}

	return true, nil
}

// DeleteChunks deletes all the chunks present with prefix `key`
// TODO: Try to see if this can be optimised by using tx.ForEach
// and iterate over each element with prefix `key`
func DeleteChunks(tx kv.RwTx, tableName string, key []byte) error {
	for i := 0; ; i++ {
		chunkKey := append(key, []byte("_chunk_"+strconv.Itoa(i))...)
		chunk, err := tx.GetOne(tableName, chunkKey)
		if err != nil {
			return err
		}

		err = tx.Delete(tableName, chunkKey)
		if err != nil {
			return err
		}

		// Check if this is the last chunk
		if len(chunk) == 0 {
			break
		}
	}

	return nil
}

// FindOldestWitness returns the block number of the oldest stored block
func FindOldestWitness(tx kv.Tx, tableName string) (uint64, error) {
	cursor, err := tx.Cursor(tableName)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()

	k, _, err := cursor.First()
	if err != nil {
		return 0, err
	}

	return BytesToUint64(k), nil
}

func (db *WitnessDBWriter) MustUpsertOneWitness(blockNumber uint64, witness *trie.Witness) {
	k := make([]byte, 8)

	binary.LittleEndian.PutUint64(k, blockNumber)

	var buf bytes.Buffer
	_, err := witness.WriteInto(&buf)
	if err != nil {
		panic(fmt.Sprintf("error extracting witness for block %d: %v\n", blockNumber, err))
	}

	wb := buf.Bytes()

	tx, err := db.storage.BeginRw(context.Background())
	if err != nil {
		panic(fmt.Errorf("error opening tx: %w", err))
	}

	defer tx.Rollback()

	fmt.Printf("Size of witness: %d\n", len(wb))

	err = WriteChunks(tx, kv.Witnesses, k, common.CopyBytes(wb))

	tx.Commit()

	if err != nil {
		panic(fmt.Errorf("error while upserting witness: %w", err))
	}
}

func (db *WitnessDBWriter) MustUpsert(blockNumber uint64, maxTrieSize uint32, resolveWitnesses []*trie.Witness) {
	key := deriveDbKey(blockNumber, maxTrieSize)

	var buf bytes.Buffer

	for i, witness := range resolveWitnesses {
		if _, err := witness.WriteInto(&buf); err != nil {
			panic(fmt.Errorf("error while writing witness to a buffer: %w", err))
		}
		if i < len(resolveWitnesses)-1 {
			buf.WriteByte(byte(trie.OpNewTrie))
		}
	}

	bytes := buf.Bytes()

	tx, err := db.storage.BeginRw(context.Background())
	if err != nil {
		panic(fmt.Errorf("error opening tx: %w", err))
	}

	defer tx.Rollback()

	err = tx.Put(kv.Witnesses, common.CopyBytes(key), common.CopyBytes(bytes))

	tx.Commit()

	if err != nil {
		panic(fmt.Errorf("error while upserting witness: %w", err))
	}

	err = db.statsWriter.Write([]string{
		strconv.Itoa(int(blockNumber)),
		strconv.Itoa(int(maxTrieSize)),
		strconv.Itoa(len(bytes)),
	})

	if err != nil {
		panic(fmt.Errorf("error while writing stats: %w", err))
	}

	db.statsWriter.Flush()
}

type WitnessDBReader struct {
	db kv.RwDB
}

func NewWitnessDBReader(db kv.RwDB) *WitnessDBReader {
	return &WitnessDBReader{db}
}

func (db *WitnessDBReader) GetWitnessesForBlock(blockNumber uint64, maxTrieSize uint32) ([]byte, error) {
	key := deriveDbKey(blockNumber, maxTrieSize)

	tx, err := db.db.BeginRo(context.Background())
	if err != nil {
		panic(fmt.Errorf("error opening tx: %w", err))
	}

	defer tx.Rollback()

	return tx.GetOne(kv.Witnesses, key)
}

func deriveDbKey(blockNumber uint64, maxTrieSize uint32) []byte {
	buffer := make([]byte, 8+4)

	binary.LittleEndian.PutUint64(buffer, blockNumber)
	binary.LittleEndian.PutUint32(buffer[8:], maxTrieSize)

	return buffer
}

func BytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func Uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

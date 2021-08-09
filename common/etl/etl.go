package etl

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/log/v3"
	"github.com/ugorji/go/codec"
)

var (
	cbor codec.CborHandle
)

type Decoder interface {
	Reset(reader io.Reader)
	Decode(interface{}) error
}

type CurrentTableReader interface {
	Get([]byte) ([]byte, error)
}

type ExtractNextFunc func(originalK, k []byte, v []byte) error
type ExtractFunc func(k []byte, v []byte, next ExtractNextFunc) error

// NextKey generates the possible next key w/o changing the key length.
// for [0x01, 0x01, 0x01] it will generate [0x01, 0x01, 0x02], etc
func NextKey(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return key, fmt.Errorf("could not apply NextKey for the empty key")
	}
	nextKey := common.CopyBytes(key)
	for i := len(key) - 1; i >= 0; i-- {
		b := nextKey[i]
		if b < 0xFF {
			nextKey[i] = b + 1
			return nextKey, nil
		}
		if b == 0xFF {
			nextKey[i] = 0
		}
	}
	return key, fmt.Errorf("overflow while applying NextKey")
}

// LoadCommitHandler is a callback called each time a new batch is being
// loaded from files into a DB
// * `key`: last commited key to the database (use etl.NextKey helper to use in LoadStartKey)
// * `isDone`: true, if everything is processed
type LoadCommitHandler func(db kv.Putter, key []byte, isDone bool) error
type AdditionalLogArguments func(k, v []byte) (additionalLogArguments []interface{})

type TransformArgs struct {
	ExtractStartKey []byte
	ExtractEndKey   []byte
	FixedBits       int
	BufferType      int
	BufferSize      int
	Quit            <-chan struct{}

	LogDetailsExtract AdditionalLogArguments
	LogDetailsLoad    AdditionalLogArguments

	Comparator kv.CmpFunc
}

func Transform(
	logPrefix string,
	db kv.RwTx,
	fromBucket string,
	toBucket string,
	tmpdir string,
	extractFunc ExtractFunc,
	loadFunc LoadFunc,
	args TransformArgs,
) error {
	bufferSize := BufferOptimalSize
	if args.BufferSize > 0 {
		bufferSize = datasize.ByteSize(args.BufferSize)
	}
	buffer := getBufferByType(args.BufferType, bufferSize)
	collector := NewCollector(tmpdir, buffer)
	defer collector.Close(logPrefix)

	t := time.Now()
	if err := extractBucketIntoFiles(logPrefix, db, fromBucket, args.ExtractStartKey, args.ExtractEndKey, args.FixedBits, collector, extractFunc, args.Quit, args.LogDetailsExtract); err != nil {
		return err
	}
	log.Debug(fmt.Sprintf("[%s] Extraction finished", logPrefix), "it took", time.Since(t))

	defer func(t time.Time) {
		log.Debug(fmt.Sprintf("[%s] Collection finished", logPrefix), "it took", time.Since(t))
	}(time.Now())
	return collector.Load(logPrefix, db, toBucket, loadFunc, args)
}

func extractBucketIntoFiles(
	logPrefix string,
	db kv.Tx,
	bucket string,
	startkey []byte,
	endkey []byte,
	fixedBits int,
	collector *Collector,
	extractFunc ExtractFunc,
	quit <-chan struct{},
	additionalLogArguments AdditionalLogArguments,
) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	var m runtime.MemStats

	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := ethdb.Walk(c, startkey, fixedBits, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quit); err != nil {
			return false, err
		}

		select {
		default:
		case <-logEvery.C:
			logArs := []interface{}{"from", bucket}
			if additionalLogArguments != nil {
				logArs = append(logArs, additionalLogArguments(k, v)...)
			} else {
				logArs = append(logArs, "current key", makeCurrentKeyStr(k))
			}

			runtime.ReadMemStats(&m)
			logArs = append(logArs, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
			log.Info(fmt.Sprintf("[%s] ETL [1/2] Extracting", logPrefix), logArs...)
		}
		if endkey != nil && bytes.Compare(k, endkey) > 0 {
			return false, nil
		}
		if err := extractFunc(k, v, collector.extractNextFunc); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}
	return collector.flushBuffer(nil, true)
}

type currentTableReader struct {
	getter kv.Tx
	bucket string
}

func (s *currentTableReader) Get(key []byte) ([]byte, error) {
	return s.getter.GetOne(s.bucket, key)
}

// IdentityLoadFunc loads entries as they are, without transformation
var IdentityLoadFunc LoadFunc = func(k []byte, value []byte, _ CurrentTableReader, next LoadNextFunc) error {
	return next(k, k, value)
}

func isIdentityLoadFunc(f LoadFunc) bool {
	return f == nil || reflect.ValueOf(IdentityLoadFunc).Pointer() == reflect.ValueOf(f).Pointer()
}

package etl

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ugorji/go/codec"
)

var (
	cbor codec.CborHandle
)

type Decoder interface {
	Reset(reader io.Reader)
	Decode(interface{}) error
}

type State interface {
	Get([]byte) ([]byte, error)
	Stopped() error
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
type LoadCommitHandler func(db ethdb.Putter, key []byte, isDone bool) error

type TransformArgs struct {
	ExtractStartKey []byte
	ExtractEndKey   []byte
	FixedBits       int
	BufferType      int
	BufferSize      int
	Quit            <-chan struct{}
	OnLoadCommit    LoadCommitHandler
	loadBatchSize   int // used in testing
}

func Transform(
	db ethdb.Database,
	fromBucket []byte,
	toBucket []byte,
	datadir string,
	extractFunc ExtractFunc,
	loadFunc LoadFunc,
	args TransformArgs,
) error {
	bufferSize := BufferOptimalSize
	if args.BufferSize > 0 {
		bufferSize = args.BufferSize
	}
	buffer := getBufferByType(args.BufferType, bufferSize)
	collector := NewCollector(datadir, buffer)

	t := time.Now()
	if err := extractBucketIntoFiles(db, fromBucket, args.ExtractStartKey, args.ExtractEndKey, args.FixedBits, collector, extractFunc, args.Quit); err != nil {
		disposeProviders(collector.dataProviders)
		return err
	}
	log.Debug("Extraction finished", "it took", time.Since(t))

	defer func(t time.Time) { log.Debug("Collection finished", "it took", time.Since(t)) }(time.Now())
	return collector.Load(db, toBucket, loadFunc, args)
}

func extractBucketIntoFiles(
	db ethdb.Database,
	bucket []byte,
	startkey []byte,
	endkey []byte,
	fixedBits int,
	collector *Collector,
	extractFunc ExtractFunc,
	quit <-chan struct{},
) error {
	if err := db.Walk(bucket, startkey, fixedBits, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quit); err != nil {
			return false, err
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
func disposeProviders(providers []dataProvider) {
	for _, p := range providers {
		err := p.Dispose()
		if err != nil {
			log.Warn("promoting hashed state, error while disposing provider", "provier", p, "err", err)
		}
	}
}

type bucketState struct {
	getter ethdb.Getter
	bucket []byte
	quit   <-chan struct{}
}

func (s *bucketState) Get(key []byte) ([]byte, error) {
	return s.getter.Get(s.bucket, key)
}

func (s *bucketState) Stopped() error {
	return common.Stopped(s.quit)
}

// IdentityLoadFunc loads entries as they are, without transformation
func IdentityLoadFunc(k []byte, value []byte, _ State, next LoadNextFunc) error {
	return next(k, k, value)
}

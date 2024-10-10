/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package etl

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

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
	nextKey := common.Copy(key)
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
	Quit              <-chan struct{}
	LogDetailsExtract AdditionalLogArguments
	LogDetailsLoad    AdditionalLogArguments
	// [ExtractStartKey, ExtractEndKey)
	ExtractStartKey []byte
	ExtractEndKey   []byte
	BufferType      int
	BufferSize      int
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
	logger log.Logger,
) error {
	bufferSize := BufferOptimalSize
	if args.BufferSize > 0 {
		bufferSize = datasize.ByteSize(args.BufferSize)
	}
	buffer := getBufferByType(args.BufferType, bufferSize, nil)
	collector := NewCollector(logPrefix, tmpdir, buffer, logger)
	defer collector.Close()

	t := time.Now()
	if err := extractBucketIntoFiles(logPrefix, db, fromBucket, args.ExtractStartKey, args.ExtractEndKey, collector, extractFunc, args.Quit, args.LogDetailsExtract, logger); err != nil {
		return err
	}
	logger.Trace(fmt.Sprintf("[%s] Extraction finished", logPrefix), "took", time.Since(t))

	defer func(t time.Time) {
		logger.Trace(fmt.Sprintf("[%s] Load finished", logPrefix), "took", time.Since(t))
	}(time.Now())
	return collector.Load(db, toBucket, loadFunc, args)
}

// extractBucketIntoFiles - [startkey, endkey)
func extractBucketIntoFiles(
	logPrefix string,
	db kv.Tx,
	bucket string,
	startkey []byte,
	endkey []byte,
	collector *Collector,
	extractFunc ExtractFunc,
	quit <-chan struct{},
	additionalLogArguments AdditionalLogArguments,
	logger log.Logger,
) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, v, e := c.Seek(startkey); k != nil; k, v, e = c.Next() {
		if e != nil {
			return e
		}
		if err := common.Stopped(quit); err != nil {
			return err
		}
		select {
		default:
		case <-logEvery.C:
			logArs := []interface{}{"from", bucket}
			if additionalLogArguments != nil {
				logArs = append(logArs, additionalLogArguments(k, v)...)
			} else {
				logArs = append(logArs, "current_prefix", makeCurrentKeyStr(k))
			}

			logger.Info(fmt.Sprintf("[%s] ETL [1/2] Extracting", logPrefix), logArs...)
		}
		if endkey != nil && bytes.Compare(k, endkey) >= 0 {
			// endKey is exclusive bound: [startkey, endkey)
			return nil
		}
		if err := extractFunc(k, v, collector.extractNextFunc); err != nil {
			return err
		}
	}
	return collector.flushBuffer(true)
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

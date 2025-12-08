// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package etl

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
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
		return key, errors.New("could not apply NextKey for the empty key")
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
	return key, errors.New("overflow while applying NextKey")
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
	EmptyVals       bool // `v=nil` case: `false` means `Del(k)`, `true` means `Put(k, nil)`
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
	buffer := getBufferByType(args.BufferType, bufferSize)
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

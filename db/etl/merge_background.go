// Copyright 2026 The Erigon Authors
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
	"sync"

	"github.com/erigontech/erigon/common"
)

const (
	mergeBatchEntries = 4096
	mergeBatchBytes   = 4 * 1024 * 1024
	mergeBatchRing    = 4
)

type mergeBatchEntry struct {
	keyOffset   int
	keyLength   int
	valueOffset int
	valueLength int
	keyNil      bool
	valueNil    bool
}

type mergeBatch struct {
	entries []mergeBatchEntry
	arena   []byte
}

func newMergeBatch(entries, arena int) *mergeBatch {
	return &mergeBatch{entries: make([]mergeBatchEntry, 0, entries), arena: make([]byte, 0, arena)}
}

func (b *mergeBatch) full(key, value []byte) bool {
	return len(b.entries) >= mergeBatchEntries || len(b.entries) > 0 && len(b.arena)+len(key)+len(value) > mergeBatchBytes
}

func (b *mergeBatch) add(key, value []byte) {
	entry := mergeBatchEntry{
		keyOffset:   len(b.arena),
		keyLength:   len(key),
		valueOffset: len(b.arena) + len(key),
		valueLength: len(value),
		keyNil:      key == nil,
		valueNil:    value == nil,
	}
	b.arena = append(b.arena, key...)
	b.arena = append(b.arena, value...)
	b.entries = append(b.entries, entry)
}

func (b *mergeBatch) row(index int) ([]byte, []byte) {
	entry := b.entries[index]
	var key, value []byte
	if !entry.keyNil {
		key = b.arena[entry.keyOffset : entry.keyOffset+entry.keyLength]
	}
	if !entry.valueNil {
		value = b.arena[entry.valueOffset : entry.valueOffset+entry.valueLength]
	}
	return key, value
}

func mergeSortFilesInBackground(logPrefix string, providers []dataProvider, loadFunc simpleLoadFunc, args TransformArgs) error {
	free := make(chan *mergeBatch, mergeBatchRing)
	ready := make(chan *mergeBatch, mergeBatchRing)
	for range mergeBatchRing {
		free <- newMergeBatch(0, 0)
	}

	mergeQuit := make(chan struct{})
	producerStop := make(chan struct{})
	producerDone := make(chan struct{})
	producerResult := make(chan error, 1)
	var stopMerge sync.Once
	var stopProducer sync.Once
	stop := func() {
		stopProducer.Do(func() { close(producerStop) })
		stopMerge.Do(func() { close(mergeQuit) })
	}

	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-args.Quit:
			stopMerge.Do(func() { close(mergeQuit) })
		case <-producerDone:
		}
	}()

	go func() {
		defer close(producerDone)
		take := func() (*mergeBatch, error) {
			select {
			case batch := <-free:
				return batch, nil
			case <-producerStop:
				return nil, common.ErrStopped
			}
		}
		current, err := take()
		if err != nil {
			producerResult <- err
			close(ready)
			return
		}
		publish := func(batch *mergeBatch) error {
			select {
			case ready <- batch:
				return nil
			case <-producerStop:
				return common.ErrStopped
			}
		}
		emit := func(key, value []byte) error {
			select {
			case <-producerStop:
				return common.ErrStopped
			default:
			}
			if current.full(key, value) {
				if publishErr := publish(current); publishErr != nil {
					return publishErr
				}
				next, takeErr := take()
				if takeErr != nil {
					return takeErr
				}
				current = next
			}
			current.add(key, value)
			return nil
		}
		mergeArgs := args
		mergeArgs.Quit = mergeQuit
		err = mergeSortFiles(logPrefix, providers, emit, mergeArgs)
		if len(current.entries) != 0 {
			publishErr := publish(current)
			if err == nil {
				err = publishErr
			}
		}
		producerResult <- err
		close(ready)
	}()

	var consumed uint64
	for batch := range ready {
		for i := range batch.entries {
			consumed++
			if consumed%1024 == 0 {
				if err := common.Stopped(args.Quit); err != nil {
					stop()
					producerErr := <-producerResult
					<-watchDone
					_ = producerErr
					return err
				}
			}
			key, value := batch.row(i)
			if err := loadFunc(key, value); err != nil {
				stop()
				producerErr := <-producerResult
				<-watchDone
				_ = producerErr
				return err
			}
		}
		free <- newMergeBatch(len(batch.entries), len(batch.arena))
	}
	producerErr := <-producerResult
	<-watchDone
	if producerErr != nil {
		return producerErr
	}
	return common.Stopped(args.Quit)
}

// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for
// more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v4

import (
	"bytes"
	"errors"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

var errPhaseAUpdate = errors.New("commitment v4: invalid phase A update")

type storageOp uint8

const (
	storageSkip storageOp = iota
	storagePut
	storageDelete
)

type storageEntry struct {
	path  []byte
	value []byte
	op    storageOp
}

func storageEntryOf(path []byte, update *commitment.Update) (storageEntry, error) {
	switch {
	case update == nil || update.Flags == 0:
		return storageEntry{path: path}, nil
	case update.Deleted():
		return storageEntry{path: path, op: storageDelete}, nil
	case update.Flags&commitment.StorageUpdate == 0 || update.StorageLen < 0:
		return storageEntry{}, errPhaseAUpdate
	}
	return storageEntry{path: path, value: update.Storage[:update.StorageLen], op: storagePut}, nil
}

type storageTask struct {
	addrHash [32]byte
	entries  []storageEntry
	wipe     bool
}

type accountEntry struct {
	hashedKey    []byte
	update       *commitment.Update
	storageDirty bool
}

type partitioner struct {
	storage  []storageTask
	accounts []accountEntry
}

func (p *partitioner) add(hashedKey []byte, update *commitment.Update) error {
	if len(hashedKey) != 64 && len(hashedKey) != 128 {
		return nil
	}

	accountHash := hashedKey[:64:64]
	if len(p.accounts) == 0 || !bytes.Equal(p.accounts[len(p.accounts)-1].hashedKey, accountHash) {
		p.accounts = append(p.accounts, accountEntry{hashedKey: accountHash})
	}
	current := &p.accounts[len(p.accounts)-1]

	if len(hashedKey) == 64 {
		current.update = update
		return nil
	}

	if !current.storageDirty {
		p.storage = append(p.storage, storageTask{addrHash: hashAddressPath(current.hashedKey)})
		current.storageDirty = true
	}
	entry, err := storageEntryOf(hashedKey[64:128:128], update)
	if err != nil {
		return err
	}
	task := &p.storage[len(p.storage)-1]
	task.entries = append(task.entries, entry)
	return nil
}

func (p *partitioner) done() (storage []storageTask, accounts []accountEntry) {
	storage, accounts = p.storage, p.accounts
	for i := range accounts {
		if accounts[i].storageDirty || accounts[i].update == nil || !accounts[i].update.Deleted() {
			continue
		}
		storage = append(storage, storageTask{addrHash: hashAddressPath(accounts[i].hashedKey), wipe: true})
	}
	return storage, accounts
}

func hashAddressPath(path []byte) [32]byte {
	var addrHash [32]byte
	packPath(path, addrHash[:0])
	return addrHash
}

const defaultStorageFanOutMin = 1024

func runStorageTaskWithPlan(ctx commitment.PatriciaContext, task storageTask, plan foldPlan) ([32]byte, deltaParts, error) {
	if ctx == nil {
		return [32]byte{}, nil, errors.New("commitment v4: nil phase A context")
	}
	if task.wipe {
		records, err := enumerateStorageRecords(ctx, task.addrHash)
		if err != nil || len(records) == 0 {
			return empty.RootHash, nil, err
		}
		return empty.RootHash, deltaParts{records}, nil
	}

	g := graph{plane: planeStorage, addrHash: task.addrHash[:]}
	root, err := g.loadRoot(ctx)
	if err != nil {
		return [32]byte{}, nil, err
	}

	fanned := false
	if len(task.entries) >= plan.fanOutMin {
		fanned, err = g.fanOutRoot(ctx, root, len(task.entries), func(i int) byte { return task.entries[i].path[0] }, plan, func(ctx commitment.PatriciaContext, i int) error {
			if task.entries[i].op == storageSkip {
				return nil
			}
			return g.ensurePath(ctx, root, task.entries[i].path)
		})
		if err != nil {
			return [32]byte{}, nil, err
		}
	}
	if !fanned {
		plan = foldPlan{}
	}

	for _, entry := range task.entries {
		if entry.op == storageSkip {
			continue
		}
		if !fanned && bytes.HasPrefix(entry.path, root.path) {
			if err := g.ensurePath(ctx, root, entry.path); err != nil {
				return [32]byte{}, nil, err
			}
		}
		if entry.op == storageDelete {
			if err := removeRoot(root, entry.path); err != nil {
				if errors.Is(err, ErrRemoveNotFound) {
					continue
				}
				return [32]byte{}, nil, err
			}
			continue
		}
		if err := insertRoot(root, entry.path, entry.value); err != nil {
			return [32]byte{}, nil, err
		}
	}

	parts, err := g.persistGraph(ctx, root, plan)
	if err != nil {
		return [32]byte{}, nil, err
	}
	hash, err := fold(root, 0)
	return hash, parts, err
}

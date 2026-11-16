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

package v3

import (
	"bytes"
	"errors"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

var errPhaseAUpdate = errors.New("commitment v3: invalid phase A update")

type storageEntry struct {
	path    []byte
	value   []byte
	deleted bool
}

func storageEntryOf(path []byte, update *commitment.Update) (storageEntry, error) {
	switch {
	case update.Deleted():
		return storageEntry{path: path, deleted: true}, nil
	case update.Flags&commitment.StorageUpdate == 0 || update.StorageLen < 0 || int(update.StorageLen) > len(update.Storage):
		return storageEntry{}, errPhaseAUpdate
	}
	return storageEntry{path: path, value: update.Storage[:update.StorageLen]}, nil
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
	next := 0
	for i := range accounts {
		task := -1
		if accounts[i].storageDirty {
			task, next = next, next+1
		}
		update := accounts[i].update
		switch {
		case update == nil:
		case update.Deleted():
			wipe := storageTask{addrHash: hashAddressPath(accounts[i].hashedKey), wipe: true}
			if task >= 0 {
				storage[task] = wipe
				continue
			}
			storage = append(storage, wipe)
		case update.DeleteStorageSubtree:
			if task >= 0 {
				storage[task].wipe = true
				continue
			}
			storage = append(storage, storageTask{addrHash: hashAddressPath(accounts[i].hashedKey), wipe: true})
			accounts[i].storageDirty = true
		}
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
		return [32]byte{}, nil, errors.New("commitment v3: nil phase A context")
	}
	g := graph{plane: planeStorage, addrHash: task.addrHash[:]}
	if task.wipe {
		records, err := enumerateStorageRecords(ctx, task.addrHash)
		if err != nil {
			return [32]byte{}, nil, err
		}
		if len(task.entries) != 0 {
			return g.rebuildStorage(ctx, task.entries, records)
		}
		if len(records) == 0 {
			return empty.RootHash, nil, nil
		}
		return empty.RootHash, deltaParts{records}, nil
	}

	root, err := g.loadRoot(ctx)
	if err != nil {
		return [32]byte{}, nil, err
	}

	fanned := false
	if len(task.entries) >= plan.fanOutMin {
		fanned, err = g.fanOutRoot(ctx, root, len(task.entries), func(i int) byte { return task.entries[i].path[0] }, plan, func(ctx commitment.PatriciaContext, i int) error {
			return g.ensurePath(ctx, root, task.entries[i].path)
		}, nil, nil)
		if err != nil {
			return [32]byte{}, nil, err
		}
	}
	if !fanned {
		plan = foldPlan{}
	}

	for _, entry := range task.entries {
		if !fanned && bytes.HasPrefix(entry.path, root.path) {
			if err := g.ensurePath(ctx, root, entry.path); err != nil {
				return [32]byte{}, nil, err
			}
		}
		if entry.deleted {
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

	parts, hash, err := g.persistGraph(ctx, root, plan)
	return hash, parts, err
}

func (g graph) rebuildStorage(ctx commitment.PatriciaContext, entries []storageEntry, wiped []recordDelta) ([32]byte, deltaParts, error) {
	root := fork(nil)
	root.loaded, root.plane = true, g.plane
	for _, entry := range entries {
		if entry.deleted {
			continue
		}
		if err := insertRoot(root, entry.path, entry.value); err != nil {
			return [32]byte{}, nil, err
		}
	}
	parts, hash, err := g.persistGraph(wipedContext{ctx}, root, foldPlan{})
	if err != nil {
		return [32]byte{}, nil, err
	}
	prev := make(map[string][]byte, len(wiped))
	for _, r := range wiped {
		prev[string(r.Key)] = r.Prev
	}
	var out deltaParts
	for _, part := range parts {
		for _, d := range part {
			if p, ok := prev[string(d.Key)]; ok {
				d.Prev = p
				delete(prev, string(d.Key))
			}
			out.add(d)
		}
	}
	for _, r := range wiped {
		if _, ok := prev[string(r.Key)]; ok {
			out.add(r)
		}
	}
	return hash, out, nil
}

type wipedContext struct{ commitment.PatriciaContext }

func (wipedContext) Branch([]byte) ([]byte, kv.Step, error) { return nil, 0, nil }

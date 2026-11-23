package state

import (
	"github.com/puzpuzpuz/xsync/v4"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// slotID is a storage key interned in a slotTable, so slot maps key by 8 bytes
// instead of 32. Two ids are equal only when they come from the same table.
type slotID = *accounts.StorageKey

// slotTable assigns slot ids. A shared table belongs to one block's VersionMap
// and every set of that block may hold it; a private table belongs to one set
// and its copies, because its map is not safe for concurrent use.
type slotTable struct {
	shared *xsync.Map[accounts.StorageKey, slotID]
	own    map[accounts.StorageKey]slotID
}

func newSharedSlotTable(sizeHint int) *slotTable {
	return &slotTable{shared: xsync.NewMap[accounts.StorageKey, slotID](xsync.WithPresize(sizeHint))}
}

func (t *slotTable) intern(k accounts.StorageKey) slotID {
	if t.shared != nil {
		if id, ok := t.shared.Load(k); ok {
			return id
		}
		kc := k
		id, _ := t.shared.LoadOrStore(k, &kc)
		return id
	}
	if id, ok := t.own[k]; ok {
		return id
	}
	if t.own == nil {
		t.own = make(map[accounts.StorageKey]slotID)
	}
	kc := k
	t.own[k] = &kc
	return &kc
}

func (t *slotTable) find(k accounts.StorageKey) (slotID, bool) {
	if t == nil {
		return nil, false
	}
	if t.shared != nil {
		return t.shared.Load(k)
	}
	id, ok := t.own[k]
	return id, ok
}

// adopt returns t's id for id of table from.
func (t *slotTable) adopt(from *slotTable, id slotID) slotID {
	if t == from {
		return id
	}
	return t.intern(*id)
}

// findFrom is adopt without interning: ok is false when t has no id for the key.
func (t *slotTable) findFrom(from *slotTable, id slotID) (slotID, bool) {
	if t == from {
		return id, true
	}
	return t.find(*id)
}

// joinSlots returns the table for a set that holds t and takes ids of src:
// src when it is a block table and the set has no storage yet, else t, or a new
// private table when t is nil.
func joinSlots(t *slotTable, empty bool, src *slotTable) *slotTable {
	if empty && src != nil && src.shared != nil {
		return src
	}
	if t == nil {
		return &slotTable{}
	}
	return t
}

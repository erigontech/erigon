// Copyright 2024 The Erigon Authors
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

package state

import (
	"bytes"
	"encoding/binary"
	"sync"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/db/kv"
)

type DomainMaps struct {
	muMaps  sync.RWMutex
	domains [kv.DomainLen]map[string]dataWithPrevStep
	storage *btree2.Map[string, dataWithPrevStep]
	estSize int
}

func NewDomainMaps() *DomainMaps {
	dm := &DomainMaps{
		storage: btree2.NewMap[string, dataWithPrevStep](128),
	}
	for i := range dm.domains {
		dm.domains[i] = map[string]dataWithPrevStep{}
	}
	return dm
}

func (dm *DomainMaps) DomainPut(domain kv.Domain, key string, val []byte, prevStep kv.Step) {
	dm.muMaps.Lock()
	defer dm.muMaps.Unlock()
	valWithPrevStep := dataWithPrevStep{data: val, prevStep: prevStep}
	var sizeDelta int
	if domain == kv.StorageDomain {
		if old, ok := dm.storage.Set(key, valWithPrevStep); ok {
			sizeDelta = len(val) - len(old.data)
		} else {
			sizeDelta = len(key) + len(val)
		}
		dm.estSize += sizeDelta
		return
	}

	if old, ok := dm.domains[domain][key]; ok {
		sizeDelta = len(val) - len(old.data)
	} else {
		sizeDelta = len(key) + len(val)
	}
	dm.domains[domain][key] = valWithPrevStep
	dm.estSize += sizeDelta
}

func (dm *DomainMaps) GetLatest(table kv.Domain, key []byte) (v []byte, prevStep kv.Step, ok bool) {
	dm.muMaps.RLock()
	defer dm.muMaps.RUnlock()

	keyS := toStringZeroCopy(key)
	var dataWithPrevStep dataWithPrevStep
	if table == kv.StorageDomain {
		dataWithPrevStep, ok = dm.storage.Get(keyS)
		return dataWithPrevStep.data, dataWithPrevStep.prevStep, ok
	}

	dataWithPrevStep, ok = dm.domains[table][keyS]
	return dataWithPrevStep.data, dataWithPrevStep.prevStep, ok
}

func (dm *DomainMaps) DomainDel(domain kv.Domain, key string, prevStep kv.Step) {
	dm.muMaps.Lock()
	defer dm.muMaps.Unlock()
	valWithPrevStep := dataWithPrevStep{data: nil, prevStep: prevStep}
	var sizeDelta int
	if domain == kv.StorageDomain {
		if old, ok := dm.storage.Set(key, valWithPrevStep); ok {
			sizeDelta = -len(old.data)
		}
		dm.estSize += sizeDelta
		return
	}

	if old, ok := dm.domains[domain][key]; ok {
		sizeDelta = -len(old.data)
	}
	dm.domains[domain][key] = valWithPrevStep
	dm.estSize += sizeDelta
}

func (dm *DomainMaps) ClearRam() {
	dm.muMaps.Lock()
	defer dm.muMaps.Unlock()
	for i := range dm.domains {
		dm.domains[i] = map[string]dataWithPrevStep{}
	}
	dm.storage = btree2.NewMap[string, dataWithPrevStep](128)
	dm.estSize = 0
}

func (dm *DomainMaps) SizeEstimate() int {
	dm.muMaps.RLock()
	defer dm.muMaps.RUnlock()
	return dm.estSize
}

func (dm *DomainMaps) ReadsValid(readLists map[string]*KvList) bool {
	dm.muMaps.RLock()
	defer dm.muMaps.RUnlock()

	for table, list := range readLists {
		switch table {
		case kv.AccountsDomain.String():
			m := dm.domains[kv.AccountsDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case kv.CodeDomain.String():
			m := dm.domains[kv.CodeDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case kv.StorageDomain.String():
			for i, key := range list.Keys {
				if val, ok := dm.storage.Get(key); ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case CodeSizeTableFake:
			m := dm.domains[kv.CodeDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if binary.BigEndian.Uint64(list.Vals[i]) != uint64(len(val.data)) {
						return false
					}
				}
			}
		default:
			panic(table)
		}
	}

	return true
}

func (dm *DomainMaps) GetStorageIter() btree2.MapIter[string, dataWithPrevStep] {
	dm.muMaps.RLock()
	defer dm.muMaps.RUnlock()
	return dm.storage.Iter()
}

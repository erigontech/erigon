// Copyright 2022 The Erigon Authors
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
	"github.com/erigontech/erigon-lib/kv"
)

type SelectedStaticFilesV3 struct {
	d     [kv.DomainLen][]*filesItem
	dHist [kv.DomainLen][]*filesItem
	dIdx  [kv.DomainLen][]*filesItem
	ii    [kv.StandaloneIdxLen][]*filesItem
}

func (sf SelectedStaticFilesV3) Close() {
	clist := make([][]*filesItem, 0, int(kv.DomainLen)+int(kv.StandaloneIdxLen))
	for id := range sf.d {
		clist = append(clist, sf.d[id], sf.dIdx[id], sf.dHist[id])
	}

	for _, i := range sf.ii {
		clist = append(clist, i)
	}
	for _, group := range clist {
		for _, item := range group {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.index != nil {
					item.index.Close()
				}
			}
		}
	}
}

func (ac *AggregatorRoTx) staticFilesInRange(r RangesV3) (sf SelectedStaticFilesV3, err error) {
	for id := range ac.d {
		if !r.domain[id].any() {
			continue
		}
		sf.d[id], sf.dIdx[id], sf.dHist[id] = ac.d[id].staticFilesInRange(r.domain[id])
	}
	for id, rng := range r.invertedIndex {
		if rng == nil || !rng.needMerge {
			continue
		}
		sf.ii[id] = ac.iis[id].staticFilesInRange(rng.from, rng.to)
	}
	return sf, err
}

type MergedFilesV3 struct {
	d     [kv.DomainLen]*filesItem
	dHist [kv.DomainLen]*filesItem
	dIdx  [kv.DomainLen]*filesItem
	iis   [kv.StandaloneIdxLen]*filesItem
}

func (mf MergedFilesV3) FrozenList() (frozen []string) {
	for id, d := range mf.d {
		if d == nil {
			continue
		}
		frozen = append(frozen, d.decompressor.FileName())

		if mf.dHist[id] != nil && mf.dHist[id].frozen {
			frozen = append(frozen, mf.dHist[id].decompressor.FileName())
		}
		if mf.dIdx[id] != nil && mf.dIdx[id].frozen {
			frozen = append(frozen, mf.dIdx[id].decompressor.FileName())
		}
	}

	for _, ii := range mf.iis {
		if ii != nil && ii.frozen {
			frozen = append(frozen, ii.decompressor.FileName())
		}
	}
	return frozen
}
func (mf MergedFilesV3) Close() {
	clist := make([]*filesItem, 0, kv.DomainLen+4)
	for id := range mf.d {
		clist = append(clist, mf.d[id], mf.dHist[id], mf.dIdx[id])
	}
	clist = append(clist, mf.iis[:]...)

	for _, item := range clist {
		if item != nil {
			if item.decompressor != nil {
				item.decompressor.Close()
			}
			if item.index != nil {
				item.index.Close()
			}
		}
	}
}

type MergedFiles struct {
	d     [kv.DomainLen]*filesItem
	dHist [kv.DomainLen]*filesItem
	dIdx  [kv.DomainLen]*filesItem
}

func (mf MergedFiles) FillV3(m *MergedFilesV3) MergedFiles {
	for id := range m.d {
		mf.d[id], mf.dHist[id], mf.dIdx[id] = m.d[id], m.dHist[id], m.dIdx[id]
	}
	return mf
}

func (mf MergedFiles) Close() {
	for id := range mf.d {
		for _, item := range []*filesItem{mf.d[id], mf.dHist[id], mf.dIdx[id]} {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.decompressor != nil {
					item.index.Close()
				}
				if item.bindex != nil {
					item.bindex.Close()
				}
			}
		}
	}
}

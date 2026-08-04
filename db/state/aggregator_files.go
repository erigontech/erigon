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
	"github.com/erigontech/erigon/db/kv"
)

type visibleFilesForMerge struct {
	d     [kv.DomainLen][]*FilesItem
	dHist [kv.DomainLen][]*FilesItem
	dIdx  [kv.DomainLen][]*FilesItem
	ii    [kv.StandaloneIdxLen][]*FilesItem
}

func (sf *visibleFilesForMerge) DomainFiles(name kv.Domain) []*FilesItem {
	return sf.d[name]
}

func (sf *visibleFilesForMerge) DomainHistoryFiles(name kv.Domain) []*FilesItem {
	return sf.dHist[name]
}

func (sf *visibleFilesForMerge) DomainInvertedIndexFiles(name kv.Domain) []*FilesItem {
	return sf.dIdx[name]
}

func (sf *visibleFilesForMerge) InvertedIndexFiles(id int) []*FilesItem {
	return sf.ii[id]
}

func (sf *visibleFilesForMerge) Close() {
	clist := make([][]*FilesItem, 0, 3*int(kv.DomainLen)+kv.StandaloneIdxLen)
	for id := range sf.d {
		clist = append(clist, sf.d[id], sf.dIdx[id], sf.dHist[id])
	}

	clist = append(clist, sf.ii[:]...)
	for _, group := range clist {
		for _, item := range group {
			item.closeFiles()
		}
	}
}

func (at *AggregatorRoTx) filesInRange(r *Ranges) (*visibleFilesForMerge, error) {
	sf := &visibleFilesForMerge{}
	for id := range at.d {
		if at.d[id].d.Disable {
			continue
		}
		if !r.domain[id].any() {
			continue
		}
		sf.d[id], sf.dIdx[id], sf.dHist[id] = at.d[id].staticFilesInRange(r.domain[id])
	}
	for id, rng := range r.invertedIndex {
		if rng == nil || at.iis[id] == nil || at.iis[id].ii.Disable {
			continue
		}
		if !rng.needMerge {
			continue
		}
		sf.ii[id] = at.iis[id].staticFilesInRange(rng.from, rng.to)
	}
	return sf, nil
}

func (at *AggregatorRoTx) InvertedIndicesLen() int {
	return at.iisCount
}

func (at *AggregatorRoTx) InvertedIndexName(id int) kv.InvertedIdx {
	return at.iis[id].name
}

type MergeResult struct {
	d     [kv.DomainLen]*FilesItem
	dHist [kv.DomainLen]*FilesItem
	dIdx  [kv.DomainLen]*FilesItem
	iis   [kv.StandaloneIdxLen]*FilesItem
}

// A single per-domain merge round can populate any subset of {values,
// history, idx}. Values-only, values+history+idx, and history+idx-only
// are all valid outputs — history/idx merge when values doesn't need
// it (see DomainRoTx.findMergeRange: history only runs when !r.any()
// on values). Each slot must be checked independently or the Inventory
// notification silently drops the ones the caller did produce and
// NotifyOnFilesChange panics on empty names.
func (mf MergeResult) FilePaths(relative string) (fPaths []string) {
	for id := range mf.d {
		if mf.d[id] != nil {
			fPaths = append(fPaths, mf.d[id].FilePaths(relative)...)
		}
		if mf.dHist[id] != nil {
			fPaths = append(fPaths, mf.dHist[id].FilePaths(relative)...)
		}
		if mf.dIdx[id] != nil {
			fPaths = append(fPaths, mf.dIdx[id].FilePaths(relative)...)
		}
	}

	for _, ii := range mf.iis {
		if ii == nil {
			continue
		}
		fPaths = append(fPaths, ii.FilePaths(relative)...)
	}
	return fPaths
}
func (mf *MergeResult) Close() {
	if mf == nil {
		return
	}
	clist := make([]*FilesItem, 0, 3*int(kv.DomainLen)+kv.StandaloneIdxLen)
	for id := range mf.d {
		clist = append(clist, mf.d[id], mf.dHist[id], mf.dIdx[id])
	}
	clist = append(clist, mf.iis[:]...)
	for _, item := range clist {
		item.closeFiles()
	}
}

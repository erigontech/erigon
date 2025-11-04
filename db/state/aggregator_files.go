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

type SelectedStaticFiles struct {
	d     [kv.DomainLen][]*FilesItem
	dHist [kv.DomainLen][]*FilesItem
	dIdx  [kv.DomainLen][]*FilesItem
	ii    [][]*FilesItem
}

func (sf *SelectedStaticFiles) DomainFiles(name kv.Domain) []*FilesItem {
	return sf.d[name]
}

func (sf *SelectedStaticFiles) DomainHistoryFiles(name kv.Domain) []*FilesItem {
	return sf.dHist[name]
}

func (sf *SelectedStaticFiles) DomainInvertedIndexFiles(name kv.Domain) []*FilesItem {
	return sf.dIdx[name]
}

func (sf *SelectedStaticFiles) InvertedIndexFiles(id int) []*FilesItem {
	return sf.ii[id]
}

func (sf *SelectedStaticFiles) Close() {
	clist := make([][]*FilesItem, 0, int(kv.DomainLen)+len(sf.ii))
	for id := range sf.d {
		clist = append(clist, sf.d[id], sf.dIdx[id], sf.dHist[id])
	}

	clist = append(clist, sf.ii...)
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

func (at *AggregatorRoTx) FilesInRange(r *Ranges) (*SelectedStaticFiles, error) {
	sf := &SelectedStaticFiles{ii: make([][]*FilesItem, len(r.invertedIndex))}
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
		if at.iis[id].ii.Disable {
			continue
		}
		if rng == nil || !rng.needMerge {
			continue
		}
		sf.ii[id] = at.iis[id].staticFilesInRange(rng.from, rng.to)
	}
	return sf, nil
}

func (at *AggregatorRoTx) InvertedIndicesLen() int {
	return len(at.iis)
}

func (at *AggregatorRoTx) InvertedIndexName(id int) kv.InvertedIdx {
	return at.iis[id].name
}

type MergedFilesV3 struct {
	d     [kv.DomainLen]*FilesItem
	dHist [kv.DomainLen]*FilesItem
	dIdx  [kv.DomainLen]*FilesItem
	iis   []*FilesItem
}

func (mf MergedFilesV3) FilePaths(relative string) (fPaths []string) {
	for id, d := range mf.d {
		if d == nil {
			continue
		}
		fPaths = append(fPaths, d.FilePaths(relative)...)
		if mf.dHist[id] != nil {
			fPaths = append(fPaths, mf.dHist[id].FilePaths(relative)...)
		}
		if mf.dIdx[id] != nil && mf.dIdx[id].frozen {
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
func (mf *MergedFilesV3) Close() {
	if mf == nil {
		return
	}
	clist := make([]*FilesItem, 0, kv.DomainLen+4)
	for id := range mf.d {
		clist = append(clist, mf.d[id], mf.dHist[id], mf.dIdx[id])
	}
	clist = append(clist, mf.iis...)
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
	d     [kv.DomainLen]*FilesItem
	dHist [kv.DomainLen]*FilesItem
	dIdx  [kv.DomainLen]*FilesItem
}

func (mf MergedFiles) FillV3(m *MergedFilesV3) MergedFiles {
	for id := range m.d {
		mf.d[id], mf.dHist[id], mf.dIdx[id] = m.d[id], m.dHist[id], m.dIdx[id]
	}
	return mf
}

func (mf MergedFiles) Close() {
	for id := range mf.d {
		for _, item := range []*FilesItem{mf.d[id], mf.dHist[id], mf.dIdx[id]} {
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

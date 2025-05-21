package state

import (
	"github.com/erigontech/erigon-lib/kv"
)

type aggDirtyFilesRoTx struct {
	agg    *Aggregator
	domain []*domainDirtyFilesRoTx
	ii     []*iiDirtyFilesRoTx
}

type domainDirtyFilesRoTx struct {
	d       *Domain
	files   []*filesItem
	history *historyDirtyFilesRoTx
}

type historyDirtyFilesRoTx struct {
	h     *History
	files []*filesItem
	ii    *iiDirtyFilesRoTx
}

type iiDirtyFilesRoTx struct {
	ii    *InvertedIndex
	files []*filesItem
}

func (a *Aggregator) DebugBeginDirtyFilesRo() *aggDirtyFilesRoTx {
	ac := &aggDirtyFilesRoTx{
		agg:    a,
		domain: make([]*domainDirtyFilesRoTx, len(a.d)),
		ii:     make([]*iiDirtyFilesRoTx, len(a.iis)),
	}

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	for i, d := range a.d {
		ac.domain[i] = d.DebugBeginDirtyFilesRo()
	}

	for i, ii := range a.iis {
		ac.ii[i] = ii.DebugBeginDirtyFilesRo()
	}

	return ac
}

func (ac *aggDirtyFilesRoTx) MadvNormal() *aggDirtyFilesRoTx {
	for _, d := range ac.domain {
		for _, f := range d.files {
			f.MadvNormal()
		}
		for _, f := range d.history.files {
			f.MadvNormal()
		}
		for _, f := range d.history.ii.files {
			f.MadvNormal()
		}
	}
	for _, ii := range ac.ii {
		for _, f := range ii.files {
			f.MadvNormal()
		}
	}
	return ac
}
func (ac *aggDirtyFilesRoTx) DisableReadAhead() {
	for _, d := range ac.domain {
		for _, f := range d.files {
			f.DisableReadAhead()
		}
		for _, f := range d.history.files {
			f.DisableReadAhead()
		}
		for _, f := range d.history.ii.files {
			f.DisableReadAhead()
		}
	}
	for _, ii := range ac.ii {
		for _, f := range ii.files {
			f.DisableReadAhead()
		}
	}
}

func (ac *aggDirtyFilesRoTx) FilesWithMissedAccessors() (mf *MissedAccessorAggFiles) {
	mf = &MissedAccessorAggFiles{
		domain: make(map[kv.Domain]*MissedAccessorDomainFiles),
		ii:     make(map[kv.InvertedIdx]*MissedAccessorIIFiles),
	}

	for _, d := range ac.domain {
		mf.domain[d.d.name] = d.FilesWithMissedAccessors()
	}

	for _, ii := range ac.ii {
		mf.ii[ii.ii.name] = ii.FilesWithMissedAccessors()
	}

	return
}

func (ac *aggDirtyFilesRoTx) Close() {
	if ac.agg == nil {
		return
	}
	// not doing closeAndRemove() because that needs dirtyFilesLock.
	// if canDelete is true, it'll get removed in the AggRoTx.Close() path.
	for _, d := range ac.domain {
		d.Close()
	}

	for _, ii := range ac.ii {
		ii.Close()
	}
	ac.agg = nil
	ac.domain = nil
	ac.ii = nil
}

func (d *Domain) DebugBeginDirtyFilesRo() *domainDirtyFilesRoTx {
	var files []*filesItem
	d.dirtyFiles.Walk(func(items []*filesItem) bool {
		files = append(files, items...)
		for _, item := range items {
			item.refcount.Add(1)
		}
		return true
	})
	return &domainDirtyFilesRoTx{
		d:       d,
		files:   files,
		history: d.History.DebugBeginDirtyFilesRo(),
	}
}

func (d *domainDirtyFilesRoTx) FilesWithMissedAccessors() (mf *MissedAccessorDomainFiles) {
	return &MissedAccessorDomainFiles{
		files: map[Accessors][]*filesItem{
			AccessorBTree:   d.d.missedBtreeAccessors(d.files),
			AccessorHashMap: d.d.missedMapAccessors(d.files),
		},
		history: d.history.FilesWithMissedAccessors(),
	}
}

func (d *domainDirtyFilesRoTx) Close() {
	if d.d == nil {
		return
	}
	d.history.Close()
	for _, item := range d.files {
		item.refcount.Add(-1)
	}
	d.files = nil
	d.d = nil
}

func (h *History) DebugBeginDirtyFilesRo() *historyDirtyFilesRoTx {
	var files []*filesItem
	h.dirtyFiles.Walk(func(items []*filesItem) bool {
		files = append(files, items...)
		for _, item := range items {
			item.refcount.Add(1)
		}
		return true
	})
	return &historyDirtyFilesRoTx{
		h:     h,
		files: files,
		ii:    h.InvertedIndex.DebugBeginDirtyFilesRo(),
	}
}

func (f *historyDirtyFilesRoTx) FilesWithMissedAccessors() (mf *MissedAccessorHistoryFiles) {
	return &MissedAccessorHistoryFiles{
		ii: f.ii.FilesWithMissedAccessors(),
		files: map[Accessors][]*filesItem{
			AccessorHashMap: f.h.missedMapAccessors(f.files),
		},
	}
}

func (f *historyDirtyFilesRoTx) Close() {
	if f.h == nil {
		return
	}
	f.ii.Close()
	for _, item := range f.files {
		item.refcount.Add(-1)
	}
	f.files = nil
	f.h = nil
}

func (ii *InvertedIndex) DebugBeginDirtyFilesRo() *iiDirtyFilesRoTx {
	var files []*filesItem
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		files = append(files, items...)
		for _, item := range items {
			item.refcount.Add(1)
		}
		return true
	})
	return &iiDirtyFilesRoTx{
		ii:    ii,
		files: files,
	}
}

func (f *iiDirtyFilesRoTx) FilesWithMissedAccessors() (mf *MissedAccessorIIFiles) {
	return &MissedAccessorIIFiles{
		files: map[Accessors][]*filesItem{
			AccessorHashMap: f.ii.missedMapAccessors(f.files),
		},
	}
}

func (f *iiDirtyFilesRoTx) Close() {
	if f.ii == nil {
		return
	}
	for _, item := range f.files {
		item.refcount.Add(-1)
	}
	f.files = nil
	f.ii = nil
}

// fileItems collection of missed files
type MissedFilesMap map[Accessors][]*filesItem
type MissedAccessorAggFiles struct {
	domain map[kv.Domain]*MissedAccessorDomainFiles
	ii     map[kv.InvertedIdx]*MissedAccessorIIFiles
}

type MissedAccessorDomainFiles struct {
	history *MissedAccessorHistoryFiles
	files   MissedFilesMap
}

func (m *MissedAccessorDomainFiles) missedBtreeAccessors() []*filesItem {
	return m.files[AccessorBTree]
}

func (m *MissedAccessorDomainFiles) missedMapAccessors() []*filesItem {
	return m.files[AccessorHashMap]
}

type MissedAccessorHistoryFiles struct {
	ii    *MissedAccessorIIFiles
	files MissedFilesMap
}

func (m *MissedAccessorHistoryFiles) missedMapAccessors() []*filesItem {
	return m.files[AccessorHashMap]
}

type MissedAccessorIIFiles struct {
	files MissedFilesMap
}

func (m *MissedAccessorIIFiles) missedMapAccessors() []*filesItem {
	return m.files[AccessorHashMap]
}

func (at *AggregatorRoTx) DbgDomain(idx kv.Domain) *DomainRoTx         { return at.d[idx] }
func (at *AggregatorRoTx) DbgII(idx kv.InvertedIdx) *InvertedIndexRoTx { return at.searchII(idx) }
func (at *AggregatorRoTx) searchII(idx kv.InvertedIdx) *InvertedIndexRoTx {
	for _, iit := range at.iis {
		if iit.name == idx {
			return iit
		}
	}
	return nil
}

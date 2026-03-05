package state

import "github.com/erigontech/erigon/db/kv"

type forkableAggDirtyFilesRoTx struct {
	closed       bool
	markedRoTx   []*forkableDirtyFilesRoTx
	unmarkedRoTx []*forkableDirtyFilesRoTx
}

type forkableDirtyFilesRoTx struct {
	p     *ProtoForkable
	files []*FilesItem
}

func (f *forkableDirtyFilesRoTx) Close() {
	if f.p == nil {
		return
	}
	f.p = nil
	for _, item := range f.files {
		if !item.frozen {
			item.refcount.Add(-1)
		}
	}
	f.files = nil
}

type MissedAccessorForkAggFiles struct {
	unmarked map[kv.ForkableId]*MissedFilesMap
	marked   map[kv.ForkableId]*MissedFilesMap
}

func (m *MissedAccessorForkAggFiles) IsEmpty() bool {
	if m == nil {
		return true
	}
	for _, m := range m.marked {
		if !m.IsEmpty() {
			return false
		}
	}
	for _, u := range m.unmarked {
		if !u.IsEmpty() {
			return false
		}
	}
	return true
}

func (r *ForkableAgg) DebugBeginDirtyFilesRo() *forkableAggDirtyFilesRoTx {
	fc := &forkableAggDirtyFilesRoTx{
		markedRoTx:   make([]*forkableDirtyFilesRoTx, len(r.marked)),
		unmarkedRoTx: make([]*forkableDirtyFilesRoTx, len(r.unmarked)),
	}

	r.dirtyFilesLock.Lock()
	defer r.dirtyFilesLock.Unlock()
	for i, m := range r.marked {
		fc.markedRoTx[i] = m.DebugBeginDirtyFilesRo()
	}
	for i, u := range r.unmarked {
		fc.unmarkedRoTx[i] = u.DebugBeginDirtyFilesRo()
	}

	return fc
}

// need dirty files lock
func (r *forkableAggDirtyFilesRoTx) FilesWithMissedAccessors() (mf *MissedAccessorForkAggFiles) {
	mf = &MissedAccessorForkAggFiles{
		unmarked: make(map[kv.ForkableId]*MissedFilesMap),
		marked:   make(map[kv.ForkableId]*MissedFilesMap),
	}

	for _, m := range r.markedRoTx {
		mf.marked[m.p.id] = m.p.FilesWithMissedAccessors()
	}
	for _, u := range r.unmarkedRoTx {
		mf.unmarked[u.p.id] = u.p.FilesWithMissedAccessors()
	}
	return
}

func (r *forkableAggDirtyFilesRoTx) Close() {
	if r.closed {
		return
	}
	r.closed = true
	for _, m := range r.markedRoTx {
		m.Close()
	}
	for _, u := range r.unmarkedRoTx {
		u.Close()
	}
}

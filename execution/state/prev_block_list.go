package state

import "sync"

// PrevBlockList is an ordered list of finished-but-not-yet-committed blocks'
// versionMaps, newest at the head. A finished block is pushed at the head, a
// committed one removed from the tail, both in block order, so its length is the
// exec-ahead-of-commit window rather than the whole chain.
//
// The maps are read-only once listed, so reads need no coordination; the mutex
// guards only the list links against concurrent PushHead / RemoveTail.
type PrevBlockList struct {
	mu   sync.Mutex
	head *prevBlockNode // newest finished block
	tail *prevBlockNode // oldest not-yet-committed block
	n    int
}

type prevBlockNode struct {
	blockNum uint64
	endTxNum uint64 // the block's last (block-end) txNum
	vm       *VersionMap
	older    *prevBlockNode // toward the tail
	newer    *prevBlockNode // toward the head
}

func NewPrevBlockList() *PrevBlockList { return &PrevBlockList{} }

// TailBlockNum returns the oldest not-yet-committed block's number, or ok=false
// when the list is empty, so the commit path can assert it is dropping the block
// it just committed rather than a wrong overlay on a push/remove desync.
func (l *PrevBlockList) TailBlockNum() (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.tail == nil {
		return 0, false
	}
	return l.tail.blockNum, true
}

// PushHead adds a newly finished block at the head. endTxNum lets a reader select
// the window by txNum (matching the per-tx apply) rather than blockNum.
func (l *PrevBlockList) PushHead(blockNum, endTxNum uint64, vm *VersionMap) {
	l.mu.Lock()
	defer l.mu.Unlock()
	node := &prevBlockNode{blockNum: blockNum, endTxNum: endTxNum, vm: vm, older: l.head}
	if l.head != nil {
		l.head.newer = node
	} else {
		l.tail = node
	}
	l.head = node
	l.n++
}

// RemoveTail drops the oldest block once its writes are committed to the shared
// domain: reads for it now fall through to the shared domain. Commits are in
// block order, so the committed block is always the tail.
func (l *PrevBlockList) RemoveTail() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.tail == nil {
		return
	}
	l.tail = l.tail.newer
	if l.tail != nil {
		l.tail.older = nil
	} else {
		l.head = nil
	}
	l.n--
}

// Before returns the versionMaps of blocks earlier than blockNum, tail→head
// (oldest→newest) so a layered reader wraps the newest outermost. The current
// block's own map is never included.
func (l *PrevBlockList) Before(blockNum uint64) []*VersionMap {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []*VersionMap
	for node := l.tail; node != nil; node = node.newer {
		if node.blockNum < blockNum {
			out = append(out, node.vm)
		}
	}
	return out
}

// BeforeTxNum returns the versionMaps of blocks whose block-end txNum is < txNum,
// tail→head (oldest→newest) so a layered reader wraps the newest outermost. This
// is the txNum-keyed selection (matching the per-tx apply): a reader positioned
// at txNum sees every finished-but-uncommitted block up to that point.
func (l *PrevBlockList) BeforeTxNum(txNum uint64) []*VersionMap {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []*VersionMap
	for node := l.tail; node != nil; node = node.newer {
		if node.endTxNum < txNum {
			out = append(out, node.vm)
		}
	}
	return out
}

// Len is the current window length (exec-ahead-of-commit). Test/metric use.
func (l *PrevBlockList) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.n
}

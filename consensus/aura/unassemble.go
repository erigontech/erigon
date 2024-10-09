package aura

import (
	"container/list"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// nolint
type unAssembledHeader struct {
	hash    libcommon.Hash
	number  uint64
	signers []libcommon.Address
}
type unAssembledHeaders struct {
	l *list.List
}

func (u unAssembledHeaders) PushBack(header *unAssembledHeader)  { u.l.PushBack(header) }
func (u unAssembledHeaders) PushFront(header *unAssembledHeader) { u.l.PushFront(header) }
func (u unAssembledHeaders) Pop() *unAssembledHeader {
	e := u.l.Front()
	if e == nil {
		return nil
	}
	u.l.Remove(e)
	return e.Value.(*unAssembledHeader)
}
func (u unAssembledHeaders) Front() *unAssembledHeader {
	e := u.l.Front()
	if e == nil {
		return nil
	}
	return e.Value.(*unAssembledHeader)
}

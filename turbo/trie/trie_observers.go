package trie

import (
	libcommon "github.com/erigontech/erigon-lib/common"
)

type Observer interface {
	BranchNodeCreated(hex []byte)
	BranchNodeDeleted(hex []byte)
	BranchNodeTouched(hex []byte)

	CodeNodeCreated(hex []byte, size uint)
	CodeNodeDeleted(hex []byte)
	CodeNodeTouched(hex []byte)
	CodeNodeSizeChanged(hex []byte, newSize uint)

	WillUnloadBranchNode(key []byte, nodeHash libcommon.Hash, incarnation uint64)
	WillUnloadNode(key []byte, nodeHash libcommon.Hash)
	BranchNodeLoaded(prefixAsNibbles []byte, incarnation uint64)
}

var _ Observer = (*NoopObserver)(nil) // make sure that NoopTrieObserver is compliant

// NoopTrieObserver might be used to emulate optional methods in observers
type NoopObserver struct{}

func (*NoopObserver) BranchNodeCreated(_ []byte)                                {}
func (*NoopObserver) BranchNodeDeleted(_ []byte)                                {}
func (*NoopObserver) BranchNodeTouched(_ []byte)                                {}
func (*NoopObserver) CodeNodeCreated(_ []byte, _ uint)                          {}
func (*NoopObserver) CodeNodeDeleted(_ []byte)                                  {}
func (*NoopObserver) CodeNodeTouched(_ []byte)                                  {}
func (*NoopObserver) CodeNodeSizeChanged(_ []byte, _ uint)                      {}
func (*NoopObserver) WillUnloadBranchNode(_ []byte, _ libcommon.Hash, _ uint64) {}
func (*NoopObserver) WillUnloadNode(_ []byte, _ libcommon.Hash)                 {}
func (*NoopObserver) BranchNodeLoaded(_ []byte, _ uint64)                       {}

// TrieObserverMux multiplies the callback methods and sends them to
// all it's children.
type ObserverMux struct {
	children []Observer
}

func NewTrieObserverMux() *ObserverMux {
	return &ObserverMux{make([]Observer, 0)}
}

func (mux *ObserverMux) AddChild(child Observer) {
	if child == nil {
		return
	}

	mux.children = append(mux.children, child)
}

func (mux *ObserverMux) BranchNodeCreated(hex []byte) {
	for _, child := range mux.children {
		child.BranchNodeCreated(hex)
	}
}

func (mux *ObserverMux) BranchNodeDeleted(hex []byte) {
	for _, child := range mux.children {
		child.BranchNodeDeleted(hex)
	}
}

func (mux *ObserverMux) BranchNodeTouched(hex []byte) {
	for _, child := range mux.children {
		child.BranchNodeTouched(hex)
	}
}

func (mux *ObserverMux) CodeNodeCreated(hex []byte, size uint) {
	for _, child := range mux.children {
		child.CodeNodeCreated(hex, size)
	}
}

func (mux *ObserverMux) CodeNodeDeleted(hex []byte) {
	for _, child := range mux.children {
		child.CodeNodeDeleted(hex)
	}
}

func (mux *ObserverMux) CodeNodeTouched(hex []byte) {
	for _, child := range mux.children {
		child.CodeNodeTouched(hex)
	}
}

func (mux *ObserverMux) CodeNodeSizeChanged(hex []byte, newSize uint) {
	for _, child := range mux.children {
		child.CodeNodeSizeChanged(hex, newSize)
	}
}

func (mux *ObserverMux) WillUnloadNode(key []byte, nodeHash libcommon.Hash) {
	for _, child := range mux.children {
		child.WillUnloadNode(key, nodeHash)
	}
}

func (mux *ObserverMux) WillUnloadBranchNode(key []byte, nodeHash libcommon.Hash, incarnation uint64) {
	for _, child := range mux.children {
		child.WillUnloadBranchNode(key, nodeHash, incarnation)
	}
}

func (mux *ObserverMux) BranchNodeLoaded(prefixAsNibbles []byte, incarnation uint64) {
	for _, child := range mux.children {
		child.BranchNodeLoaded(prefixAsNibbles, incarnation)
	}
}

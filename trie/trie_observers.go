package trie

import "github.com/ledgerwatch/turbo-geth/common"

type TrieObserver interface {
	BranchNodeCreated(hex []byte)
	BranchNodeDeleted(hex []byte)
	BranchNodeTouched(hex []byte)

	CodeNodeCreated(hex []byte, size uint)
	CodeNodeDeleted(hex []byte)
	CodeNodeTouched(hex []byte)
	CodeNodeSizeChanged(hex []byte, newSize uint)

	WillUnloadBranchNode(key []byte, nodeHash common.Hash)
	BranchNodeLoaded(prefixAsNibbles []byte)
}

var _ TrieObserver = (*NoopTrieObserver)(nil) // make sure that NoopTrieObserver is compliant

// NoopTrieObserver might be used to emulate optional methods in observers
type NoopTrieObserver struct{}

func (*NoopTrieObserver) BranchNodeCreated(_ []byte)                   {}
func (*NoopTrieObserver) BranchNodeDeleted(_ []byte)                   {}
func (*NoopTrieObserver) BranchNodeTouched(_ []byte)                   {}
func (*NoopTrieObserver) CodeNodeCreated(_ []byte, _ uint)             {}
func (*NoopTrieObserver) CodeNodeDeleted(_ []byte)                     {}
func (*NoopTrieObserver) CodeNodeTouched(_ []byte)                     {}
func (*NoopTrieObserver) CodeNodeSizeChanged(_ []byte, _ uint)         {}
func (*NoopTrieObserver) WillUnloadBranchNode(_ []byte, _ common.Hash) {}
func (*NoopTrieObserver) BranchNodeLoaded(_ []byte)                    {}

// TrieObserverMux multiplies the callback methods and sends them to
// all it's children.
type TrieObserversMux struct {
	children []TrieObserver
}

func NewTrieObserverMux() *TrieObserversMux {
	return &TrieObserversMux{make([]TrieObserver, 0)}
}

func (mux *TrieObserversMux) AddChild(child TrieObserver) {
	if child == nil {
		return
	}

	mux.children = append(mux.children, child)
}

func (mux *TrieObserversMux) BranchNodeCreated(hex []byte) {
	for _, child := range mux.children {
		child.BranchNodeCreated(hex)
	}
}

func (mux *TrieObserversMux) BranchNodeDeleted(hex []byte) {
	for _, child := range mux.children {
		child.BranchNodeDeleted(hex)
	}
}

func (mux *TrieObserversMux) BranchNodeTouched(hex []byte) {
	for _, child := range mux.children {
		child.BranchNodeTouched(hex)
	}
}

func (mux *TrieObserversMux) CodeNodeCreated(hex []byte, size uint) {
	for _, child := range mux.children {
		child.CodeNodeCreated(hex, size)
	}
}

func (mux *TrieObserversMux) CodeNodeDeleted(hex []byte) {
	for _, child := range mux.children {
		child.CodeNodeDeleted(hex)
	}
}

func (mux *TrieObserversMux) CodeNodeTouched(hex []byte) {
	for _, child := range mux.children {
		child.CodeNodeTouched(hex)
	}
}

func (mux *TrieObserversMux) CodeNodeSizeChanged(hex []byte, newSize uint) {
	for _, child := range mux.children {
		child.CodeNodeSizeChanged(hex, newSize)
	}
}

func (mux *TrieObserversMux) WillUnloadBranchNode(key []byte, nodeHash common.Hash) {
	for _, child := range mux.children {
		child.WillUnloadBranchNode(key, nodeHash)
	}
}

func (mux *TrieObserversMux) BranchNodeLoaded(prefixAsNibbles []byte) {
	for _, child := range mux.children {
		child.BranchNodeLoaded(prefixAsNibbles)
	}
}

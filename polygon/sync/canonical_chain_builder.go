package sync

import (
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -destination=./mock/canonical_chain_builder_mock.go -package=mock . CanonicalChainBuilder
type CanonicalChainBuilder interface {
	Reset(root *types.Header)
	ContainsHash(hash libcommon.Hash) bool
	Tip() *types.Header
	HeadersInRange(start uint64, count uint64) []*types.Header
	Prune(newRootNum uint64) error
	Connect(headers []*types.Header) error
}

type producerSlotIndex uint64

type forkTreeNode struct {
	parent   *forkTreeNode
	children map[producerSlotIndex]*forkTreeNode

	header     *types.Header
	headerHash libcommon.Hash

	totalDifficulty uint64
}

type canonicalChainBuilderImpl struct {
	root *forkTreeNode
	tip  *forkTreeNode

	difficultyCalc DifficultyCalculator
}

func NewCanonicalChainBuilder(
	root *types.Header,
	difficultyCalc DifficultyCalculator,
) CanonicalChainBuilder {
	impl := &canonicalChainBuilderImpl{
		difficultyCalc: difficultyCalc,
	}
	impl.Reset(root)
	return impl
}

func (impl *canonicalChainBuilderImpl) Reset(root *types.Header) {
	impl.root = &forkTreeNode{
		children:   make(map[producerSlotIndex]*forkTreeNode),
		header:     root,
		headerHash: root.Hash(),
	}
	impl.tip = impl.root
}

// depth-first search
func (impl *canonicalChainBuilderImpl) enumerate(visitFunc func(*forkTreeNode) bool) {
	stack := []*forkTreeNode{impl.root}
	for len(stack) > 0 {
		// pop
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if !visitFunc(node) {
			break
		}

		for _, child := range node.children {
			stack = append(stack, child)
		}
	}
}

func (impl *canonicalChainBuilderImpl) nodeByHash(hash libcommon.Hash) *forkTreeNode {
	var result *forkTreeNode
	impl.enumerate(func(node *forkTreeNode) bool {
		if node.headerHash == hash {
			result = node
		}
		return result == nil
	})
	return result
}

func (impl *canonicalChainBuilderImpl) ContainsHash(hash libcommon.Hash) bool {
	return impl.nodeByHash(hash) != nil
}

func (impl *canonicalChainBuilderImpl) Tip() *types.Header {
	return impl.tip.header
}

func (impl *canonicalChainBuilderImpl) Headers() []*types.Header {
	var headers []*types.Header
	node := impl.tip
	for node != nil {
		headers = append(headers, node.header)
		node = node.parent
	}
	libcommon.SliceReverse(headers)
	return headers
}

func (impl *canonicalChainBuilderImpl) HeadersInRange(start uint64, count uint64) []*types.Header {
	headers := impl.Headers()
	if len(headers) == 0 {
		return nil
	}
	if headers[0].Number.Uint64() > start {
		return nil
	}
	if headers[len(headers)-1].Number.Uint64() < start+count-1 {
		return nil
	}

	offset := start - headers[0].Number.Uint64()
	return headers[offset : offset+count]
}

func (impl *canonicalChainBuilderImpl) Prune(newRootNum uint64) error {
	if (newRootNum < impl.root.header.Number.Uint64()) || (newRootNum > impl.Tip().Number.Uint64()) {
		return errors.New("canonicalChainBuilderImpl.Prune: newRootNum outside of the canonical chain")
	}

	newRoot := impl.tip
	for newRoot.header.Number.Uint64() > newRootNum {
		newRoot = newRoot.parent
	}

	impl.root = newRoot
	return nil
}

func (impl *canonicalChainBuilderImpl) updateTipIfNeeded(tipCandidate *forkTreeNode) {
	if tipCandidate.totalDifficulty > impl.tip.totalDifficulty {
		impl.tip = tipCandidate
	}
	// else if tipCandidate.totalDifficulty == impl.tip.totalDifficulty {
	// TODO: is it possible? which one is selected?
	// }
}

func (impl *canonicalChainBuilderImpl) Connect(headers []*types.Header) error {
	if (len(headers) > 0) && (headers[0].Number != nil) && (headers[0].Number.Cmp(impl.root.header.Number) == 0) {
		headers = headers[1:]
	}
	if len(headers) == 0 {
		return nil
	}

	parent := impl.nodeByHash(headers[0].ParentHash)
	if parent == nil {
		return errors.New("canonicalChainBuilderImpl.Connect: can't connect headers")
	}

	headersHashes := libcommon.SliceMap(headers, func(header *types.Header) libcommon.Hash {
		return header.Hash()
	})

	// check if headers are linked by ParentHash
	for i, header := range headers[1:] {
		if header.ParentHash != headersHashes[i] {
			return errors.New("canonicalChainBuilderImpl.Connect: invalid headers slice ParentHash")
		}
	}

	// skip existing matching nodes until a new header is found
	for len(headers) > 0 {
		var matchingNode *forkTreeNode
		for _, c := range parent.children {
			if c.headerHash == headersHashes[0] {
				matchingNode = c
				break
			}
		}
		if matchingNode != nil {
			parent = matchingNode
			headers = headers[1:]
			headersHashes = headersHashes[1:]
		} else {
			break
		}
	}

	// if all headers are already inserted
	if len(headers) == 0 {
		return nil
	}

	// attach nodes for the new headers
	for i, header := range headers {
		if (header.Number == nil) && (header.Number.Uint64() != parent.header.Number.Uint64()+1) {
			return errors.New("canonicalChainBuilderImpl.Connect: invalid header.Number")
		}

		// TODO: validate using CalcProducerDelay
		if header.Time <= parent.header.Time {
			return errors.New("canonicalChainBuilderImpl.Connect: invalid header.Time")
		}

		if err := bor.ValidateHeaderExtraField(header.Extra); err != nil {
			return fmt.Errorf("canonicalChainBuilderImpl.Connect: invalid header.Extra %w", err)
		}

		difficulty, err := impl.difficultyCalc.HeaderDifficulty(header)
		if err != nil {
			return fmt.Errorf("canonicalChainBuilderImpl.Connect: header difficulty error %w", err)
		}
		if (header.Difficulty == nil) || (header.Difficulty.Uint64() != difficulty) {
			return &bor.WrongDifficultyError{
				Number:   header.Number.Uint64(),
				Expected: difficulty,
				Actual:   header.Difficulty.Uint64(),
				Signer:   []byte{},
			}
		}

		slot := producerSlotIndex(difficulty)
		if _, ok := parent.children[slot]; ok {
			return errors.New("canonicalChainBuilderImpl.Connect: producer slot is already filled by a different header")
		}

		node := &forkTreeNode{
			parent:   parent,
			children: make(map[producerSlotIndex]*forkTreeNode),

			header:     header,
			headerHash: headersHashes[i],

			totalDifficulty: parent.totalDifficulty + difficulty,
		}

		parent.children[slot] = node
		parent = node
		impl.updateTipIfNeeded(node)
	}

	return nil
}

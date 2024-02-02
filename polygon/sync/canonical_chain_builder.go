package sync

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
)

//go:generate mockgen -destination=./canonical_chain_builder_mock.go -package=sync . CanonicalChainBuilder
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

type canonicalChainBuilder struct {
	root *forkTreeNode
	tip  *forkTreeNode

	difficultyCalc  DifficultyCalculator
	headerValidator HeaderValidator
	spansCache      *SpansCache
}

func NewCanonicalChainBuilder(
	root *types.Header,
	difficultyCalc DifficultyCalculator,
	headerValidator HeaderValidator,
	spansCache *SpansCache,
) CanonicalChainBuilder {
	ccb := &canonicalChainBuilder{
		difficultyCalc:  difficultyCalc,
		headerValidator: headerValidator,
		spansCache:      spansCache,
	}
	ccb.Reset(root)
	return ccb
}

func (ccb *canonicalChainBuilder) Reset(root *types.Header) {
	ccb.root = &forkTreeNode{
		children:   make(map[producerSlotIndex]*forkTreeNode),
		header:     root,
		headerHash: root.Hash(),
	}
	ccb.tip = ccb.root
	if ccb.spansCache != nil {
		ccb.spansCache.Prune(root.Number.Uint64())
	}
}

// depth-first search
func (ccb *canonicalChainBuilder) enumerate(visitFunc func(*forkTreeNode) bool) {
	stack := []*forkTreeNode{ccb.root}
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

func (ccb *canonicalChainBuilder) nodeByHash(hash libcommon.Hash) *forkTreeNode {
	var result *forkTreeNode
	ccb.enumerate(func(node *forkTreeNode) bool {
		if node.headerHash == hash {
			result = node
		}
		return result == nil
	})
	return result
}

func (ccb *canonicalChainBuilder) ContainsHash(hash libcommon.Hash) bool {
	return ccb.nodeByHash(hash) != nil
}

func (ccb *canonicalChainBuilder) Tip() *types.Header {
	return ccb.tip.header
}

func (ccb *canonicalChainBuilder) Headers() []*types.Header {
	var headers []*types.Header
	node := ccb.tip
	for node != nil {
		headers = append(headers, node.header)
		node = node.parent
	}
	libcommon.SliceReverse(headers)
	return headers
}

func (ccb *canonicalChainBuilder) HeadersInRange(start uint64, count uint64) []*types.Header {
	headers := ccb.Headers()
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

func (ccb *canonicalChainBuilder) Prune(newRootNum uint64) error {
	if (newRootNum < ccb.root.header.Number.Uint64()) || (newRootNum > ccb.Tip().Number.Uint64()) {
		return errors.New("canonicalChainBuilder.Prune: newRootNum outside of the canonical chain")
	}

	newRoot := ccb.tip
	for newRoot.header.Number.Uint64() > newRootNum {
		newRoot = newRoot.parent
	}
	ccb.root = newRoot

	if ccb.spansCache != nil {
		ccb.spansCache.Prune(newRootNum)
	}
	return nil
}

// compareForkTreeNodes compares 2 fork tree nodes.
// It returns a positive number if the chain ending at node1 is "better" than the chain ending at node2.
// The better node belongs to the canonical chain, and it has:
// * a greater total difficulty,
// * or a smaller block number,
// * or a lexicographically greater hash.
// See: https://github.com/maticnetwork/bor/blob/master/core/forkchoice.go#L82
func compareForkTreeNodes(node1 *forkTreeNode, node2 *forkTreeNode) int {
	difficultyDiff := int64(node1.totalDifficulty) - int64(node2.totalDifficulty)
	if difficultyDiff != 0 {
		return int(difficultyDiff)
	}
	blockNumDiff := node1.header.Number.Cmp(node2.header.Number)
	if blockNumDiff != 0 {
		return -blockNumDiff
	}
	return bytes.Compare(node1.headerHash.Bytes(), node2.headerHash.Bytes())
}

func (ccb *canonicalChainBuilder) updateTipIfNeeded(tipCandidate *forkTreeNode) {
	if compareForkTreeNodes(tipCandidate, ccb.tip) > 0 {
		ccb.tip = tipCandidate
	}
}

func (ccb *canonicalChainBuilder) Connect(headers []*types.Header) error {
	if (len(headers) > 0) && (headers[0].Number != nil) && (headers[0].Number.Cmp(ccb.root.header.Number) == 0) {
		headers = headers[1:]
	}
	if len(headers) == 0 {
		return nil
	}

	parent := ccb.nodeByHash(headers[0].ParentHash)
	if parent == nil {
		return errors.New("canonicalChainBuilder.Connect: can't connect headers")
	}

	headersHashes := libcommon.SliceMap(headers, func(header *types.Header) libcommon.Hash {
		return header.Hash()
	})

	// check if headers are linked by ParentHash
	for i, header := range headers[1:] {
		if header.ParentHash != headersHashes[i] {
			return errors.New("canonicalChainBuilder.Connect: invalid headers slice ParentHash")
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
		if (header.Number == nil) || (header.Number.Uint64() != parent.header.Number.Uint64()+1) {
			return errors.New("canonicalChainBuilder.Connect: invalid header.Number")
		}

		if ccb.headerValidator != nil {
			if err := ccb.headerValidator.ValidateHeader(header, parent.header, time.Now()); err != nil {
				return fmt.Errorf("canonicalChainBuilder.Connect: invalid header error %w", err)
			}
		}

		difficulty, err := ccb.difficultyCalc.HeaderDifficulty(header)
		if err != nil {
			return fmt.Errorf("canonicalChainBuilder.Connect: header difficulty error %w", err)
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
			return errors.New("canonicalChainBuilder.Connect: producer slot is already filled by a different header")
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
		ccb.updateTipIfNeeded(node)
	}

	return nil
}

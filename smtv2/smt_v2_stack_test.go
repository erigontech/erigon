package smtv2

import (
	"errors"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon/smt/pkg/utils"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/stretchr/testify/assert"
)

var (
	reallyLongLeafFinishingLeft = []int{
		0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0}

	reallyLongLeafFinishingRight = []int{
		0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
		1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1}
)

// MockAccountValueGetter implements AccountValueGetter for testing
type MockInputTapeIterator struct {
	items []InputTapeItem
	index int
}

func (m *MockInputTapeIterator) Next() (InputTapeItem, error) {
	if m.index >= len(m.items) {
		return InputTapeItem{}, ErrNothingFound
	}

	item := m.items[m.index]
	m.index++

	return item, nil
}

func (m *MockInputTapeIterator) Size() int {
	return len(m.items)
}

func (m *MockInputTapeIterator) Processed() int {
	return m.index
}

type dbMockable interface {
	getTapeItems() []InputTapeItem
}

type valueIntersTestInput struct {
	tapeItems                  []InputTapeItem
	expectedRoot               common.Hash
	expectedLeafCount          int
	expectedBranchCount        int
	expectedIntermediateHashes []IntermediateHash
	debug                      bool
}

func (v valueIntersTestInput) getTapeItems() []InputTapeItem {
	return v.tapeItems
}

var valueOnlyTests = map[string]valueIntersTestInput{
	"empty tree": {
		tapeItems:                  []InputTapeItem{},
		expectedRoot:               common.HexToHash("0x0"),
		expectedLeafCount:          0,
		expectedBranchCount:        0,
		expectedIntermediateHashes: []IntermediateHash{},
		debug:                      false,
	},
	"single key left side": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_Value,
				Key:   UintToSmtKey(0),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		expectedRoot:      common.HexToHash("0x42bb2f66296df03552203ae337815976ca9c1bf52cc1bdd59399ede8fea8a822"),
		expectedLeafCount: 2,
		expectedIntermediateHashes: []IntermediateHash{
			// in the case of a single key on the left or right we expect a re-hash of the leaf node
			// so there will be two interhashes for the same key/value pair but with different hashes
			{
				Path:      paddedPathWithRelevantBit([]int{0}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   UintToSmtKey(0),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{0}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   UintToSmtKey(0),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		debug: false,
	},
	"single key right side": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_Value,
				Key:   UintToSmtKey(1),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		expectedRoot:      common.HexToHash("0xb26e0de762d186d2efc35d9ff4388def6c96ec15f942d83d779141386fe1d2e1"),
		expectedLeafCount: 2,
		expectedIntermediateHashes: []IntermediateHash{
			// in the case of a single key on the left or right we expect a re-hash of the leaf node
			// so there will be two interhashes for the same key/value pair but with different hashes
			{
				Path:      paddedPathWithRelevantBit([]int{1}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   UintToSmtKey(1),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{1}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   UintToSmtKey(1),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		debug: false,
	},
	"two keys no depth": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_Value,
				Key:   UintToSmtKey(0),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   UintToSmtKey(1),
				Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
		},
		expectedRoot:        common.HexToHash("0x8500ebd22ea34240552b4656fdc8df9ca878879093b107177357c066c619173"),
		expectedLeafCount:   2,
		expectedBranchCount: 0,
		expectedIntermediateHashes: []IntermediateHash{
			{
				Path:      paddedPathWithRelevantBit([]int{0}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   UintToSmtKey(0),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{1}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   UintToSmtKey(1),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
		},
		debug: false,
	},
	"two keys at level 2 with a branch at level 1": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{0, 0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{0, 1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		expectedRoot:        common.HexToHash("0xf73142cdf0a8d0aaa89eace78f51f95aa2821ab9f1d5ab9e8db3c06cfdd34b16"),
		expectedLeafCount:   2,
		expectedBranchCount: 1,
		expectedIntermediateHashes: []IntermediateHash{
			{
				Path:      paddedPathWithRelevantBit([]int{0, 0}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{0, 0}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{0, 1}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{0, 1}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:     paddedPathWithRelevantBit([]int{0}),
				HashType: IntermediateHashType_Branch,
			},
		},
		debug: false,
	},
	"longer tree on right side": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{1, 1, 1, 0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{1, 1, 1, 1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		expectedRoot:        common.HexToHash("0x6365779af09737a1d3f81a91ff50f09511f1047404b911a1121a7266377503ea"),
		expectedLeafCount:   2,
		expectedBranchCount: 3,
		expectedIntermediateHashes: []IntermediateHash{
			{
				Path:      paddedPathWithRelevantBit([]int{1, 1, 1, 1}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{1, 1, 1, 1}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{1, 1, 1, 0}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{1, 1, 1, 0}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
			{
				Path:     paddedPathWithRelevantBit([]int{1, 1, 1}),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit([]int{1, 1}),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit([]int{1}),
				HashType: IntermediateHashType_Branch,
			},
		},
		debug: false,
	},
	"complex test with left and right branches": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{0, 1, 0, 0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{1, 0, 0, 0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{1, 0, 0, 1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
		},
		expectedRoot:        common.HexToHash("0xd296007aa78a6251c721e5275d315a40bcd93715085dae0c56cae634a996659"),
		expectedLeafCount:   4,
		expectedBranchCount: 6,
		expectedIntermediateHashes: []IntermediateHash{
			{
				Path:      paddedPathWithRelevantBit([]int{0, 1, 0, 0}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{0, 1, 0, 0}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{0, 1, 0, 1}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{0, 1, 0, 1}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{1, 0, 0, 0}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{1, 0, 0, 0}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{1, 0, 0, 1}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{1, 0, 0, 1}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
			{
				Path:     paddedPathWithRelevantBit([]int{0, 1, 0}),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit([]int{0, 1}),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit([]int{0}),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit([]int{1, 0, 0}),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit([]int{1, 0}),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit([]int{1}),
				HashType: IntermediateHashType_Branch,
			},
		},
		debug: false,
	},
	"zig zag tree at full 256 bit depth, with a single node at the right": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath(reallyLongLeafFinishingLeft),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath(reallyLongLeafFinishingRight),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		expectedRoot:        common.HexToHash("0xe3ac4269fb614b636c2f2cc37c8aceb4ddc424647cb2105bf5a3bdf7fb0c79a7"),
		expectedLeafCount:   2,
		expectedBranchCount: 256,
		expectedIntermediateHashes: []IntermediateHash{
			{
				Path:      paddedPathWithRelevantBit(reallyLongLeafFinishingLeft),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath(reallyLongLeafFinishingLeft),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit(reallyLongLeafFinishingRight),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath(reallyLongLeafFinishingRight),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Path:      paddedPathWithRelevantBit([]int{1}),
				HashType:  IntermediateHashType_Value,
				LeafKey:   nodeKeyFromPath([]int{1}),
				LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			// some random branch nodes following the matching paths of the leaf nodes
			{
				Path:     paddedPathWithRelevantBit(reallyLongLeafFinishingLeft[:32]),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit(reallyLongLeafFinishingLeft[:64]),
				HashType: IntermediateHashType_Branch,
			},
			{
				Path:     paddedPathWithRelevantBit(reallyLongLeafFinishingLeft[:96]),
				HashType: IntermediateHashType_Branch,
			},
		},
		debug: false,
	},
}

type interAndValueTestInput struct {
	tapeItems    []InputTapeItem
	expectedRoot common.Hash
	debug        bool
}

func (v interAndValueTestInput) getTapeItems() []InputTapeItem {
	return v.tapeItems
}

var interAndValueTests = map[string]interAndValueTestInput{
	"single IH on the left": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_IntermediateHashValue,
				Path:  paddedPathWithRelevantBit([]int{0}),
				Hash:  utils.HexToKey("0x42bb2f66296df03552203ae337815976ca9c1bf52cc1bdd59399ede8fea8a822"),
				Key:   nodeKeyFromPath([]int{0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		expectedRoot: common.HexToHash("0x42bb2f66296df03552203ae337815976ca9c1bf52cc1bdd59399ede8fea8a822"),
		debug:        false,
	},
	"single IH on the right": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_IntermediateHashValue,
				Path:  paddedPathWithRelevantBit([]int{1}),
				Hash:  utils.HexToKey("0xb26e0de762d186d2efc35d9ff4388def6c96ec15f942d83d779141386fe1d2e1"),
				Key:   nodeKeyFromPath([]int{1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
		},
		expectedRoot: common.HexToHash("0xb26e0de762d186d2efc35d9ff4388def6c96ec15f942d83d779141386fe1d2e1"),
		debug:        false,
	},
	"left tree, value on left IH sibling, right subtree IH": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{0, 1, 0, 0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type:  IntersInputType_IntermediateHashValue,
				Path:  paddedPathWithRelevantBit([]int{0, 1, 0, 1}),
				Hash:  utils.HexToKey("0x42bb2f66296df03552203ae337815976ca9c1bf52cc1bdd59399ede8fea8a822"),
				Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type: IntersInputType_IntermediateHashBranch,
				Path: paddedPathWithRelevantBit([]int{1}),
				Hash: utils.HexToKey("0x22a5985d12c4a602f9c5ecef5fba32cae47299811e00fa68d5d8b9209bf381c3"),
			},
		},
		expectedRoot: common.HexToHash("0xd296007aa78a6251c721e5275d315a40bcd93715085dae0c56cae634a996659"),
		debug:        false,
	},
	"left tree, value on right IH sibling, right subtree IH": {
		tapeItems: []InputTapeItem{
			{
				Type:  IntersInputType_IntermediateHashValue,
				Path:  paddedPathWithRelevantBit([]int{0, 1, 0, 0}),
				Hash:  utils.HexToKey("0x42bb2f66296df03552203ae337815976ca9c1bf52cc1bdd59399ede8fea8a822"),
				Key:   nodeKeyFromPath([]int{0, 1, 0, 0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
			},
			{
				Type: IntersInputType_IntermediateHashBranch,
				Path: paddedPathWithRelevantBit([]int{1}),
				Hash: utils.HexToKey("0x22a5985d12c4a602f9c5ecef5fba32cae47299811e00fa68d5d8b9209bf381c3"),
			},
		},
		expectedRoot: common.HexToHash("0xd296007aa78a6251c721e5275d315a40bcd93715085dae0c56cae634a996659"),
		debug:        false,
	},
	"right tree, value on left IH sibling, left subtree IH": {
		tapeItems: []InputTapeItem{
			{
				Type: IntersInputType_IntermediateHashBranch,
				Path: paddedPathWithRelevantBit([]int{0}),
				Hash: utils.HexToKey("0xbcf9db4b5b745d5169904964f2d537b009f8e7a6d5801bfbc077c03880f8e307"),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{1, 0, 0, 0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
			{
				Type:  IntersInputType_IntermediateHashValue,
				Path:  paddedPathWithRelevantBit([]int{1, 0, 0, 1}),
				Hash:  utils.HexToKey("0xa6c0289125f68e18937b7f127362e62ba7219b1e91392ff8c66461ffff8de5c4"),
				Key:   nodeKeyFromPath([]int{1, 0, 0, 1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
		},
		expectedRoot: common.HexToHash("0xd296007aa78a6251c721e5275d315a40bcd93715085dae0c56cae634a996659"),
		debug:        false,
	},
	"right tree, value on right IH sibling, left subtree IH": {
		tapeItems: []InputTapeItem{
			{
				Type: IntersInputType_IntermediateHashBranch,
				Path: paddedPathWithRelevantBit([]int{0}),
				Hash: utils.HexToKey("0xbcf9db4b5b745d5169904964f2d537b009f8e7a6d5801bfbc077c03880f8e307"),
			},
			{
				Type:  IntersInputType_IntermediateHashValue,
				Path:  paddedPathWithRelevantBit([]int{1, 0, 0, 0}),
				Hash:  utils.HexToKey("0xa6c0289125f68e18937b7f127362e62ba7219b1e91392ff8c66461ffff8de5c4"),
				Key:   nodeKeyFromPath([]int{1, 0, 0, 0}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
			{
				Type:  IntersInputType_Value,
				Key:   nodeKeyFromPath([]int{1, 0, 0, 1}),
				Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
			},
		},
		expectedRoot: common.HexToHash("0xd296007aa78a6251c721e5275d315a40bcd93715085dae0c56cae634a996659"),
		debug:        false,
	},
}

func prepareMockDB(tt dbMockable) *MockInputTapeIterator {
	mockDB := &MockInputTapeIterator{}
	mockDB.items = append(mockDB.items, tt.getTapeItems()...)
	return mockDB
}

func nodeKeyFromPath(path []int) SmtKey {
	// first pad out the path to 256 bits
	for len(path) < 256 {
		path = append(path, 0)
	}

	result, err := SmtKeyFromPath(path)
	if err != nil {
		panic(err)
	}

	return result
}

func paddedPathWithRelevantBit(path []int) [257]int {
	result := [257]int{}

	copy(result[:], path[:])

	// now pad out the rest of the path to 256 bits
	for i := len(path); i < 256; i++ {
		result[i] = 0
	}

	// then add the relevant bit to the end
	result[256] = len(path)

	return result
}

type MockIntermediateHashCollector struct {
	hashes []*IntermediateHash
}

func (m *MockIntermediateHashCollector) AddIntermediateHash(hash *IntermediateHash) error {
	m.hashes = append(m.hashes, hash)
	return nil
}

func TestRegenerateRoot(t *testing.T) {
	for name, tt := range valueOnlyTests {
		t.Run(name, func(t *testing.T) {
			mockDB := prepareMockDB(tt)

			hasher := NewSmtStackHasher(mockDB, nil)
			hasher.SetDebug(tt.debug)

			root, _, _, err := hasher.RegenerateRoot(name, 10*time.Second)

			rootHash := common.BigToHash(root)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRoot, rootHash)
		})
	}
}

func TestRegenerateRoot_WithIntermediateHashes(t *testing.T) {
	for name, tt := range interAndValueTests {
		t.Run(name, func(t *testing.T) {
			mockDB := prepareMockDB(tt)
			hasher := NewSmtStackHasher(mockDB, nil)
			hasher.SetDebug(tt.debug)

			root, _, _, err := hasher.RegenerateRoot(name, 10*time.Second)

			rootHash := common.BigToHash(root)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRoot, rootHash)
		})
	}
}

func TestIntermediateHash_GetsCollected(t *testing.T) {
	for name, tt := range valueOnlyTests {
		collector := &MockIntermediateHashCollector{}
		t.Run(name, func(t *testing.T) {
			mockDB := prepareMockDB(tt)

			hasher := NewSmtStackHasher(mockDB, collector)
			hasher.SetDebug(tt.debug)

			_, _, _, err := hasher.RegenerateRoot(name, 10*time.Second)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedLeafCount+tt.expectedBranchCount, len(collector.hashes))

			if len(tt.expectedIntermediateHashes) > 0 {
				for _, expectedHash := range tt.expectedIntermediateHashes {
					found := false
					isBranch := expectedHash.HashType == IntermediateHashType_Branch
					for _, testHash := range collector.hashes {
						pathExactMatch := cmp.Equal(testHash.Path, expectedHash.Path)
						hashTypeMatch := cmp.Equal(testHash.HashType, expectedHash.HashType)
						leafKeyMatch := cmp.Equal(testHash.LeafKey, expectedHash.LeafKey)
						leafValueMatch := cmp.Equal(testHash.LeafValue, expectedHash.LeafValue, cmp.AllowUnexported(big.Int{}))

						if isBranch {
							// no key or value for a branch node
							if pathExactMatch && hashTypeMatch {
								found = true
								break
							}
						} else {
							if pathExactMatch && hashTypeMatch && leafKeyMatch && leafValueMatch {
								found = true
								break
							}
						}

					}
					assert.True(t, found, "isBranch: %v, hashType: %v, path: %v", isBranch, expectedHash.HashType, expectedHash.Path)
				}
			}
		})
	}
}

func TestIntermediateHash_SerialiseDeserialise_Value(t *testing.T) {
	path := [257]int{0, 0, 1, 1, 1, 1, 1, 0, 00, 1, 1, 1, 1, 1, 1, 1, 0, 0}
	hash := [4]uint64{1, 2, 3, 4}
	value := SmtValue8{1, 2, 3, 4, 5, 6, 7, 8}
	hashType := IntermediateHashType_Value
	key := SmtKey{100, 200, 300, 400}
	intermediateHash := &IntermediateHash{Path: path, HashType: hashType, Hash: hash, LeafKey: key, LeafValue: value}

	serialised := intermediateHash.Serialise()

	deserialised := DeserialiseIntermediateHash(serialised)
	assert.Equal(t, intermediateHash, deserialised)
}

func TestIntermediateHash_SerialiseDeserialise_Branch(t *testing.T) {
	path := [257]int{0, 0, 1, 1, 1, 1, 1, 0, 00, 1, 1, 1, 1, 1, 1, 1, 0, 0}
	hash := [4]uint64{1, 2, 3, 4}
	hashType := IntermediateHashType_Branch
	intermediateHash := &IntermediateHash{Path: path, HashType: hashType, Hash: hash}

	serialised := intermediateHash.Serialise()

	deserialised := DeserialiseIntermediateHash(serialised)
	assert.Equal(t, intermediateHash, deserialised)
}

func TestIntermediateHash_SerialiseDeserialise_OldBranch(t *testing.T) {
	path := [257]int{0, 0, 1, 1, 1, 1, 1, 0, 00, 1, 1, 1, 1, 1, 1, 1, 0, 0}
	hash := [4]uint64{1, 2, 3, 4}
	hashType := IntermediateHashType_Branch
	intermediateHash := &IntermediateHash{Path: path, HashType: hashType, Hash: hash}

	serialised := intermediateHash.Serialise()

	// now trim off the leaf key and value to simulate the older format for a branch node
	serialised = serialised[:290]

	deserialised := DeserialiseIntermediateHash(serialised)
	assert.Equal(t, intermediateHash, deserialised)
}

func TestEtlInputTapeIterator(t *testing.T) {
	collector := etl.NewCollector("", os.TempDir(), etl.NewSortableBuffer(etl.BufferOptimalSize), log.New())
	defer collector.Close()

	key0 := nodeKeyFromPath([]int{0, 0, 0})
	key1 := nodeKeyFromPath([]int{0, 0, 1})
	key2 := nodeKeyFromPath([]int{1, 0, 0})

	val0 := SmtValue8{0, 0, 0, 0, 0, 0, 0, 1}
	val1 := SmtValue8{0, 0, 0, 0, 0, 0, 0, 2}
	val2 := SmtValue8{0, 0, 0, 0, 0, 0, 0, 3}

	path0 := []byte{}
	for _, b := range key0.GetPath() {
		path0 = append(path0, byte(b))
	}

	path1 := []byte{}
	for _, b := range key1.GetPath() {
		path1 = append(path1, byte(b))
	}

	path2 := []byte{}
	for _, b := range key2.GetPath() {
		path2 = append(path2, byte(b))
	}

	collector.Collect(path0, val0.ToBytes())
	collector.Collect(path1, val1.ToBytes())
	collector.Collect(path2, val2.ToBytes())

	expectedItems := []InputTapeItem{
		{
			Type:  IntersInputType_Value,
			Key:   key0,
			Value: val0,
		},
		{
			Type:  IntersInputType_Value,
			Key:   key1,
			Value: val1,
		},
		{
			Type:  IntersInputType_Value,
			Key:   key2,
			Value: val2,
		},
	}

	iterator := NewEtlInputTapeIterator(collector, 3)
	iterator.Start()

	total := 0
	reachedTerminatorError := false
	for {
		item, err := iterator.Next()
		if err != nil {
			if errors.Is(err, ErrNothingFound) {
				reachedTerminatorError = true
				break
			}
			t.Fatalf("error: %v", err)
		}

		assert.Equal(t, expectedItems[total].Type, item.Type)
		assert.Equal(t, expectedItems[total].Key, item.Key)
		assert.Equal(t, expectedItems[total].Value, item.Value)
		total++
	}

	assert.Equal(t, len(expectedItems), total)
	assert.True(t, reachedTerminatorError)

}

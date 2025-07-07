package smtv2

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/erigontech/erigon/smt/pkg/utils"
	"gotest.tools/v3/assert"
)

type tapeTestInput struct {
	changeSet   []ChangeSetEntry
	interhashes []IntermediateHash
	expected    []InputTapeItem
	errExpected error
	debug       bool
}

type mockInterhashTape struct {
	items []IntermediateHash
	index int
}

func (m *mockInterhashTape) Next() (IntermediateHash, error) {
	toReturn := m.items[m.index]
	m.index++
	return toReturn, nil
}

func (m *mockInterhashTape) Current() IntermediateHash {
	return m.items[m.index]
}

func (m *mockInterhashTape) Size() (uint64, error) {
	return uint64(len(m.items)), nil
}

func (m *mockInterhashTape) FastForward(prefix []byte) (bool, error) {
	if m.index < len(m.items) {
		m.index++
	}

	return m.index < len(m.items), nil
}

func TestTapesProcessor_Terminator(t *testing.T) {
	tests := map[string]tapeTestInput{
		"IH0 with CS_TT = IH0": {
			changeSet: []ChangeSetEntry{},
			interhashes: []IntermediateHash{
				{
					HashType: IntermediateHashType_Branch,
					Path:     paddedPathWithRelevantBit([]int{0}),
					Hash:     utils.HexToKey("0x0"),
				},
			},
			expected: []InputTapeItem{
				{
					Type: IntersInputType_IntermediateHashBranch,
					Path: paddedPathWithRelevantBit([]int{0}),
					Hash: utils.HexToKey("0x0"),
				},
			},
		},
		"IHv0 with CS_TT = V0": {
			changeSet: []ChangeSetEntry{},
			interhashes: []IntermediateHash{
				{
					HashType:  IntermediateHashType_Value,
					Path:      paddedPathWithRelevantBit([]int{0}),
					Hash:      utils.HexToKey("0x0"),
					LeafKey:   nodeKeyFromPath([]int{0}),
					LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			expected: []InputTapeItem{
				{
					Type:  IntersInputType_Value,
					Key:   nodeKeyFromPath([]int{0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"IH1 with CS_TT = IH1": {
			changeSet: []ChangeSetEntry{},
			interhashes: []IntermediateHash{
				{
					HashType: IntermediateHashType_Branch,
					Path:     paddedPathWithRelevantBit([]int{1}),
					Hash:     utils.HexToKey("0x0"),
				},
			},
			expected: []InputTapeItem{
				{
					Type: IntersInputType_IntermediateHashBranch,
					Path: paddedPathWithRelevantBit([]int{1}),
					Hash: utils.HexToKey("0x0"),
				},
			},
		},
		"IHv1 with CS_TT = V1": {
			changeSet: []ChangeSetEntry{},
			interhashes: []IntermediateHash{
				{
					HashType:  IntermediateHashType_Value,
					Path:      paddedPathWithRelevantBit([]int{1}),
					Hash:      utils.HexToKey("0x0"),
					LeafKey:   nodeKeyFromPath([]int{1}),
					LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			expected: []InputTapeItem{
				{
					Type:  IntersInputType_Value,
					Key:   nodeKeyFromPath([]int{1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"IH0 IH1 with CS_TT = IH0 IH1": {
			changeSet: []ChangeSetEntry{},
			interhashes: []IntermediateHash{
				{
					HashType: IntermediateHashType_Branch,
					Path:     paddedPathWithRelevantBit([]int{0}),
					Hash:     utils.HexToKey("0x0"),
				},
				{
					HashType: IntermediateHashType_Branch,
					Path:     paddedPathWithRelevantBit([]int{1}),
					Hash:     utils.HexToKey("0x1"),
				},
			},
			expected: []InputTapeItem{
				{
					Type: IntersInputType_IntermediateHashBranch,
					Path: paddedPathWithRelevantBit([]int{0}),
					Hash: utils.HexToKey("0x0"),
				},
				{
					Type: IntersInputType_IntermediateHashBranch,
					Path: paddedPathWithRelevantBit([]int{1}),
					Hash: utils.HexToKey("0x1"),
				},
			},
		},
		"IHv0 IH1 with CS_TT = v0 IH1": {
			changeSet: []ChangeSetEntry{},
			interhashes: []IntermediateHash{
				{
					HashType:  IntermediateHashType_Value,
					Path:      paddedPathWithRelevantBit([]int{0}),
					Hash:      utils.HexToKey("0x1"),
					LeafKey:   nodeKeyFromPath([]int{0}),
					LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					HashType: IntermediateHashType_Branch,
					Path:     paddedPathWithRelevantBit([]int{1}),
					Hash:     utils.HexToKey("0x2"),
				},
			},
			expected: []InputTapeItem{
				{
					Type:  IntersInputType_Value,
					Key:   nodeKeyFromPath([]int{0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type: IntersInputType_IntermediateHashBranch,
					Path: paddedPathWithRelevantBit([]int{1}),
					Hash: utils.HexToKey("0x2"),
				},
			},
		},
		"IH0 IHv1 with CS_TT = IH0 V1": {
			changeSet: []ChangeSetEntry{},
			interhashes: []IntermediateHash{
				{
					HashType: IntermediateHashType_Branch,
					Path:     paddedPathWithRelevantBit([]int{0}),
					Hash:     utils.HexToKey("0x2"),
				},
				{
					HashType:  IntermediateHashType_Value,
					Path:      paddedPathWithRelevantBit([]int{1}),
					Hash:      utils.HexToKey("0x1"),
					LeafKey:   nodeKeyFromPath([]int{1}),
					LeafValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			expected: []InputTapeItem{
				{
					Type: IntersInputType_IntermediateHashBranch,
					Path: paddedPathWithRelevantBit([]int{0}),
					Hash: utils.HexToKey("0x2"),
				},
				{
					Type:  IntersInputType_Value,
					Key:   nodeKeyFromPath([]int{1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"[ IH0 IHv00 IHv01 IH1 IHv10 IHv11 ] [ CS_TT ] => IH0 IH1": {
			changeSet: []ChangeSetEntry{},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0}, big.NewInt(2)),
				makeIHValue([]int{0, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
				makeIHValue([]int{1, 0}, big.NewInt(3)),
				makeIHValue([]int{1, 1}, big.NewInt(4)),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0}, "0x1"),
				makeTapeBranch([]int{1}, "0x3"),
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			performTest(t, test)
		})
	}
}

func TestTapesProcessor_Add(t *testing.T) {
	tests := map[string]tapeTestInput{
		"add to empty tree": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{1, 0, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
			interhashes: []IntermediateHash{},
			expected: []InputTapeItem{
				makeTapeValue([]int{1, 0, 0, 1}, big.NewInt(2)),
			},
		},
		"[ IH11 ] [CS_ADD 00] => V00 IH11": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{1, 1}, "0x1"),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 0}, big.NewInt(2)),
				makeTapeBranch([]int{1, 1}, "0x1"),
			},
		},
		"[ IH00 ] [ CS_ADD 11 ] => IH00 V11": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{1, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0, 0}, "0x1"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0, 0}, "0x1"),
				makeTapeValue([]int{1, 1}, big.NewInt(2)),
			},
		},
		"[ IH0 IH00 ] [ CS_ADD 11 ] => IH0 V11": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{1, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHBranch([]int{0, 0}, "0x1"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0}, "0x1"),
				makeTapeValue([]int{1, 1}, big.NewInt(2)),
			},
		},
		"[ IH00 IH1 ] [ CS_ADD 01 ] => IH00 V01 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHBranch([]int{1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0, 0}, "0x1"),
				makeTapeValue([]int{0, 1}, big.NewInt(1)),
				makeTapeBranch([]int{1}, "0x2"),
			},
		},
		"[ IH01 IH1 ] [ CS_ADD 00 ] => V00 IH01 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0, 1}, "0x1"),
				makeIHBranch([]int{1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 0}, big.NewInt(1)),
				makeTapeBranch([]int{0, 1}, "0x1"),
				makeTapeBranch([]int{1}, "0x2"),
			},
		},
		"[IH00 IH11] [ ADD 01 ] => IH00 V01 IH11": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHBranch([]int{1, 1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0, 0}, "0x1"),
				makeTapeValue([]int{0, 1}, big.NewInt(1)),
				makeTapeBranch([]int{1, 1}, "0x2"),
			},
		},
		"[ IH0 IH00 IH01 ] [ADD 11] => [IH0 V11]": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{1, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHBranch([]int{0, 0}, "0x2"),
				makeIHBranch([]int{0, 1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0}, "0x1"),
				makeTapeValue([]int{1, 1}, big.NewInt(1)),
			},
		},
		"[ IHv00000000 IH1 ] [ ADD 00000001 ] => V00000000 V00000001 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 0, 0, 0, 0, 0, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHValue([]int{0, 0, 0, 0, 0, 0, 0, 0}, big.NewInt(0)),
				makeIHBranch([]int{1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 0, 0, 0, 0, 0, 0, 0}, big.NewInt(0)),
				makeTapeValue([]int{0, 0, 0, 0, 0, 0, 0, 1}, big.NewInt(1)),
				makeTapeBranch([]int{1}, "0x2"),
			},
		},
		"[ IHv01000001 IH1 ] [ ADD 01000000 ] => V01000000 V01000001 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 0, 0, 0, 0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(0)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHValue([]int{0, 1, 0, 0, 0, 0, 0, 1}, big.NewInt(1)),
				makeIHBranch([]int{1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 1, 0, 0, 0, 0, 0, 0}, big.NewInt(0)),
				makeTapeValue([]int{0, 1, 0, 0, 0, 0, 0, 1}, big.NewInt(1)),
				makeTapeBranch([]int{1}, "0x2"),
			},
		},
		"[ IHv01000010 IH1 ] [ ADD 01000000 ADD 01000001 ] => V01000000 V01000001 V01000010 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 0, 0, 0, 0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 0, 0, 0, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHValue([]int{0, 1, 0, 0, 0, 0, 1, 0}, big.NewInt(3)),
				makeIHBranch([]int{1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 1, 0, 0, 0, 0, 0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 1, 0, 0, 0, 0, 0, 1}, big.NewInt(2)),
				makeTapeValue([]int{0, 1, 0, 0, 0, 0, 1, 0}, big.NewInt(3)),
				makeTapeBranch([]int{1}, "0x2"),
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			performTest(t, test)
		})
	}
}

func TestTapesProcessor_Change(t *testing.T) {
	tests := map[string]tapeTestInput{
		"[ IH00 IH11 ] [ CHANGE 0101 ] => assert! (nothing found at 01)": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHBranch([]int{1, 1}, "0x2"),
			},
			expected:    []InputTapeItem{},
			errExpected: ErrOvershoot,
		},
		"[ IHv01 IH11] [ CHANGE 0101 ] => V0101 IH11": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHValue([]int{0, 1}, big.NewInt(1)),
				makeIHBranch([]int{1, 1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 1, 0, 1}, big.NewInt(1)),
				makeTapeBranch([]int{1, 1}, "0x2"),
			},
		},
		"[ IH00 IHv01 IH1 ] [ CHANGE 0101 ] => IH00 V0101 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHValue([]int{0, 1}, big.NewInt(1)),
				makeIHBranch([]int{1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0, 0}, "0x1"),
				makeTapeValue([]int{0, 1, 0, 1}, big.NewInt(1)),
				makeTapeBranch([]int{1}, "0x2"),
			},
		},
		"[ IH00 IHv01 IHv10 IH11 ] [ CHANGE 0101 CHANGE 1010] => IH00 V0101 V1010 IH11": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{1, 0, 1, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHValue([]int{0, 1}, big.NewInt(1)),
				makeIHValue([]int{1, 0}, big.NewInt(2)),
				makeIHBranch([]int{1, 1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0, 0}, "0x1"),
				makeTapeValue([]int{0, 1, 0, 1}, big.NewInt(1)),
				makeTapeValue([]int{1, 0, 1, 0}, big.NewInt(2)),
				makeTapeBranch([]int{1, 1}, "0x2"),
			},
		},
		"[ IH00 IH01 IH1 ] [ CHANGE 0101 ] — assert (overshoot)": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHBranch([]int{0, 1}, "0x2"),
				makeIHBranch([]int{1}, "0x3"),
			},
			expected:    []InputTapeItem{},
			errExpected: ErrOvershoot,
		},
		"[ IH10 ] [ CHANGE 0101 ] — assert (overshoot)": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{1, 0}, "0x1"),
			},
			expected:    []InputTapeItem{},
			errExpected: ErrOvershoot,
		},
		"[ IH0 IH00 IH01 IHv010 IHv011 IH1] [CHANGE 011] => IH00 V010 V011 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHBranch([]int{0, 1}, "0x1"),
				makeIHValue([]int{0, 1, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x2"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0, 0}, "0x1"),
				makeTapeValue([]int{0, 1, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 1, 1}, big.NewInt(1)), // element being changed
				makeTapeBranch([]int{1}, "0x2"),
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			performTest(t, test)
		})
	}
}

func TestTapesProcessor_Delete(t *testing.T) {
	tests := map[string]tapeTestInput{
		"[ IH0 IHv00 IHv01 ] [ DELETE 00 ] => V01": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 0}),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1}, big.NewInt(2)),
			},
			expected: []InputTapeItem{
				makeTapeDeletedInterhash([]int{0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 1}, big.NewInt(2)),
			},
		},
		"[ IH0 IHv00 IHv01 ] [ DELETE 01 ] => V00": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 1}),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1}, big.NewInt(2)),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 0}, big.NewInt(1)),
				makeTapeDeletedInterhash([]int{0, 1}, big.NewInt(2)),
			},
		},
		"[ IH0 IHv00 IH01 IHv010 IH1 ] [ DELETE 011 ] => assert (overshoot)": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 1, 1}),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0}, big.NewInt(1)),
				makeIHBranch([]int{0, 1}, "0x2"),
				makeIHValue([]int{0, 1, 0}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
			},
			expected:    []InputTapeItem{},
			errExpected: ErrNoHandlerForDelete,
		},
		"[ IH0 IHv00 IH01 IHv010 IHv011 IH1 ] [ DELETE 011 ] => V00 V010 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 1, 1}),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0}, big.NewInt(1)),
				makeIHBranch([]int{0, 1}, "0x2"),
				makeIHValue([]int{0, 1, 0}, big.NewInt(2)),
				makeIHValue([]int{0, 1, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 1, 0}, big.NewInt(2)),
				makeTapeDeletedInterhash([]int{0, 1, 1}, big.NewInt(2)),
				makeTapeBranch([]int{1}, "0x3"),
			},
			errExpected: nil,
		},
		"[ IH0 IH00 IH01 IHv010 IHv011 IH1 ] [ DELETE 011 ] => IH00, V010, IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 1, 1}),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHBranch([]int{0, 1}, "0x2"),
				makeIHValue([]int{0, 1, 0}, big.NewInt(2)),
				makeIHValue([]int{0, 1, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0, 0}, "0x1"),
				makeTapeValue([]int{0, 1, 0}, big.NewInt(2)),
				makeTapeDeletedInterhash([]int{0, 1, 1}, big.NewInt(2)),
				makeTapeBranch([]int{1}, "0x3"),
			},
			errExpected: nil,
		},
		"[ IH0 IH00 IHv000 IHv001 IH1 IHv10 IHv11 ] [ DELETE 10 ] => IH0 V11": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{1, 0}),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHValue([]int{0, 0, 0}, big.NewInt(2)),
				makeIHValue([]int{0, 0, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
				makeIHValue([]int{1, 0}, big.NewInt(3)),
				makeIHValue([]int{1, 1}, big.NewInt(4)),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0}, "0x1"),
				makeTapeDeletedInterhash([]int{1, 0}, big.NewInt(3)),
				makeTapeValue([]int{1, 1}, big.NewInt(4)),
			},
			errExpected: nil,
		},
		"[ IH0 IH00 IHv000 IHv001 IH1 IHv10 IHv11 ] [ DELETE 11 ] => IH0 V10": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{1, 1}),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHBranch([]int{0, 0}, "0x1"),
				makeIHValue([]int{0, 0, 0}, big.NewInt(2)),
				makeIHValue([]int{0, 0, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
				makeIHValue([]int{1, 0}, big.NewInt(3)),
				makeIHValue([]int{1, 1}, big.NewInt(4)),
			},
			expected: []InputTapeItem{
				makeTapeBranch([]int{0}, "0x1"),
				makeTapeValue([]int{1, 0}, big.NewInt(3)),
				makeTapeDeletedInterhash([]int{1, 1}, big.NewInt(4)),
			},
			errExpected: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			performTest(t, test)
		})
	}
}

func TestTapesProcessor_PickNMix(t *testing.T) {
	tests := map[string]tapeTestInput{
		"[ IH0 IHv00 IHv01 IH1 IHv10 IHv11 ]  + [ CS_DELETE 0000 CS_CHANGE 0100 ] => V0100(new) IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 0, 0, 0}),
				},
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(99)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
				makeIHValue([]int{1, 0}, big.NewInt(3)),
				makeIHValue([]int{1, 1}, big.NewInt(4)),
			},
			expected: []InputTapeItem{
				makeTapeDeletedInterhash([]int{0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 1, 0, 0}, big.NewInt(99)),
				makeTapeBranch([]int{1}, "0x3"),
			},
			debug: false,
		},
		"[ IH0 IHv0000 IHv0100 IH1 IHv1000 IHv1100 ] + [ CS_DELETE 0000 CS_ADD 0010 CS_CHANGE 0100 ] => V0010 V0100(new) IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 0, 0, 0}),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 0, 1, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1001)),
				},
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1002)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0, 0, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1, 0, 0}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
				makeIHValue([]int{1, 0, 0, 0}, big.NewInt(3)),
				makeIHValue([]int{1, 1, 0, 0}, big.NewInt(4)),
			},
			expected: []InputTapeItem{
				makeTapeDeletedInterhash([]int{0, 0, 0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 0, 1, 0}, big.NewInt(1001)),
				makeTapeValue([]int{0, 1, 0, 0}, big.NewInt(1002)),
				makeTapeBranch([]int{1}, "0x3"),
			},
			debug: false,
		},
		"[ IH0 IHv0000 IHv0100 IH1 IHv1000 IHv1100 ] + [ CS_DELETE 0000 CS_CHANGE 0100 CS_ADD 0101 ] => V0100(new) V0101 IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 0, 0, 0}),
				},
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1001)),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1002)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0, 0, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1, 0, 0}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
				makeIHValue([]int{1, 0, 0, 0}, big.NewInt(3)),
				makeIHValue([]int{1, 1, 0, 0}, big.NewInt(4)),
			},
			expected: []InputTapeItem{
				makeTapeDeletedInterhash([]int{0, 0, 0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 1, 0, 0}, big.NewInt(1001)),
				makeTapeValue([]int{0, 1, 0, 1}, big.NewInt(1002)),
				makeTapeBranch([]int{1}, "0x3"),
			},
			debug: false,
		},
		"[ IH0 IHv0000 IHv0101 IH1 IHv1000 IHv1100] + [ CS_DELETE 0000 CS_ADD 0100 CS_CHANGE 0101 ] => V0100 V0101(new) IH1": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 0, 0, 0}),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1001)),
				},
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{0, 1, 0, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1002)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0, 0, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1, 0, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
				makeIHValue([]int{1, 0, 0, 0}, big.NewInt(3)),
				makeIHValue([]int{1, 1, 0, 0}, big.NewInt(4)),
			},
			expected: []InputTapeItem{
				makeTapeDeletedInterhash([]int{0, 0, 0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 1, 0, 0}, big.NewInt(1001)),
				makeTapeValue([]int{0, 1, 0, 1}, big.NewInt(1002)),
				makeTapeBranch([]int{1}, "0x3"),
			},
			debug: false,
		},
		"[ IH0 IHv00 IHv01 IH1 IHv10 IHv11 ] + [ CS_DELETE 00 CS_CHANGE 11 ] => V01 V10 V11(new)": {
			changeSet: []ChangeSetEntry{
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 0}),
				},
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{1, 1}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1001)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1}, big.NewInt(2)),
				makeIHBranch([]int{1}, "0x3"),
				makeIHValue([]int{1, 0}, big.NewInt(3)),
				makeIHValue([]int{1, 1}, big.NewInt(4)),
			},
			expected: []InputTapeItem{
				makeTapeDeletedInterhash([]int{0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 1}, big.NewInt(2)),
				makeTapeValue([]int{1, 0}, big.NewInt(3)),
				makeTapeValue([]int{1, 1}, big.NewInt(1001)),
			},
			debug: false,
		},
		"[ IH0 IHv0000 IHv0100 IHv1000 IH11 IHv1100 IHv1110 ] + [ CS_ADD 0010 CS_DELETE 0100 CS_CHANGE 1000 ] => V0000 V0010 V1000 IH11": {
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   nodeKeyFromPath([]int{0, 0, 1, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1001)),
				},
				{
					Type: ChangeSetEntryType_Delete,
					Key:  nodeKeyFromPath([]int{0, 1, 0, 0}),
				},
				{
					Type:  ChangeSetEntryType_Change,
					Key:   nodeKeyFromPath([]int{1, 0, 0, 0}),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1002)),
				},
			},
			interhashes: []IntermediateHash{
				makeIHBranch([]int{0}, "0x1"),
				makeIHValue([]int{0, 0, 0, 0}, big.NewInt(1)),
				makeIHValue([]int{0, 1, 0, 0}, big.NewInt(2)),
				makeIHValue([]int{1, 0, 0, 0}, big.NewInt(3)),
				makeIHBranch([]int{1, 1}, "0x3"),
				makeIHValue([]int{1, 1, 0, 0}, big.NewInt(4)),
				makeIHValue([]int{1, 1, 1, 0}, big.NewInt(5)),
			},
			expected: []InputTapeItem{
				makeTapeValue([]int{0, 0, 0, 0}, big.NewInt(1)),
				makeTapeValue([]int{0, 0, 1, 0}, big.NewInt(1001)),
				makeTapeDeletedInterhash([]int{0, 1, 0, 0}, big.NewInt(2)),
				makeTapeValue([]int{1, 0, 0, 0}, big.NewInt(1002)),
				makeTapeBranch([]int{1, 1}, "0x3"),
			},
			debug: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			performTest(t, test)
		})
	}
}

func performTest(t *testing.T, test tapeTestInput) {
	changeSetTape := NewChangeSetTape()
	for _, entry := range test.changeSet {
		changeSetTape.Add(entry.Type, entry.Key, entry.Value, entry.OriginalValue)
	}

	changeSetTape.Add(ChangeSetEntryType_Terminator, nodeKeyFromPath([]int{}), ScalarToSmtValue8FromBits(big.NewInt(0)), ScalarToSmtValue8FromBits(big.NewInt(0)))

	interhashTape := &mockInterhashTape{
		items: test.interhashes,
	}

	// always add a terminator to the test case so it can finish
	interhashTape.items = append(interhashTape.items, IntermediateHash{
		HashType: IntermediateHashType_Terminator,
		Path:     terminatorPath,
		Hash:     utils.HexToKey("0x0"),
	})

	tapesProcessor, err := NewTapesProcessor(changeSetTape, interhashTape, test.debug)
	if err != nil {
		t.Fatalf("error creating tapes processor: %v", err)
	}
	tapesProcessor.SetDebug(test.debug)

	outputs := []InputTapeItem{}

	for {
		item, err := tapesProcessor.Next()
		if err != nil {
			if errors.Is(err, ErrNothingFound) {
				break
			}
			if test.errExpected != nil && !errors.Is(err, test.errExpected) {
				t.Fatalf("expected error %v, got %v", test.errExpected, err)
			}
			// seen the error we expected so break the loop
			return
		}
		outputs = append(outputs, item)
	}

	assert.DeepEqual(t, outputs, test.expected, bigIntComparer)
}

func makeIHBranch(path []int, hash string) IntermediateHash {
	return IntermediateHash{
		HashType: IntermediateHashType_Branch,
		Path:     paddedPathWithRelevantBit(path),
		Hash:     utils.HexToKey(hash),
	}
}

func makeIHValue(key []int, value *big.Int) IntermediateHash {
	return IntermediateHash{
		HashType:  IntermediateHashType_Value,
		Path:      paddedPathWithRelevantBit(key),
		LeafKey:   nodeKeyFromPath(key),
		LeafValue: ScalarToSmtValue8FromBits(value),
	}
}

func makeTapeBranch(path []int, hash string) InputTapeItem {
	return InputTapeItem{
		Type: IntersInputType_IntermediateHashBranch,
		Path: paddedPathWithRelevantBit(path),
		Hash: utils.HexToKey(hash),
	}
}

func makeTapeValue(key []int, value *big.Int) InputTapeItem {
	return InputTapeItem{
		Type:  IntersInputType_Value,
		Key:   nodeKeyFromPath(key),
		Value: ScalarToSmtValue8FromBits(value),
	}
}

func makeTapeDeletedInterhash(key []int, value *big.Int) InputTapeItem {
	path := [257]int{}
	copy(path[:], key)
	path[256] = len(key)
	return InputTapeItem{
		Type:  IntersInputType_DeletedIntermediateHash,
		Path:  path,
		Key:   nodeKeyFromPath(key),
		Hash:  utils.HexToKey("0x0"),
		Value: ScalarToSmtValue8FromBits(value),
	}
}

func makeInputTape(inputs ...string) []InputTapeItem {
	result := []InputTapeItem{}
	for _, input := range inputs {
		parts := strings.Split(input, "-")
		if len(parts) != 2 {
			panic(fmt.Sprintf("invalid input %s", input))
		}

		key := binaryStringToNodeKey(parts[0])
		value := parts[1]
		value = strings.TrimPrefix(value, "0x")
		val, ok := new(big.Int).SetString(value, 16)
		if !ok {
			panic(fmt.Sprintf("invalid hex value %s", value))
		}

		result = append(result, makeTapeValue(key, val))
	}
	return result

}

func makeTapeValueHex(key []int, value string) InputTapeItem {
	value = strings.TrimPrefix(value, "0x")
	val, ok := new(big.Int).SetString(value, 16)
	if !ok {
		panic(fmt.Sprintf("invalid hex value %s", value))
	}
	return InputTapeItem{
		Type:  IntersInputType_Value,
		Key:   nodeKeyFromPath(key),
		Value: ScalarToSmtValue8FromBits(val),
	}
}

// Create custom comparer for big.Int values
var bigIntComparer = cmp.Comparer(func(x, y *big.Int) bool {
	if x == nil || y == nil {
		return x == y
	}
	return x.Cmp(y) == 0
})

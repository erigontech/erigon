package trie

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func generateOperands() []WitnessOperator {
	return []WitnessOperator{
		&OperatorBranch{Mask: 0xFF},
		&OperatorEmptyRoot{},
		&OperatorCode{[]byte("code-operand-1")},
		&OperatorExtension{[]byte{5, 5, 4, 6, 6, 6}},
		&OperatorLeafValue{[]byte{5, 5, 4, 3, 2, 1}, []byte("leaf-value-value-1")},
		&OperatorHash{libcommon.HexToHash("0xabcabcabcabc")},
		&OperatorLeafAccount{
			[]byte{2, 2, 4, 5, 6},
			999,
			big.NewInt(552),
			true,
			false,
			10,
		},
		&OperatorLeafAccount{
			[]byte{2, 2, 4, 5, 7},
			757,
			big.NewInt(334),
			true,
			true,
			11,
		},
		&OperatorLeafAccount{
			[]byte{2, 2, 4, 5, 8},
			333,
			big.NewInt(11112),
			false,
			false,
			12,
		},
		&OperatorLeafAccount{
			[]byte{2, 2, 4, 5, 9},
			0,
			big.NewInt(0),
			false,
			false,
			13,
		},
	}
}

func witnessesEqual(w1, w2 *Witness) bool {
	if w1 == nil {
		return w2 == nil
	}

	if w2 == nil {
		return w1 == nil
	}

	var buff bytes.Buffer

	w1.WriteDiff(w2, &buff)

	diff := buff.String()

	fmt.Printf("%s", diff)

	return len(diff) == 0
}

func TestWitnessSerialization(t *testing.T) {
	expectedHeader := defaultWitnessHeader()

	expectedOperands := generateOperands()

	expectedWitness := Witness{expectedHeader, expectedOperands}

	var buffer bytes.Buffer

	if _, err := expectedWitness.WriteInto(&buffer); err != nil {
		t.Error(err)
	}

	decodedWitness, err := NewWitnessFromReader(&buffer, false /* trace */)
	if err != nil {
		t.Error(err)
	}

	if !witnessesEqual(&expectedWitness, decodedWitness) {
		t.Errorf("witnesses not equal: expected %+v; got %+v", expectedWitness, decodedWitness)
	}
}

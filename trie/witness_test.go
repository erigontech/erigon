package trie

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

func generateOperands() []WitnessOperator {
	return []WitnessOperator{
		&OperatorBranch{Mask: 0xFF},
		&OperatorEmptyRoot{},
		&OperatorCode{[]byte("code-operand-1")},
		&OperatorExtension{[]byte("extension-key-1")},
		&OperatorLeafValue{[]byte("leaf-value-key-1"), []byte("leaf-value-value-1")},
		&OperatorHash{common.HexToHash("0xabcabcabcabc")},
		&OperatorLeafAccount{
			[]byte("lead-account-key-1"),
			999,
			big.NewInt(552),
			true,
			false,
		},
		&OperatorLeafAccount{
			[]byte("lead-account-key-2"),
			757,
			big.NewInt(334),
			true,
			true,
		},
		&OperatorLeafAccount{
			[]byte("lead-account-key-2"),
			333,
			big.NewInt(11112),
			false,
			false,
		},
		&OperatorLeafAccount{
			[]byte("lead-account-key-2"),
			0,
			big.NewInt(0),
			false,
			false,
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

	if _, err := expectedWitness.WriteTo(&buffer); err != nil {
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

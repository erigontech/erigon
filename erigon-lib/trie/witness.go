package trie

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// WitnessStorage is an interface representing a single
type WitnessStorage interface {
	GetWitnessesForBlock(uint64, uint32) ([]byte, error)
}

// WitnessVersion represents the current version of the block witness
// in case of incompatible changes it should be updated and the code to migrate the
// old witness format should be present
const WitnessVersion = uint8(0)

// WitnessHeader contains version information and maybe some future format bits
// the version is always the 1st bit.
type WitnessHeader struct {
	Version uint8
}

func (h *WitnessHeader) WriteTo(out *OperatorMarshaller) error {
	_, err := out.WithColumn(ColumnStructure).Write([]byte{h.Version})
	return err
}

func (h *WitnessHeader) LoadFrom(input io.Reader) error {
	version := make([]byte, 1)
	if _, err := input.Read(version); err != nil {
		return err
	}

	h.Version = version[0]
	return nil
}

func defaultWitnessHeader() WitnessHeader {
	return WitnessHeader{WitnessVersion}
}

type Witness struct {
	Header    WitnessHeader
	Operators []WitnessOperator
}

func NewWitness(operands []WitnessOperator) *Witness {
	return &Witness{
		Header:    defaultWitnessHeader(),
		Operators: operands,
	}
}

func (w *Witness) WriteInto(out io.Writer) (*BlockWitnessStats, error) {
	statsCollector := NewOperatorMarshaller(out)

	if err := w.Header.WriteTo(statsCollector); err != nil {
		return nil, err
	}

	for _, op := range w.Operators {
		if err := op.WriteTo(statsCollector); err != nil {
			return nil, err
		}
	}
	return statsCollector.GetStats(), nil
}

func NewWitnessFromReader(input io.Reader, trace bool) (*Witness, error) {
	var header WitnessHeader
	if err := header.LoadFrom(input); err != nil {
		return nil, err
	}

	if header.Version != WitnessVersion {
		return nil, fmt.Errorf("unexpected witness version: expected %d, got %d", WitnessVersion, header.Version)
	}

	operatorLoader := NewOperatorUnmarshaller(input)

	opcode := make([]byte, 1)
	var err error
	operands := make([]WitnessOperator, 0)
	for _, err = input.Read(opcode); ; _, err = input.Read(opcode) {
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		var op WitnessOperator
		switch OperatorKindCode(opcode[0]) {
		case OpHash:
			op = &OperatorHash{}
		case OpLeaf:
			op = &OperatorLeafValue{}
		case OpAccountLeaf:
			op = &OperatorLeafAccount{}
		case OpCode:
			op = &OperatorCode{}
		case OpBranch:
			op = &OperatorBranch{}
		case OpEmptyRoot:
			op = &OperatorEmptyRoot{}
		case OpExtension:
			op = &OperatorExtension{}
		case OpNewTrie:
			/* end of the current trie, end the function */
		default:
			return nil, fmt.Errorf("unexpected opcode while reading witness: %x", opcode[0])
		}

		if op == nil {
			break
		}

		err = op.LoadFrom(operatorLoader)
		if err != nil {
			return nil, err
		}

		if trace {
			fmt.Printf("read op %T -> %+v\n", op, op)
		}

		operands = append(operands, op)
	}
	if trace {
		fmt.Println("end of read ***** ")
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return &Witness{Header: header, Operators: operands}, nil
}

func (w *Witness) WriteDiff(w2 *Witness, output io.Writer) {
	if w.Header.Version != w2.Header.Version {
		fmt.Fprintf(output, "w1 header %d; w2 header %d\n", w.Header.Version, w2.Header.Version)
	}

	if len(w.Operators) != len(w2.Operators) {
		fmt.Fprintf(output, "w1 operands: %d; w2 operands: %d\n", len(w.Operators), len(w2.Operators))
	}

	length := len(w.Operators)
	if len(w2.Operators) > length {
		length = len(w2.Operators)
	}

	for i := 0; i < length; i++ {
		var op WitnessOperator
		if i < len(w.Operators) {
			op = w.Operators[i]
		}
		if i >= len(w2.Operators) {
			fmt.Fprintf(output, "unexpected o1[%d] = %T %v; o2[%d] = nil\n", i, op, op, i)
			continue
		}
		switch o1 := op.(type) {
		case *OperatorBranch:
			o2, ok := w2.Operators[i].(*OperatorBranch)
			if !ok {
				fmt.Fprintf(output, "o1[%d] = %T %+v; o2[%d] = %T %+v\n", i, o1, o1, i, o2, o2)
			}
			if o1.Mask != o2.Mask {
				fmt.Fprintf(output, "o1[%d].Mask = %v; o2[%d].Mask = %v", i, o1.Mask, i, o2.Mask)
			}
		case *OperatorHash:
			o2, ok := w2.Operators[i].(*OperatorHash)
			if !ok {
				fmt.Fprintf(output, "o1[%d] = %T %+v; o2[%d] = %T %+v\n", i, o1, o1, i, o2, o2)
			}
			if !bytes.Equal(o1.Hash.Bytes(), o2.Hash.Bytes()) {
				fmt.Fprintf(output, "o1[%d].Hash = %s; o2[%d].Hash = %s\n", i, o1.Hash.Hex(), i, o2.Hash.Hex())
			}
		case *OperatorCode:
			o2, ok := w2.Operators[i].(*OperatorCode)
			if !ok {
				fmt.Fprintf(output, "o1[%d] = %T %+v; o2[%d] = %T %+v\n", i, o1, o1, i, o2, o2)
			}
			if !bytes.Equal(o1.Code, o2.Code) {
				fmt.Fprintf(output, "o1[%d].Code = %x; o2[%d].Code = %x\n", i, o1.Code, i, o2.Code)
			}
		case *OperatorEmptyRoot:
			o2, ok := w2.Operators[i].(*OperatorEmptyRoot)
			if !ok {
				fmt.Fprintf(output, "o1[%d] = %T %+v; o2[%d] = %T %+v\n", i, o1, o1, i, o2, o2)
			}
		case *OperatorExtension:
			o2, ok := w2.Operators[i].(*OperatorExtension)
			if !ok {
				fmt.Fprintf(output, "o1[%d] = %T %+v; o2[%d] = %T %+v\n", i, o1, o1, i, o2, o2)
			}
			if !bytes.Equal(o1.Key, o2.Key) {
				fmt.Fprintf(output, "extension o1[%d].Key = %x; o2[%d].Key = %x\n", i, o1.Key, i, o2.Key)
			}
		case *OperatorLeafAccount:
			o2, ok := w2.Operators[i].(*OperatorLeafAccount)
			if !ok {
				fmt.Fprintf(output, "o1[%d] = %T %+v; o2[%d] = %T %+v\n", i, o1, o1, i, o2, o2)
			}
			if !bytes.Equal(o1.Key, o2.Key) {
				fmt.Fprintf(output, "leafAcc o1[%d].Key = %x; o2[%d].Key = %x\n", i, o1.Key, i, o2.Key)
			}
			if o1.Nonce != o2.Nonce {
				fmt.Fprintf(output, "leafAcc o1[%d].Nonce = %v; o2[%d].Nonce = %v\n", i, o1.Nonce, i, o2.Nonce)
			}
			if o1.Balance.String() != o2.Balance.String() {
				fmt.Fprintf(output, "leafAcc o1[%d].Balance = %v; o2[%d].Balance = %v\n", i, o1.Balance.String(), i, o2.Balance.String())
			}
			if o1.HasCode != o2.HasCode {
				fmt.Fprintf(output, "leafAcc o1[%d].HasCode = %v; o2[%d].HasCode = %v\n", i, o1.HasCode, i, o2.HasCode)
			}
			if o1.HasStorage != o2.HasStorage {
				fmt.Fprintf(output, "leafAcc o1[%d].HasStorage = %v; o2[%d].HasStorage = %v\n", i, o1.HasStorage, i, o2.HasStorage)
			}
		case *OperatorLeafValue:
			o2, ok := w2.Operators[i].(*OperatorLeafValue)
			if !ok {
				fmt.Fprintf(output, "o1[%d] = %T %+v; o2[%d] = %T %+v\n", i, o1, o1, i, o2, o2)
			}
			if !bytes.Equal(o1.Key, o2.Key) {
				fmt.Fprintf(output, "leafVal o1[%d].Key = %x; o2[%d].Key = %x\n", i, o1.Key, i, o2.Key)
			}
			if !bytes.Equal(o1.Value, o2.Value) {
				fmt.Fprintf(output, "leafVal o1[%d].Value = %x; o2[%d].Value = %x\n", i, o1.Value, i, o2.Value)
			}
		default:
			o2 := w2.Operators[i]
			fmt.Fprintf(output, "unexpected o1[%d] = %T %+v; o2[%d] = %T %+v\n", i, o1, o1, i, o2, o2)
		}
	}
}

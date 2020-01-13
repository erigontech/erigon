package trie

import (
	"fmt"
	"io"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ugorji/go/codec"
)

var cbor codec.CborHandle

const (
	flagCode = 1 << iota
	flagStorage
	flagNonce
	flagBalance
)

// Instruction is "enum" type for defining the opcodes of the stack machine that reconstructs the structure of tries from Structure tape
type Instruction uint8

const (
	// OpLeaf creates leaf node and pushes it onto the node stack, its hash onto the hash stack
	OpLeaf Instruction = iota
	// OpExtension pops a node from the node stack, constructs extension node from it and its operand's key, and pushes this extension node onto
	// the node stack, its hash onto the hash stack
	OpExtension
	// OpBranch has operand, which is a bitset representing digits in the branch node. Pops the children nodes from the node stack (the number of
	// children is equal to the number of bits in the bitset), constructs branch node and pushes it onto the node stack, its hash onto the hash stack
	OpBranch
	// OpHash and pushes the hash them onto the stack.
	OpHash
	// OpCode constructs code node and pushes it onto the node stack, its hash onto the hash stack.
	OpCode
	// OpAccountLeaf constructs an account node (without any storage and code) and pushes it onto the node stack, its hash onto the hash stack.
	OpAccountLeaf
	// OpEmptyRoot places nil onto the node stack, and empty root hash onto the hash stack.
	OpEmptyRoot
)

// WitnessOperand is a single operand in the block witness. It knows how to serialize/deserialize itself.
type WitnessOperand interface {
	WriteTo(*WitnessStatsCollector) error

	// LoadFrom always assumes that the opcode value was already read
	LoadFrom(io.Reader) error
}

type OperandHash struct {
	Hash common.Hash
}

func (o *OperandHash) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpHash, output.WithColumn(ColumnStructure)); err != nil {
		return nil
	}
	_, err := output.WithColumn(ColumnHashes).Write(o.Hash.Bytes())
	return err
}

func (o *OperandHash) LoadFrom(input io.Reader) error {
	var hash common.Hash
	bytesRead, err := input.Read(hash[:])
	if err != nil {
		return err
	}
	if bytesRead != len(hash) {
		return fmt.Errorf("error while reading hash from input. expected to read %d bytes, read only %d", len(hash), bytesRead)
	}
	o.Hash = hash
	return nil
}

type OperandLeafValue struct {
	Key   []byte
	Value []byte
}

func (o *OperandLeafValue) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpLeaf, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}

	if err := encodeByteArray(o.Key, output.WithColumn(ColumnLeafKeys)); err != nil {
		return err
	}

	return encodeByteArray(o.Value, output.WithColumn(ColumnLeafValues))
}

func (o *OperandLeafValue) LoadFrom(input io.Reader) error {
	key, err := decodeByteArray(input)
	if err != nil {
		return err
	}

	o.Key = key

	value, err := decodeByteArray(input)
	if err != nil {
		return err
	}

	o.Value = value
	return nil
}

type OperandLeafAccount struct {
	Key        []byte
	Nonce      uint64
	Balance    big.Int
	HasCode    bool
	HasStorage bool
}

func (o *OperandLeafAccount) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpAccountLeaf, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}

	if err := encodeByteArray(o.Key, output.WithColumn(ColumnLeafKeys)); err != nil {
		return err
	}

	flags := byte(0)
	if o.HasCode {
		flags |= flagCode
	}
	if o.HasStorage {
		flags |= flagStorage
	}
	if o.Nonce > 0 {
		flags |= flagNonce
	}

	if o.Balance.Uint64() > 0 {
		flags |= flagBalance
	}

	_, err := output.WithColumn(ColumnLeafValues).Write([]byte{flags})

	if o.Nonce > 0 {
		encoder := codec.NewEncoder(output.WithColumn(ColumnLeafValues), &cbor)
		if err := encoder.Encode(o.Nonce); err != nil {
			return err
		}
	}

	if o.Balance.Uint64() > 0 {
		if err := encodeByteArray(o.Balance.Bytes(), output.WithColumn(ColumnLeafValues)); err != nil {
			return err
		}
	}

	return err
}

func (o *OperandLeafAccount) LoadFrom(input io.Reader) error {
	key, err := decodeByteArray(input)
	if err != nil {
		return err
	}
	o.Key = key

	flags := make([]byte, 1)
	_, err = input.Read(flags)
	if err != nil {
		return err
	}
	o.HasCode = flags[0]&flagCode != 0
	o.HasStorage = flags[0]&flagStorage != 0

	if flags[0]&flagNonce != 0 {
		var nonce uint64
		decoder := codec.NewDecoder(input, &cbor)
		err = decoder.Decode(&nonce)
		if err != nil {
			return err
		}

		o.Nonce = nonce
	}

	balance := big.NewInt(0)

	if flags[0]&flagBalance != 0 {
		var balanceBytes []byte
		balanceBytes, err = decodeByteArray(input)
		if err != nil {
			return err
		}
		balance.SetBytes(balanceBytes)
	}

	o.Balance = *balance

	return nil
}

type OperandCode struct {
	Code []byte
}

func (o *OperandCode) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpCode, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}

	return encodeByteArray(o.Code, output.WithColumn(ColumnCodes))
}

func (o *OperandCode) LoadFrom(input io.Reader) error {
	code, err := decodeByteArray(input)
	if err != nil {
		return err
	}
	o.Code = code
	return nil
}

type OperandBranch struct {
	Mask uint32
}

func (o *OperandBranch) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpBranch, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}
	encoder := codec.NewEncoder(output.WithColumn(ColumnStructure), &cbor)
	return encoder.Encode(o.Mask)
}

func (o *OperandBranch) LoadFrom(input io.Reader) error {
	var mask uint32
	decoder := codec.NewDecoder(input, &cbor)
	if err := decoder.Decode(&mask); err != nil {
		return err
	}

	o.Mask = mask

	return nil
}

type OperandEmptyRoot struct{}

func (o *OperandEmptyRoot) WriteTo(output *WitnessStatsCollector) error {
	return writeOpCode(OpEmptyRoot, output.WithColumn(ColumnStructure))
}

func (o *OperandEmptyRoot) LoadFrom(input io.Reader) error {
	// no-op
	return nil
}

type OperandExtension struct {
	Key []byte
}

func (o *OperandExtension) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpExtension, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}
	return encodeByteArray(o.Key, output.WithColumn(ColumnLeafKeys))
}

func (o *OperandExtension) LoadFrom(input io.Reader) error {
	key, err := decodeByteArray(input)
	if err != nil {
		return err
	}

	o.Key = key

	return nil
}

func decodeByteArray(input io.Reader) ([]byte, error) {
	codec := codec.NewDecoder(input, &cbor)

	var buffer interface{}
	err := codec.Decode(&buffer)
	if err != nil {
		return []byte{}, err
	}

	b, ok := buffer.([]byte)
	if !ok {
		return []byte{}, fmt.Errorf("unexpected decode result, expected []byte, got %T", buffer)
	}

	return b, nil
}

func encodeByteArray(data []byte, output io.Writer) error {
	encoder := codec.NewEncoder(output, &cbor)
	return encoder.Encode(data)
}

func writeOpCode(opcode Instruction, output io.Writer) error {
	_, err := output.Write([]byte{byte(opcode)})
	return err
}

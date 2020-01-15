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

// OperatorKindCode is "enum" type for defining the opcodes of the stack machine that reconstructs the structure of tries from Structure tape
type OperatorKindCode uint8

const (
	// OpLeaf creates leaf node and pushes it onto the node stack, its hash onto the hash stack
	OpLeaf OperatorKindCode = iota
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

// WitnessOperator is a single operand in the block witness. It knows how to serialize/deserialize itself.
type WitnessOperator interface {
	WriteTo(*WitnessStatsCollector) error

	// LoadFrom always assumes that the opcode value was already read
	LoadFrom(*OperatorLoader) error
}

type OperatorHash struct {
	Hash common.Hash
}

func (o *OperatorHash) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpHash, output.WithColumn(ColumnStructure)); err != nil {
		return nil
	}
	_, err := output.WithColumn(ColumnHashes).Write(o.Hash.Bytes())
	return err
}

func (o *OperatorHash) LoadFrom(loader *OperatorLoader) error {
	hash, err := loader.ReadHash()
	if err != nil {
		return err
	}

	o.Hash = hash
	return nil
}

type OperatorLeafValue struct {
	Key   []byte
	Value []byte
}

func (o *OperatorLeafValue) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpLeaf, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}

	if err := encodeByteArray(keyNibblesToBytes(o.Key), output.WithColumn(ColumnLeafKeys)); err != nil {
		return err
	}

	return encodeByteArray(o.Value, output.WithColumn(ColumnLeafValues))
}

func (o *OperatorLeafValue) LoadFrom(loader *OperatorLoader) error {
	key, err := loader.ReadByteArray()
	if err != nil {
		return err
	}

	o.Key = keyBytesToNibbles(key)

	value, err := loader.ReadByteArray()
	if err != nil {
		return err
	}

	o.Value = value
	return nil
}

type OperatorLeafAccount struct {
	Key        []byte
	Nonce      uint64
	Balance    *big.Int
	HasCode    bool
	HasStorage bool
}

func (o *OperatorLeafAccount) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpAccountLeaf, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}

	if err := encodeByteArray(keyNibblesToBytes(o.Key), output.WithColumn(ColumnLeafKeys)); err != nil {
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

	emptyBalance := big.NewInt(0)

	if o.Balance.Cmp(emptyBalance) != 0 {
		flags |= flagBalance
	}

	if _, err := output.WithColumn(ColumnLeafValues).Write([]byte{flags}); err != nil {
		return nil
	}

	if o.Nonce > 0 {
		encoder := codec.NewEncoder(output.WithColumn(ColumnLeafValues), &cbor)
		if err := encoder.Encode(o.Nonce); err != nil {
			return err
		}
	}

	if o.Balance.Cmp(emptyBalance) != 0 {
		bb := o.Balance.Bytes()
		if err := encodeByteArray(bb, output.WithColumn(ColumnLeafValues)); err != nil {
			return err
		}
	}

	return nil
}

func (o *OperatorLeafAccount) LoadFrom(loader *OperatorLoader) error {
	key, err := loader.ReadByteArray()
	if err != nil {
		return err
	}
	o.Key = keyBytesToNibbles(key)

	flags, err := loader.ReadByte()
	if err != nil {
		return err
	}

	o.HasCode = flags&flagCode != 0
	o.HasStorage = flags&flagStorage != 0

	if flags&flagNonce != 0 {
		nonce, err := loader.ReadUInt64()
		if err != nil {
			return err
		}
		o.Nonce = nonce
	}

	balance := big.NewInt(0)

	if flags&flagBalance != 0 {
		var balanceBytes []byte
		balanceBytes, err = loader.ReadByteArray()
		if err != nil {
			return err
		}
		balance.SetBytes(balanceBytes)
	}

	o.Balance = balance

	return nil
}

type OperatorCode struct {
	Code []byte
}

func (o *OperatorCode) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpCode, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}

	return encodeByteArray(o.Code, output.WithColumn(ColumnCodes))
}

func (o *OperatorCode) LoadFrom(loader *OperatorLoader) error {
	code, err := loader.ReadByteArray()
	if err != nil {
		return err
	}
	o.Code = code
	return nil
}

type OperatorBranch struct {
	Mask uint32
}

func (o *OperatorBranch) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpBranch, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}
	encoder := codec.NewEncoder(output.WithColumn(ColumnStructure), &cbor)
	return encoder.Encode(o.Mask)
}

func (o *OperatorBranch) LoadFrom(loader *OperatorLoader) error {
	mask, err := loader.ReadUint32()
	if err != nil {
		return err
	}

	o.Mask = mask
	return nil
}

type OperatorEmptyRoot struct{}

func (o *OperatorEmptyRoot) WriteTo(output *WitnessStatsCollector) error {
	return writeOpCode(OpEmptyRoot, output.WithColumn(ColumnStructure))
}

func (o *OperatorEmptyRoot) LoadFrom(loader *OperatorLoader) error {
	// no-op
	return nil
}

type OperatorExtension struct {
	Key []byte
}

func (o *OperatorExtension) WriteTo(output *WitnessStatsCollector) error {
	if err := writeOpCode(OpExtension, output.WithColumn(ColumnStructure)); err != nil {
		return err
	}
	return encodeByteArray(keyNibblesToBytes(o.Key), output.WithColumn(ColumnLeafKeys))
}

func (o *OperatorExtension) LoadFrom(loader *OperatorLoader) error {
	key, err := loader.ReadByteArray()
	if err != nil {
		return err
	}

	o.Key = keyBytesToNibbles(key)

	return nil
}

func encodeByteArray(data []byte, output io.Writer) error {
	encoder := codec.NewEncoder(output, &cbor)
	return encoder.Encode(data)
}

func writeOpCode(opcode OperatorKindCode, output io.Writer) error {
	_, err := output.Write([]byte{byte(opcode)})
	return err
}

func keyNibblesToBytes(nibbles []byte) []byte {
	if len(nibbles) < 1 {
		return []byte{}
	}
	if len(nibbles) < 2 {
		return nibbles
	}
	hasTerminator := false
	if nibbles[len(nibbles)-1] == 0x10 {
		nibbles = nibbles[:len(nibbles)-1]
		hasTerminator = true
	}

	targetLen := len(nibbles)/2 + len(nibbles)%2 + 1

	result := make([]byte, targetLen)
	nibbleIndex := 0
	result[0] = byte(len(nibbles) % 2) // parity bit
	for i := 1; i < len(result); i++ {
		result[i] = nibbles[nibbleIndex] * 16
		nibbleIndex++
		if nibbleIndex < len(nibbles) {
			result[i] += nibbles[nibbleIndex]
			nibbleIndex++
		}
	}
	if hasTerminator {
		result[0] |= 1 << 1
	}

	return result
}

func keyBytesToNibbles(b []byte) []byte {
	if len(b) < 1 {
		return []byte{}
	}
	if len(b) < 2 {
		return b
	}

	hasTerminator := b[0]&(1<<1) != 0

	targetLen := (len(b)-1)*2 - int(b[0]&1)

	nibbles := make([]byte, targetLen)

	nibbleIndex := 0
	for i := 1; i < len(b); i++ {
		nibbles[nibbleIndex] = b[i] / 16
		nibbleIndex++
		if nibbleIndex < len(nibbles) {
			nibbles[nibbleIndex] = b[i] % 16
			nibbleIndex++
		}
	}
	if hasTerminator {
		return append(nibbles, 0x10)
	}
	return nibbles
}

type OperatorLoader struct {
	reader  io.Reader
	decoder *codec.Decoder
}

func NewOperatorLoader(r io.Reader) *OperatorLoader {
	return &OperatorLoader{r, codec.NewDecoder(r, &cbor)}
}

func (l *OperatorLoader) ReadByteArray() ([]byte, error) {
	var buffer []byte
	err := l.decoder.Decode(&buffer)
	if err != nil {
		return []byte{}, err
	}

	return buffer, nil
}

func (l *OperatorLoader) ReadHash() (common.Hash, error) {
	var hash common.Hash
	bytesRead, err := l.reader.Read(hash[:])
	if err != nil {
		return hash, err
	}
	if bytesRead != len(hash) {
		return hash, fmt.Errorf("error while reading hash from input. expected to read %d bytes, read only %d", len(hash), bytesRead)
	}
	return hash, nil
}

func (l *OperatorLoader) ReadUint32() (uint32, error) {
	var value uint32
	if err := l.decoder.Decode(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func (l *OperatorLoader) ReadByte() (byte, error) {
	values := make([]byte, 1)
	bytesRead, err := l.reader.Read(values)
	if err != nil {
		return 0, err
	}
	if bytesRead < 1 {
		return 0, fmt.Errorf("could not read a byte from a reader")
	}
	return values[0], nil
}

func (l *OperatorLoader) ReadUInt64() (uint64, error) {
	var value uint64
	err := l.decoder.Decode(&value)
	if err != nil {
		return 0, err
	}
	return value, nil

}

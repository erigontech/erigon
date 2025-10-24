package trie

import (
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/ugorji/go/codec"
)

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

	// OpNewTrie stops the processing, because another trie is encoded into the witness.
	OpNewTrie = OperatorKindCode(0xBB)
)

// WitnessOperator is a single operand in the block witness. It knows how to serialize/deserialize itself.
type WitnessOperator interface {
	WriteTo(*OperatorMarshaller) error

	// LoadFrom always assumes that the opcode value was already read
	LoadFrom(*OperatorUnmarshaller) error
}

type OperatorHash struct {
	Hash common.Hash
}

func (o *OperatorHash) WriteTo(output *OperatorMarshaller) error {
	if err := output.WriteOpCode(OpHash); err != nil {
		return err
	}
	return output.WriteHash(o.Hash)
}

func (o *OperatorHash) LoadFrom(loader *OperatorUnmarshaller) error {
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

func (o *OperatorLeafValue) WriteTo(output *OperatorMarshaller) error {
	if err := output.WriteOpCode(OpLeaf); err != nil {
		return err
	}

	if err := output.WriteKey(o.Key); err != nil {
		return err
	}

	return output.WriteByteArrayValue(o.Value)
}

func (o *OperatorLeafValue) LoadFrom(loader *OperatorUnmarshaller) error {
	key, err := loader.ReadKey()
	if err != nil {
		return err
	}

	o.Key = key

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
	CodeSize   uint64
}

func (o *OperatorLeafAccount) WriteTo(output *OperatorMarshaller) error {
	if err := output.WriteOpCode(OpAccountLeaf); err != nil {
		return err
	}

	if err := output.WriteKey(o.Key); err != nil {
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

	if o.Balance.Sign() != 0 {
		flags |= flagBalance
	}

	if err := output.WriteByteValue(flags); err != nil {
		return err
	}

	if o.Nonce > 0 {
		if err := output.WriteUint64Value(o.Nonce); err != nil {
			return err
		}
	}

	if o.Balance.Sign() != 0 {
		if err := output.WriteByteArrayValue(o.Balance.Bytes()); err != nil {
			return err
		}
	}

	if o.HasCode {
		if err := output.WriteUint64Value(o.CodeSize); err != nil {
			return err
		}
	}

	return nil
}

func (o *OperatorLeafAccount) LoadFrom(loader *OperatorUnmarshaller) error {
	key, err := loader.ReadKey()
	if err != nil {
		return err
	}
	o.Key = key

	flags, err := loader.ReadByte()
	if err != nil {
		return err
	}

	o.HasCode = flags&flagCode != 0
	o.HasStorage = flags&flagStorage != 0

	if flags&flagNonce != 0 {
		o.Nonce, err = loader.ReadUInt64()
		if err != nil {
			return err
		}
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

	if o.HasCode {
		if codeSize, err := loader.ReadUInt64(); err == nil {
			o.CodeSize = codeSize
		} else {
			return err
		}
	}

	return nil
}

type OperatorCode struct {
	Code []byte
}

func (o *OperatorCode) WriteTo(output *OperatorMarshaller) error {
	if err := output.WriteOpCode(OpCode); err != nil {
		return err
	}

	return output.WriteCode(o.Code)
}

func (o *OperatorCode) LoadFrom(loader *OperatorUnmarshaller) error {
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

func (o *OperatorBranch) WriteTo(output *OperatorMarshaller) error {
	if err := output.WriteOpCode(OpBranch); err != nil {
		return err
	}

	encoder := codec.NewEncoder(output.WithColumn(ColumnStructure), &cbor)
	return encoder.Encode(o.Mask)
}

func (o *OperatorBranch) LoadFrom(loader *OperatorUnmarshaller) error {
	mask, err := loader.ReadUint32()
	if err != nil {
		return err
	}

	o.Mask = mask
	return nil
}

type OperatorEmptyRoot struct{}

func (o *OperatorEmptyRoot) WriteTo(output *OperatorMarshaller) error {
	return output.WriteOpCode(OpEmptyRoot)
}

func (o *OperatorEmptyRoot) LoadFrom(loader *OperatorUnmarshaller) error {
	// no-op
	return nil
}

type OperatorExtension struct {
	Key []byte
}

func (o *OperatorExtension) WriteTo(output *OperatorMarshaller) error {
	if err := output.WriteOpCode(OpExtension); err != nil {
		return err
	}
	return output.WriteKey(o.Key)
}

func (o *OperatorExtension) LoadFrom(loader *OperatorUnmarshaller) error {
	key, err := loader.ReadKey()
	if err != nil {
		return err
	}
	o.Key = key

	return nil
}

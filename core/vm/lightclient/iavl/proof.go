package iavl

import (
	"bytes"
	"fmt"

	"github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/crypto/tmhash"
	cmn "github.com/tendermint/tendermint/libs/common"
)

var (
	// ErrInvalidProof is returned by Verify when a proof cannot be validated.
	ErrInvalidProof = fmt.Errorf("invalid proof")

	// ErrInvalidInputs is returned when the inputs passed to the function are invalid.
	ErrInvalidInputs = fmt.Errorf("invalid inputs")

	// ErrInvalidRoot is returned when the root passed in does not match the proof's.
	ErrInvalidRoot = fmt.Errorf("invalid root")
)

//----------------------------------------

type proofInnerNode struct {
	Height  int8   `json:"height"`
	Size    int64  `json:"size"`
	Version int64  `json:"version"`
	Left    []byte `json:"left"`
	Right   []byte `json:"right"`
}

func (pin proofInnerNode) String() string {
	return pin.stringIndented("")
}

func (pin proofInnerNode) stringIndented(indent string) string {
	return fmt.Sprintf(`proofInnerNode{
%s  Height:  %v
%s  Size:    %v
%s  Version: %v
%s  Left:    %X
%s  Right:   %X
%s}`,
		indent, pin.Height,
		indent, pin.Size,
		indent, pin.Version,
		indent, pin.Left,
		indent, pin.Right,
		indent)
}

func (pin proofInnerNode) Hash(childHash []byte) []byte {
	hasher := tmhash.New()
	buf := new(bytes.Buffer)

	err := amino.EncodeInt8(buf, pin.Height)
	if err == nil {
		err = amino.EncodeVarint(buf, pin.Size)
	}
	if err == nil {
		err = amino.EncodeVarint(buf, pin.Version)
	}

	if len(pin.Left) == 0 {
		if err == nil {
			err = amino.EncodeByteSlice(buf, childHash)
		}
		if err == nil {
			err = amino.EncodeByteSlice(buf, pin.Right)
		}
	} else {
		if err == nil {
			err = amino.EncodeByteSlice(buf, pin.Left)
		}
		if err == nil {
			err = amino.EncodeByteSlice(buf, childHash)
		}
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to hash proofInnerNode: %v", err))
	}

	hasher.Write(buf.Bytes())
	return hasher.Sum(nil)
}

//----------------------------------------

type proofLeafNode struct {
	Key       cmn.HexBytes `json:"key"`
	ValueHash cmn.HexBytes `json:"value"`
	Version   int64        `json:"version"`
}

func (pln proofLeafNode) String() string {
	return pln.stringIndented("")
}

func (pln proofLeafNode) stringIndented(indent string) string {
	return fmt.Sprintf(`proofLeafNode{
%s  Key:       %v
%s  ValueHash: %X
%s  Version:   %v
%s}`,
		indent, pln.Key,
		indent, pln.ValueHash,
		indent, pln.Version,
		indent)
}

func (pln proofLeafNode) Hash() []byte {
	hasher := tmhash.New()
	buf := new(bytes.Buffer)

	err := amino.EncodeInt8(buf, 0)
	if err == nil {
		err = amino.EncodeVarint(buf, 1)
	}
	if err == nil {
		err = amino.EncodeVarint(buf, pln.Version)
	}
	if err == nil {
		err = amino.EncodeByteSlice(buf, pln.Key)
	}
	if err == nil {
		err = amino.EncodeByteSlice(buf, pln.ValueHash)
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to hash proofLeafNode: %v", err))
	}
	hasher.Write(buf.Bytes())

	return hasher.Sum(nil)
}

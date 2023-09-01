package cltypes

import (
	"encoding/hex"
	"encoding/json"
)

type Bytes96 [96]byte

func (b Bytes96) MarshalJSON() ([]byte, error) {
	return json.Marshal("0x" + hex.EncodeToString(b[:]))
}

type Bytes4 [4]byte

func (b Bytes4) MarshalJSON() ([]byte, error) {
	return json.Marshal("0x" + hex.EncodeToString(b[:]))
}

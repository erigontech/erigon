// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"encoding/json"
	"errors"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
)

// Preserve empty target, signer and message fields in the JSON forms. Resolving
// their defaults here would change the signed RLP payload on a round trip.
type frameJSON struct {
	Mode          hexutil.Uint64  `json:"mode"`
	Flags         hexutil.Uint64  `json:"flags"`
	Target        *common.Address `json:"target"`
	GasLimit      hexutil.Uint64  `json:"gasLimit"`
	StateGasLimit hexutil.Uint64  `json:"stateGasLimit"`
	Value         hexutil.U256    `json:"value"`
	Data          hexutil.Bytes   `json:"data"`
}

func (frame Frame) MarshalJSON() ([]byte, error) {
	return json.Marshal(frameJSON{
		hexutil.Uint64(frame.Mode), hexutil.Uint64(frame.Flags), frame.Target,
		hexutil.Uint64(frame.Limits.Execution), hexutil.Uint64(frame.Limits.State), hexutil.U256(frame.Value), frame.Data,
	})
}

func (frame *Frame) UnmarshalJSON(input []byte) error {
	var dec frameJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.Mode > FramePostTx || dec.Flags >= 8 {
		return errors.New("invalid frame mode or flags")
	}
	*frame = Frame{uint8(dec.Mode), uint8(dec.Flags), dec.Target, mdgas.MdGas{Execution: uint64(dec.GasLimit), State: uint64(dec.StateGasLimit)}, uint256.Int(dec.Value), dec.Data}
	return nil
}

type frameSignatureJSON struct {
	Scheme    hexutil.Uint64 `json:"scheme"`
	Signer    hexutil.Bytes  `json:"signer"`
	Message   hexutil.Bytes  `json:"msg"`
	Signature hexutil.Bytes  `json:"signature"`
}

func (sig FrameSignature) MarshalJSON() ([]byte, error) {
	return json.Marshal(frameSignatureJSON{hexutil.Uint64(sig.Scheme), sig.Signer, sig.Message, sig.Signature})
}

func (sig *FrameSignature) UnmarshalJSON(input []byte) error {
	var dec frameSignatureJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.Scheme > FrameSignatureP256 {
		return errors.New("invalid frame signature scheme")
	}
	*sig = FrameSignature{uint8(dec.Scheme), dec.Signer, dec.Message, dec.Signature}
	return nil
}

type frameTransactionJSON struct {
	Type                 hexutil.Uint64   `json:"type"`
	ChainID              *hexutil.U256    `json:"chainId"`
	NonceKeys            FrameNonceKeys   `json:"nonceKeys"`
	Nonce                *hexutil.Uint64  `json:"nonce"`
	Sender               *common.Address  `json:"sender"`
	Frames               []Frame          `json:"frames"`
	Signatures           []FrameSignature `json:"signatures"`
	MaxPriorityFeePerGas *hexutil.U256    `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.U256    `json:"maxFeePerGas"`
	MaxFeePerBlobGas     *hexutil.U256    `json:"maxFeePerBlobGas"`
	BlobVersionedHashes  []common.Hash    `json:"blobVersionedHashes"`
	Hash                 common.Hash      `json:"hash"`
}

func (tx *FrameTransaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(frameTransactionJSON{
		Type: FrameTxType, ChainID: (*hexutil.U256)(&tx.ChainID), Nonce: (*hexutil.Uint64)(&tx.Nonce), Sender: &tx.SenderAddress,
		NonceKeys: tx.NonceKeys,
		Frames:    tx.Frames, Signatures: tx.Signatures, MaxPriorityFeePerGas: (*hexutil.U256)(&tx.Fees.TipCap),
		MaxFeePerGas: (*hexutil.U256)(&tx.Fees.FeeCap), MaxFeePerBlobGas: (*hexutil.U256)(&tx.Fees.BlobFeeCap),
		BlobVersionedHashes: tx.BlobVersionedHashes, Hash: tx.Hash(),
	})
}

func (tx *FrameTransaction) UnmarshalJSON(input []byte) error {
	var dec frameTransactionJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.ChainID == nil || dec.Nonce == nil || dec.Sender == nil || dec.Frames == nil || dec.MaxFeePerGas == nil || dec.MaxPriorityFeePerGas == nil || dec.MaxFeePerBlobGas == nil {
		return errors.New("missing required frame transaction field")
	}
	*tx = FrameTransaction{
		ChainID: uint256.Int(*dec.ChainID), Nonce: uint64(*dec.Nonce), SenderAddress: *dec.Sender,
		NonceKeys: dec.NonceKeys,
		Frames:    dec.Frames, Signatures: dec.Signatures, BlobVersionedHashes: dec.BlobVersionedHashes,
		Fees: FrameFees{uint256.Int(*dec.MaxPriorityFeePerGas), uint256.Int(*dec.MaxFeePerGas), uint256.Int(*dec.MaxFeePerBlobGas)},
	}
	return tx.Validate()
}

type Frames []Frame

type FrameSignatures []FrameSignature

type FrameNonceKeys []uint256.Int

func (keys FrameNonceKeys) MarshalJSON() ([]byte, error) {
	values := make([]hexutil.U256, len(keys))
	for i := range keys {
		values[i] = hexutil.U256(keys[i])
	}
	return json.Marshal(values)
}

func (keys *FrameNonceKeys) UnmarshalJSON(input []byte) error {
	var values []hexutil.U256
	if err := json.Unmarshal(input, &values); err != nil {
		return err
	}
	*keys = make(FrameNonceKeys, len(values))
	for i := range values {
		(*keys)[i] = uint256.Int(values[i])
	}
	return nil
}

func (keys FrameNonceKeys) MarshalFastJSONTo(s *jsonstream.Stream) error {
	s.WriteArrayStart()
	for i := range keys {
		s.WriteQuotedText((*hexutil.U256)(&keys[i]))
	}
	s.WriteArrayEnd()
	return nil
}

func (frames Frames) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, frames, writeFrameElem)
	return nil
}

func writeFrameElem(s *jsonstream.Stream, frame *Frame) {
	s.WriteObjectStart()
	ethjson.Quantity(s, "mode", uint64(frame.Mode))
	ethjson.Quantity(s, "flags", uint64(frame.Flags))
	if frame.Target == nil {
		s.Field("target").WriteNil()
	} else {
		ethjson.Data(s, "target", frame.Target[:])
	}
	ethjson.Quantity(s, "gasLimit", frame.Limits.Execution)
	ethjson.Quantity(s, "stateGasLimit", frame.Limits.State)
	ethjson.Quantity256(s, "value", &frame.Value)
	ethjson.Data(s, "data", frame.Data)
	s.WriteObjectEnd()
}

func (signatures FrameSignatures) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, signatures, writeFrameSignatureElem)
	return nil
}

func writeFrameSignatureElem(s *jsonstream.Stream, sig *FrameSignature) {
	s.WriteObjectStart()
	ethjson.Quantity(s, "scheme", uint64(sig.Scheme))
	ethjson.Data(s, "signer", sig.Signer)
	ethjson.Data(s, "msg", sig.Message)
	ethjson.Data(s, "signature", sig.Signature)
	s.WriteObjectEnd()
}

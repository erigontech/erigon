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

package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/abi"
)

// A rollup manager's quorum getters, and the signer of an ECDSA proof
// system.
var (
	thresholdMethod = abi.NewMethod("threshold", "threshold", abi.Function, "view", false, false,
		nil, abi.Arguments{{Type: mustType("uint256", nil)}})
	verificationKeyMethod = abi.NewMethod("verificationKey", "verificationKey", abi.Function, "view", false, false,
		abi.Arguments{{Name: "proofSystem", Type: mustType("address", nil)}}, abi.Arguments{{Type: mustType("bytes32", nil)}})
	signerMethod = abi.NewMethod("signer", "signer", abi.Function, "view", false, false,
		nil, addressReturn)
)

// EncodeThreshold returns the input of threshold().
func EncodeThreshold() []byte { return slices.Clone(thresholdMethod.ID) }

// EncodeVerificationKey returns the input of verificationKey(proofSystem).
func EncodeVerificationKey(proofSystem common.Address) []byte {
	return append(slices.Clone(verificationKeyMethod.ID), common.LeftPadBytes(proofSystem[:], 32)...)
}

// EncodeSigner returns the input of signer().
func EncodeSigner() []byte { return slices.Clone(signerMethod.ID) }

// word returns the first 32-byte word of out, as Solidity's decoder reads a
// static return.
func word(out []byte, what string) ([]byte, error) {
	if len(out) < 32 {
		return nil, fmt.Errorf("%s return: %d bytes", what, len(out))
	}
	return out[:32], nil
}

// DecodeThreshold decodes the output of threshold().
func DecodeThreshold(out []byte) (uint64, error) {
	w, err := word(out, "threshold")
	if err != nil {
		return 0, err
	}
	if slices.ContainsFunc(w[:24], func(b byte) bool { return b != 0 }) {
		return 0, errors.New("threshold return above 64 bits")
	}
	return binary.BigEndian.Uint64(w[24:]), nil
}

// DecodeVerificationKey decodes the output of verificationKey(proofSystem).
func DecodeVerificationKey(out []byte) (common.Hash, error) {
	w, err := word(out, "verificationKey")
	if err != nil {
		return common.Hash{}, err
	}
	return common.BytesToHash(w), nil
}

// DecodeSigner decodes the output of signer().
func DecodeSigner(out []byte) (common.Address, error) {
	w, err := word(out, "signer")
	if err != nil {
		return common.Address{}, err
	}
	if slices.ContainsFunc(w[:12], func(b byte) bool { return b != 0 }) {
		return common.Address{}, errors.New("signer return is not an address")
	}
	return common.BytesToAddress(w[12:]), nil
}

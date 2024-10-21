// Copyright 2024 The Erigon Authors
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
	"crypto/sha256"

	libcommon "github.com/erigontech/erigon-lib/common"
)

const DepositRequestType byte = 0x00
const WithdrawalRequestType byte = 0x01
const ConsolidationRequestType byte = 0x02
const DepositRequestDataLen = 192       // BLSPubKeyLen + WithdrawalCredentialsLen + 8 + BLSSigLen + 8
const WithdrawalRequestDataLen = 76     // addr + pubkey + amt
const ConsolidationRequestDataLen = 116 // addr + sourcePubkey + targetPubkey

var KnownRequestTypes = []byte{DepositRequestType, WithdrawalRequestType, ConsolidationRequestType}

type FlatRequest struct {
	Type        byte
	RequestData []byte
}

// Returns the request type of the underlying request
func (f *FlatRequest) RequestType() byte { return f.Type }

// Encodes flat encoding of request the way it should be serialized
func (f *FlatRequest) Encode() []byte    { return append([]byte{f.Type}, f.RequestData...) }

// Returns pointer to deep copy of a new FlatRequest
func (f *FlatRequest) copy() *FlatRequest {
	return &FlatRequest{Type: f.Type, RequestData: append([]byte{}, f.RequestData...)}
}

type FlatRequests []FlatRequest

func (r FlatRequests) Hash() *libcommon.Hash {
	if r == nil || len(r) < len(KnownRequestTypes) {
		return nil
	}
	sha := sha256.New()
	for i, t := range KnownRequestTypes {
		hi := sha256.Sum256(append([]byte{t}, r[i].RequestData...))
		sha.Write(hi[:])
	}
	h := libcommon.BytesToHash(sha.Sum(nil))
	return &h
}

func (r FlatRequests) Len() int { return len(r) }
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

	"github.com/erigontech/erigon-lib/common"
)

const DepositRequestType byte = 0x00
const WithdrawalRequestType byte = 0x01
const ConsolidationRequestType byte = 0x02
const DepositRequestDataLen = 192       // BLSPubKeyLen + WithdrawalCredentialsLen + 8 + BLSSigLen + 8
const WithdrawalRequestDataLen = 76     // addr + pubkey + amt
const ConsolidationRequestDataLen = 116 // addr + sourcePubkey + targetPubkey

var KnownRequestTypes = []byte{DepositRequestType, WithdrawalRequestType, ConsolidationRequestType}

// FlatRequest carries serialized (flat) request data from any known Request type
// The RequestData slice can contain collated data for more than one request of the same type
type FlatRequest struct {
	Type        byte
	RequestData []byte
}

// Returns the request type of the underlying request
func (f *FlatRequest) RequestType() byte { return f.Type }

// Encodes flat encoding of request the way it should be serialized
func (f *FlatRequest) Encode() []byte { return append([]byte{f.Type}, f.RequestData...) }

type FlatRequests []FlatRequest

func (r FlatRequests) Hash() *common.Hash {
	if r == nil {
		return nil
	}
	sha := sha256.New()
	for i, t := range r {
		if len(t.RequestData) == 0 {
			continue
		}
		hi := sha256.Sum256(append([]byte{t.Type}, r[i].RequestData...))
		sha.Write(hi[:])
	}
	h := common.BytesToHash(sha.Sum(nil))
	return &h
}

func (r FlatRequests) Len() int { return len(r) }

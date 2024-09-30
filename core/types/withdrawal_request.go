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
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon/rlp"
)

// EIP-7002 Withdrawal Request see https://github.com/ethereum/EIPs/blob/master/EIPS/eip-7002.md
type WithdrawalRequest struct {
	RequestData [WithdrawalRequestDataLen]byte
}

type WithdrawalRequestJson struct {
	RequestData string
}

func (w *WithdrawalRequest) RequestType() byte {
	return WithdrawalRequestType
}

// encodingSize implements RequestData.
func (w *WithdrawalRequest) EncodingSize() (encodingSize int) {
	return WithdrawalRequestDataLen 
}
func (w *WithdrawalRequest) EncodeRLP(b io.Writer) (err error) {
	if _, err = b.Write([]byte{WithdrawalRequestType}); err != nil {
		return err
	}
	if _, err = b.Write(w.RequestData[:]); err != nil {
		return err
	}
	return
}

func (w *WithdrawalRequest) DecodeRLP(input []byte) error { return rlp.DecodeBytes(input[1:], w) }
func (w *WithdrawalRequest) copy() Request {
	return &WithdrawalRequest{
		RequestData: [WithdrawalRequestDataLen]byte(bytes.Clone(w.RequestData[:])),
	}
}

func (w *WithdrawalRequest) MarshalJSON() ([]byte, error) {
	tt := WithdrawalRequestJson{
		RequestData: hexutility.Encode(w.RequestData[:]),
	}
	return json.Marshal(tt)
}

func (w *WithdrawalRequest) UnmarshalJSON(input []byte) error {
	tt := WithdrawalRequestJson{}
	err := json.Unmarshal(input, &tt)
	if err != nil {
		return err
	}
	if len(tt.RequestData) != WithdrawalRequestDataLen {
		return fmt.Errorf("Cannot unmarshal request data, length mismatch")
	}

	w.RequestData = [WithdrawalRequestDataLen]byte(hexutility.MustDecodeString(tt.RequestData))
	return nil
}

type WithdrawalRequests []*WithdrawalRequest

// Len returns the length of s.
func (s WithdrawalRequests) Len() int { return len(s) }

// EncodeIndex encodes the i'th withdrawal request to w.
func (s WithdrawalRequests) EncodeIndex(i int, w *bytes.Buffer) {
	s[i].EncodeRLP(w)
}

// Requests creates a deep copy of each WithdrawalRequest and returns a slice (as Requests).
func (s WithdrawalRequests) Requests() (reqs Requests) {
	for _, d := range s {
		reqs = append(reqs, d)
	}
	return
}

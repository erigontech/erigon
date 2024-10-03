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
	"errors"
	"fmt"
	"io"
	"math/bits"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	rlp2 "github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/rlp"
)

const WithdrawalRequestType byte = 0x01
const DepositRequestType byte = 0x00
const ConsolidationRequestType byte = 0x02

type Request interface {
	EncodeRLP(io.Writer) error
	DecodeRLP([]byte) error
	RequestType() byte
	copy() Request
	EncodingSize() int
}

func decode(data []byte) (Request, error) {
	if len(data) <= 1 {
		return nil, errors.New("error: too short type request")
	}
	var req Request
	switch data[0] {
	case DepositRequestType:
		req = new(DepositRequest)
	case WithdrawalRequestType:
		req = new(WithdrawalRequest)
	case ConsolidationRequestType:
		req = new(ConsolidationRequest)
	default:
		return nil, fmt.Errorf("unknown request type - %d", data[0])
	}

	if err := req.DecodeRLP(data); err != nil {
		return nil, err
	}
	return req, nil
}

type Requests []Request

func (r *Requests) DecodeRLP(s *rlp.Stream) (err error) {
	if _, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			*r = nil
			return nil
		}
		return fmt.Errorf("read requests: %v", err)
	}
	*r = make(Requests, 0)
	for {
		var req Request
		kind, _, err := s.Kind()
		if err != nil {
			return err
		}
		switch kind {
		case rlp.List:
			return errors.New("error: untyped request (unexpected lit)")
		case rlp.Byte:
			return errors.New("error: too short request")
		default:
			var buf []byte
			if buf, err = s.Bytes(); err != nil {
				return err
			}
			if req, err = decode(buf); err != nil {
				return err
			}
			*r = append(*r, req)
		}
	}
}

func (r *Requests) EncodeRLP(w io.Writer) {
	if r == nil {
		return
	}
	var c int
	for _, req := range *r {
		e := req.EncodingSize()
		c += e + 1 + common.BitLenToByteLen(bits.Len(uint(e)))
	}
	b := make([]byte, 10)
	l := rlp2.EncodeListPrefix(c, b)
	w.Write(b[0:l])
	for _, req := range *r {
		buf := new(bytes.Buffer)
		// buf2 := new(bytes.Buffer)
		req.EncodeRLP(buf)
		buf2 := make([]byte, buf.Len()+2)
		_ = rlp2.EncodeString(buf.Bytes(), buf2)
		w.Write(buf2)
	}
}

func (r *Requests) EncodingSize() int {
	var c int
	for _, req := range *r {
		e := req.EncodingSize()
		c += e + 1 + common.BitLenToByteLen(bits.Len(uint(e)))
	}
	return c
}

func (r *Requests) Hash() libcommon.Hash {
	return rlpHash(r)
}

func (r Requests) Deposits() DepositRequests {
	deposits := make(DepositRequests, 0, len(r))
	for _, req := range r {
		if req.RequestType() == DepositRequestType {
			deposits = append(deposits, req.(*DepositRequest))
		}
	}
	return deposits
}

func (r *Requests) Consolidations() ConsolidationRequests {
	crs := make(ConsolidationRequests, 0, len(*r))
	for _, req := range *r {
		if req.RequestType() == ConsolidationRequestType {
			crs = append(crs, req.(*ConsolidationRequest))
		}
	}
	return crs
}

func (r *Requests) Withdrawals() WithdrawalRequests {
	wrs := make(WithdrawalRequests, 0, len(*r))
	for _, req := range *r {
		if req.RequestType() == WithdrawalRequestType {
			wrs = append(wrs, req.(*WithdrawalRequest))
		}
	}
	return wrs
}

func MarshalRequestsBinary(requests Requests) ([][]byte, error) {
	ret := make([][]byte, 0)
	for _, req := range requests {
		buf := new(bytes.Buffer)
		if err := req.EncodeRLP(buf); err != nil {
			return nil, err
		}
		ret = append(ret, buf.Bytes())
	}
	return ret, nil
}

func UnmarshalRequestsFromBinary(requests [][]byte) (reqs Requests, err error) {
	for _, b := range requests {
		switch b[0] {
		case DepositRequestType:
			d := new(DepositRequest)
			if err = d.DecodeRLP(b); err != nil {
				return nil, err
			}
			reqs = append(reqs, d)
		case WithdrawalRequestType:
			w := new(WithdrawalRequest)
			if err = w.DecodeRLP(b); err != nil {
				return nil, err
			}
			reqs = append(reqs, w)
		case ConsolidationRequestType:
			w := new(ConsolidationRequest)
			if err = w.DecodeRLP(b); err != nil {
				return nil, err
			}
			reqs = append(reqs, w)
		default:
			continue
		}
	}
	return
}

func (r Requests) Len() int { return len(r) }

// EncodeIndex encodes the i'th request to w. Note that this does not check for errors
// because we assume that *request will only ever contain valid requests that were either
// constructed by decoding or via public API in this package.
func (r Requests) EncodeIndex(i int, w *bytes.Buffer) {
	r[i].EncodeRLP(w)
}

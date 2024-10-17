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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"

	"github.com/erigontech/erigon/accounts/abi"
)

const (
	BLSPubKeyLen             = 48
	WithdrawalCredentialsLen = 32 // withdrawalCredentials size
	BLSSigLen                = 96 // signature size
)

var (
	// DepositABI is an ABI instance of beacon chain deposit events.
	DepositABI   = abi.ABI{Events: map[string]abi.Event{"DepositEvent": depositEvent}}
	bytesT, _    = abi.NewType("bytes", "", nil)
	depositEvent = abi.NewEvent("DepositEvent", "DepositEvent", false, abi.Arguments{
		{Name: "pubkey", Type: bytesT, Indexed: false},
		{Name: "withdrawal_credentials", Type: bytesT, Indexed: false},
		{Name: "amount", Type: bytesT, Indexed: false},
		{Name: "signature", Type: bytesT, Indexed: false},
		{Name: "index", Type: bytesT, Indexed: false}},
	)
)

type DepositRequest struct {
	Pubkey                [BLSPubKeyLen]byte // public key of validator
	WithdrawalCredentials libcommon.Hash     // beneficiary of the validator
	Amount                uint64             // deposit size in Gwei
	Signature             [BLSSigLen]byte    // signature over deposit msg
	Index                 uint64             // deposit count value
}

type DepositRequestJson struct {
	Pubkey                string         `json:"pubkey"`
	WithdrawalCredentials libcommon.Hash `json:"withdrawalCredentials"`
	Amount                hexutil.Uint64 `json:"amount"`
	Signature             string         `json:"signature"`
	Index                 hexutil.Uint64 `json:"index"`
}

func (d *DepositRequest) RequestType() byte { return DepositRequestType }
func (d *DepositRequest) EncodeRLP(w io.Writer) (err error) {
	b := []byte{}
	b = append(b, DepositRequestType)
	b = append(b, d.Pubkey[:]...)
	b = append(b, d.WithdrawalCredentials.Bytes()...)
	b = binary.LittleEndian.AppendUint64(b, d.Amount)
	b = append(b, d.Signature[:]...)
	b = binary.LittleEndian.AppendUint64(b, d.Index)

	if _, err = w.Write(b); err != nil {
		return err
	}
	return
}
func (d *DepositRequest) DecodeRLP(input []byte) error {
	if len(input) != d.EncodingSize() {
		return errors.New("Error decoding Deposit Request RLP - size mismatch in input")
	}
	i := 1
	d.Pubkey = [BLSPubKeyLen]byte(input[i : i+BLSPubKeyLen])
	i += BLSPubKeyLen
	d.WithdrawalCredentials = libcommon.Hash(input[i : i+WithdrawalCredentialsLen])
	i += WithdrawalCredentialsLen
	d.Amount = binary.LittleEndian.Uint64(input[i : i+8])
	i += 8
	d.Signature = [BLSSigLen]byte(input[i : i+BLSSigLen])
	i += BLSSigLen
	d.Index = binary.LittleEndian.Uint64(input[i : i+8])
	return nil
}

func (d *DepositRequest) Encode() []byte {
	b := bytes.NewBuffer([]byte{})
	d.EncodeRLP(b)
	return b.Bytes()
}

func (d *DepositRequest) copy() Request {
	return &DepositRequest{
		Pubkey:                d.Pubkey,
		WithdrawalCredentials: d.WithdrawalCredentials,
		Amount:                d.Amount,
		Signature:             d.Signature,
		Index:                 d.Index,
	}
}

func (d *DepositRequest) EncodingSize() (encodingSize int) {
	return 1 + BLSPubKeyLen + WithdrawalCredentialsLen + 8 + BLSSigLen + 8 //
}

func (d *DepositRequest) MarshalJSON() ([]byte, error) {
	tt := DepositRequestJson{
		Pubkey:                hexutility.Encode(d.Pubkey[:]),
		WithdrawalCredentials: d.WithdrawalCredentials,
		Amount:                hexutil.Uint64(d.Amount),
		Signature:             hexutility.Encode(d.Signature[:]),
		Index:                 hexutil.Uint64(d.Index),
	}
	return json.Marshal(tt)
}

func (d *DepositRequest) UnmarshalJSON(input []byte) error {
	tt := DepositRequestJson{}
	err := json.Unmarshal(input, &tt)
	if err != nil {
		return err
	}
	pubkey, err := hexutil.Decode(tt.Pubkey)
	if err != nil {
		return err
	}
	if len(pubkey) != BLSPubKeyLen {
		return errors.New("DepositRequest Pubkey len not equal to BLSPubkeyLen after UnmarshalJSON")
	}
	sig, err := hexutil.Decode(tt.Signature)
	if err != nil {
		return err
	}
	if len(sig) != BLSSigLen {
		return errors.New("DepositRequest Signature len not equal to BLSSiglen after UnmarshalJSON")
	}

	d.Pubkey = [BLSPubKeyLen]byte(pubkey)
	d.Signature = [BLSSigLen]byte(sig)
	d.WithdrawalCredentials = tt.WithdrawalCredentials
	d.Amount = tt.Amount.Uint64()
	d.Index = tt.Index.Uint64()
	return nil
}

// field type overrides for abi upacking
type depositUnpacking struct {
	Pubkey                []byte
	WithdrawalCredentials []byte
	Amount                []byte
	Signature             []byte
	Index                 []byte
}

// unpackIntoDeposit unpacks a serialized DepositEvent.
func unpackIntoDeposit(data []byte) (*DepositRequest, error) {
	var du depositUnpacking
	if err := DepositABI.UnpackIntoInterface(&du, "DepositEvent", data); err != nil {
		return nil, err
	}
	var d DepositRequest
	copy(d.Pubkey[:], du.Pubkey)
	copy(d.WithdrawalCredentials[:], du.WithdrawalCredentials)
	d.Amount = binary.LittleEndian.Uint64(du.Amount)
	copy(d.Signature[:], du.Signature)
	d.Index = binary.LittleEndian.Uint64(du.Index)

	return &d, nil
}

// ParseDepositLogs extracts the EIP-6110 deposit values from logs emitted by
// BeaconDepositContract.
func ParseDepositLogs(logs []*Log, depositContractAddress libcommon.Address) (Requests, error) {
	deposits := Requests{}
	for _, log := range logs {
		if log.Address == depositContractAddress {
			d, err := unpackIntoDeposit(log.Data)
			if err != nil {
				return nil, fmt.Errorf("unable to parse deposit data: %v", err)
			}
			deposits = append(deposits, d)
		}
	}
	return deposits, nil
}

type DepositRequests []*DepositRequest

// Len returns the length of s.
func (s DepositRequests) Len() int { return len(s) }

// EncodeIndex encodes the i'th withdrawal request to w.
func (s DepositRequests) EncodeIndex(i int, w *bytes.Buffer) {
	s[i].EncodeRLP(w)
}

// Requests creates a deep copy of each deposit and returns a slice of the
// withdrwawal requests as Request objects.
func (s DepositRequests) Requests() (reqs Requests) {
	for _, d := range s {
		reqs = append(reqs, d)
	}
	return
}

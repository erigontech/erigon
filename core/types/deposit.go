package types

import (
	"bytes"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/rlp"
)

const (
	pLen = 48 // pubkey size
	wLen = 32 // withdrawalCredentials size
	sLen = 96 // signature size
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

type Deposit struct {
	Pubkey                [pLen]byte
	WithdrawalCredentials libcommon.Hash
	Amount                uint64
	Signature             [sLen]byte
	Index                 uint64
}

func (d *Deposit) requestType() byte               { return DepositRequestType }
func (d *Deposit) encodeRLP(w *bytes.Buffer) error { return rlp.Encode(w, d) }
func (d *Deposit) decodeRLP(data []byte) error     { return rlp.DecodeBytes(data, d) }
func (d *Deposit) copy() RequestData {
	return &Deposit{
		Pubkey:                d.Pubkey,
		WithdrawalCredentials: d.WithdrawalCredentials,
		Amount:                d.Amount,
		Signature:             d.Signature,
		Index:                 d.Index,
	}
}

type Deposits []*Deposit

func (ds Deposits) ToRequests() (reqs Requests) {
	for _, d := range ds {
		reqs = append(reqs, NewRequest(d))
	}
	return
}

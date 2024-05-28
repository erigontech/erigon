package types

import (
	"encoding/binary"
	"fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/rlp"
)

const (
	DepositRequestType       byte = 0x00
	BLSPubKeyLen                  = 48
	WithdrawalCredentialsLen      = 32 // withdrawalCredentials size
	BLSSigLen                     = 96 // signature size
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
	Pubkey                [BLSPubKeyLen]byte `json:"pubkey"`                // public key of validator
	WithdrawalCredentials libcommon.Hash     `json:"withdrawalCredentials"` // beneficiary of the validator
	Amount                uint64             `json:"amount"`                // deposit size in Gwei
	Signature             [BLSSigLen]byte    `json:"signature"`             // signature over deposit msg
	Index                 uint64             `json:"index"`                 // deposit count value
}

func (d *Deposit) RequestType() byte { return DepositRequestType }
func (d *Deposit) EncodeRLP(w io.Writer) error {
	// todo @somnathb1 fix this
	return rlp.Encode(w, d)
}
func (d *Deposit) DecodeRLP(data []byte) error { return rlp.DecodeBytes(data, d) }
func (d *Deposit) copy() Request {
	return &Deposit{
		Pubkey:                d.Pubkey,
		WithdrawalCredentials: d.WithdrawalCredentials,
		Amount:                d.Amount,
		Signature:             d.Signature,
		Index:                 d.Index,
	}
}

func (d *Deposit) EncodingSize() (encodingSize int) {
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(d.Amount)
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(d.Index)

	encodingSize += 180 // 1 + 48 + 1 + 32 + 1 + 1 + 96 (0x80 + pLen, 0x80 + wLen, 0xb8 + 2 + sLen)
	encodingSize = rlp2.ListPrefixLen(encodingSize)
	encodingSize += 1 //RequestType
	return
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
func unpackIntoDeposit(data []byte) (*Deposit, error) {
	var du depositUnpacking
	if err := DepositABI.UnpackIntoInterface(&du, "DepositEvent", data); err != nil {
		return nil, err
	}
	var d Deposit
	copy(d.Pubkey[:], du.Pubkey)
	copy(d.WithdrawalCredentials[:], du.WithdrawalCredentials)
	d.Amount = binary.LittleEndian.Uint64(du.Amount)
	copy(d.Signature[:], du.Signature)
	d.Index = binary.LittleEndian.Uint64(du.Index)

	return &d, nil
}

// ParseDepositLogs extracts the EIP-6110 deposit values from logs emitted by
// BeaconDepositContract.
func ParseDepositLogs(logs []*Log, depositContractAddress *libcommon.Address) (Requests, error) {
	var deposits Requests
	for _, log := range logs {
		if log.Address == *depositContractAddress {
			d, err := unpackIntoDeposit(log.Data)
			if err != nil {
				return nil, fmt.Errorf("unable to parse deposit data: %v", err)
			}
			deposits = append(deposits, d)
		}
	}
	return deposits, nil
}

type Deposits []*Deposit

func (ds Deposits) ToRequests() (reqs Requests) {
	for _, d := range ds {
		reqs = append(reqs, d)
	}
	return
}

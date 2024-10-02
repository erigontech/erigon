package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/rlp"
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
	var buf bytes.Buffer
	bb := make([]byte, 10)
	if err = rlp.Encode(&buf, d.Pubkey); err != nil {
		return err
	}
	if err = rlp.Encode(&buf, d.WithdrawalCredentials); err != nil {
		return err
	}
	if err = rlp.EncodeInt(d.Amount, &buf, bb); err != nil {
		return err
	}
	if err = rlp.Encode(&buf, d.Signature); err != nil {
		return err
	}
	if err = rlp.EncodeInt(d.Index, &buf, bb); err != nil {
		return err
	}
	rlp2.EncodeListPrefix(buf.Len(), bb)
	if _, err = w.Write([]byte{DepositRequestType}); err != nil {
		return err
	}
	if _, err = w.Write(bb[0:2]); err != nil {
		return err
	}
	if _, err = w.Write(buf.Bytes()); err != nil {
		return err
	}

	return
}
func (d *DepositRequest) DecodeRLP(input []byte) error { return rlp.DecodeBytes(input[1:], d) }
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
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(d.Amount)
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(d.Index)

	encodingSize += 180 // 1 + 48 + 1 + 32 + 1 + 1 + 96 (0x80 + pLen, 0x80 + wLen, 0xb8 + 2 + sLen)
	encodingSize += rlp2.ListPrefixLen(encodingSize)
	encodingSize += 1 //RequestType
	return
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

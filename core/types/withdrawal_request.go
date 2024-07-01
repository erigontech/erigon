package types

import (
	"bytes"
	// "fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/rlp"
)

// EIP-7002 Withdrawal Request see https://github.com/ethereum/EIPs/blob/master/EIPS/eip-7002.md
type WithdrawalRequest struct {
	SourceAddress   libcommon.Address
	ValidatorPubkey [BLSPubKeyLen]byte // bls
	Amount          uint64
}

func (w *WithdrawalRequest) RequestType() byte {
	return WithdrawalRequestType
}

// encodingSize implements RequestData.
func (w *WithdrawalRequest) EncodingSize() (encodingSize int) {
	encodingSize += 70 // 1 + 20 + 1 + 48 (0x80 + addrSize, 0x80 + BLSPubKeyLen)
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(w.Amount)
	encodingSize += rlp2.ListPrefixLen(encodingSize)
	encodingSize += 1 // RequestType
	return
}
func (w *WithdrawalRequest) EncodeRLP(b io.Writer) (err error) {
	var buf bytes.Buffer
	bb := make([]byte, 10)
	if err = rlp.Encode(&buf, w.SourceAddress); err != nil {
		return err
	}
	if err = rlp.Encode(&buf, w.ValidatorPubkey); err != nil {
		return err
	}
	if err = rlp.EncodeInt(w.Amount, &buf, bb); err != nil {
		return err
	}
	rlp2.EncodeListPrefix(buf.Len(), bb)

	if _, err = b.Write([]byte{WithdrawalRequestType}); err != nil {
		return err
	}
	if _, err = b.Write(bb[0:2]); err != nil {
		return err
	}
	if _, err = b.Write(buf.Bytes()); err != nil {
		return err
	}
	return
}

func (w *WithdrawalRequest) DecodeRLP(input []byte) error { return rlp.DecodeBytes(input[1:], w) }
func (w *WithdrawalRequest) copy() Request {
	return &WithdrawalRequest{
		SourceAddress:   w.SourceAddress,
		ValidatorPubkey: w.ValidatorPubkey,
		Amount:          w.Amount,
	}
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

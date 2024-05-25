package types

import (
	"bytes"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/rlp"
)

type WithdrawalRequest struct {
	SourceAddress   libcommon.Address
	ValidatorPubkey [pLen]byte // bls
	Amount          uint64
}

// encodingSize implements RequestData.
func (w *WithdrawalRequest) encodingSize() (encodingSize int) {
	encodingSize += 180 // 1 + 20 + 1 + 48 (0x80 + addrSize, 0x80 + pLen)
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(w.Amount)
	return encodingSize
}

type WithdrawalRequests []*WithdrawalRequest

// Len returns the length of s.
func (s WithdrawalRequests) Len() int { return len(s) }

// EncodeIndex encodes the i'th withdrawal request to w.
func (s WithdrawalRequests) EncodeIndex(i int, w *bytes.Buffer) {
	rlp.Encode(w, s[i])
}

// Requests creates a deep copy of each deposit and returns a slice of the
// withdrwawal requests as Request objects.
func (s WithdrawalRequests) Requests() (reqs Requests) {
	for _, d := range s {
		reqs = append(reqs, NewRequest(d))
	}
	return
}

func (w *WithdrawalRequest) requestType() byte            { return WithdrawalRequestType }
func (w *WithdrawalRequest) encodeRLP(b *bytes.Buffer) error { return rlp.Encode(b, w) }
func (w *WithdrawalRequest) decodeRLP(input []byte) error    { return rlp.DecodeBytes(input, w) }
func (w *WithdrawalRequest) copy() RequestData {
	return &WithdrawalRequest{
		SourceAddress:   w.SourceAddress,
		ValidatorPubkey: w.ValidatorPubkey,
		Amount:          w.Amount,
	}
}

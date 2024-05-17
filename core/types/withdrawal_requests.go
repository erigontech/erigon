package types

import (
	"bytes"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/rlp"
)

type WithdrawalRequest struct {
	SourceAddress   libcommon.Address
	ValidatorPubkey [PublicKeyLen]byte // bls
	Amount          uint64
}

type WithdrawalRequests []*WithdrawalRequest

func (wr WithdrawalRequests) ToRequests() Requests {
	reqs := make(Requests, 0, len(wr))
	for _, d := range wr {
		reqs = append(reqs, NewRequest(d))
	}
	return reqs
}

func (wr WithdrawalRequest) encodeRLP(w *bytes.Buffer) error { return rlp.Encode(w, wr) }
func (wr WithdrawalRequest) decodeRLP(data []byte) error     { return rlp.DecodeBytes(data, wr) }

func (wr WithdrawalRequest) requestType() byte { return WithdrawalRequestType }
func (wr WithdrawalRequest) copy() RequestData {
	return &WithdrawalRequest{
		SourceAddress:   wr.SourceAddress,
		ValidatorPubkey: wr.ValidatorPubkey,
		Amount:          wr.Amount,
	}

}
func (wr WithdrawalRequest) encodingSize() int {
	// TODO
	return 0
}

package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

// EIP-7251 Consolidation Request see https://github.com/ethereum/EIPs/blob/master/EIPS/eip-7251.md
type ConsolidationRequest struct {
	RequestData [ConsolidationRequestDataLen]byte
}

type ConsolidationRequestJson struct {
	RequestData string
}

func (c *ConsolidationRequest) RequestType() byte {
	return ConsolidationRequestType
}

func (c *ConsolidationRequest) EncodingSize() (encodingSize int) {
	return ConsolidationRequestDataLen + 1 // RequestType
}
func (c *ConsolidationRequest) EncodeRLP(b io.Writer) (err error) {

	if _, err = b.Write([]byte{ConsolidationRequestType}); err != nil {
		return err
	}
	if _, err = b.Write(c.RequestData[:]); err != nil {
		return err
	}
	return
}

func (c *ConsolidationRequest) MarshalJSON() ([]byte, error) {
	tt := ConsolidationRequestJson{
		RequestData: hexutility.Encode(c.RequestData[:]),
	}
	return json.Marshal(tt)
}

func (c *ConsolidationRequest) UnmarshalJSON(input []byte) error {
	tt := ConsolidationRequestJson{}
	err := json.Unmarshal(input, &tt)
	if err != nil {
		return err
	}
	if len(tt.RequestData) != ConsolidationRequestDataLen {
		return errors.New("Cannot unmarshal consolidation request data, length mismatch")
	}
	c.RequestData = [ConsolidationRequestDataLen]byte(hexutility.MustDecodeString(tt.RequestData))
	return nil
}

func (c *ConsolidationRequest) copy() Request {
	return &ConsolidationRequest{
		RequestData: [ConsolidationRequestDataLen]byte(bytes.Clone(c.RequestData[:])),
	}
}

func (c *ConsolidationRequest) DecodeRLP(input []byte) error {
	if len(input) != ConsolidationRequestDataLen+1 {
		return errors.New("Incorrect size for decoding ConsolidationRequest RLP")
	}
	c.RequestData = [ConsolidationRequestDataLen]byte(input[1:])
	return nil
}

func (c *ConsolidationRequest) Encode() []byte {
	return append([]byte{ConsolidationRequestType}, c.RequestData[:]...)
}

type ConsolidationRequests []*ConsolidationRequest

// Len returns the length of s.
func (s ConsolidationRequests) Len() int { return len(s) }

// EncodeIndex encodes the i'th ConsolidationRequest to w.
func (s ConsolidationRequests) EncodeIndex(i int, w *bytes.Buffer) {
	s[i].EncodeRLP(w)
}

// Requests creates a deep copy of each Consolidation Request and returns a slice (as Requests).
func (s ConsolidationRequests) Requests() (reqs Requests) {
	for _, d := range s {
		reqs = append(reqs, d)
	}
	return
}

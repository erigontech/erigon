package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/rlp"
)

// EIP-7251 Consolidation Request see https://github.com/ethereum/EIPs/blob/master/EIPS/eip-7251.md
type ConsolidationRequest struct {
	SourceAddress libcommon.Address
	SourcePubKey  [BLSPubKeyLen]byte
	TargetPubKey  [BLSPubKeyLen]byte
}

type ConsolidationRequestJson struct {
	SourceAddress libcommon.Address `json:"sourceAddress"`
	SourcePubKey  string            `json:"sourcePubkey"`
	TargetPubKey  string            `json:"targetPubkey"`
}

func (w *ConsolidationRequest) RequestType() byte {
	return ConsolidationRequestType
}

func (w *ConsolidationRequest) EncodingSize() (encodingSize int) {
	encodingSize += 119 // 1 + 20 + 1 + 48 + 1 + 48 (0x80 + addrSize, 0x80 + BLSPubKeyLen, 0x80 + BLSPubKeyLen)
	encodingSize += rlp2.ListPrefixLen(encodingSize)
	encodingSize += 1 // RequestType
	return
}
func (w *ConsolidationRequest) EncodeRLP(b io.Writer) (err error) {
	var buf bytes.Buffer
	bb := make([]byte, 10)
	if err = rlp.Encode(&buf, w.SourceAddress); err != nil {
		return err
	}
	if err = rlp.Encode(&buf, w.SourcePubKey); err != nil {
		return err
	}
	if err = rlp.Encode(&buf, w.TargetPubKey); err != nil {
		return err
	}
	l := rlp2.EncodeListPrefix(buf.Len(), bb)

	if _, err = b.Write([]byte{ConsolidationRequestType}); err != nil {
		return err
	}
	if _, err = b.Write(bb[0:l]); err != nil {
		return err
	}
	if _, err = b.Write(buf.Bytes()); err != nil {
		return err
	}
	return
}

func (d *ConsolidationRequest) MarshalJSON() ([]byte, error) {
	tt := ConsolidationRequestJson{
		SourceAddress: d.SourceAddress,
		SourcePubKey:  hexutility.Encode(d.SourcePubKey[:]),
		TargetPubKey:  hexutility.Encode(d.TargetPubKey[:]),
	}
	return json.Marshal(tt)
}

func (d *ConsolidationRequest) UnmarshalJSON(input []byte) error {
	tt := ConsolidationRequestJson{}
	err := json.Unmarshal(input, &tt)
	if err != nil {
		return err
	}
	sourceKey, err := hexutil.Decode(tt.SourcePubKey)
	if err != nil {
		return err
	}
	if len(sourceKey) != BLSPubKeyLen {
		return fmt.Errorf("Unmarshalled pubkey not equal to BLSPubkeyLen")
	}
	targetKey, err := hexutil.Decode(tt.TargetPubKey)
	if err != nil {
		return err
	}
	if len(targetKey) != BLSSigLen {
		return fmt.Errorf("Unmarshalled TargetPubKey len not equal to BLSSiglen")
	}
	d.SourceAddress = tt.SourceAddress
	d.SourcePubKey = [48]byte(sourceKey)
	d.TargetPubKey = [48]byte(targetKey)
	return nil
}

func (w *ConsolidationRequest) DecodeRLP(input []byte) error { return rlp.DecodeBytes(input[1:], w) }
func (w *ConsolidationRequest) copy() Request {
	return &ConsolidationRequest{
		SourceAddress: w.SourceAddress,
		SourcePubKey:  w.SourcePubKey,
		TargetPubKey:  w.TargetPubKey,
	}
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

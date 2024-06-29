package types

import (
	"bytes"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/rlp"
)

// EIP-7251 Consolidation Request see https://github.com/ethereum/EIPs/blob/master/EIPS/eip-7251.md
type ConsolidationRequest struct {
	SourceAddress libcommon.Address
	SourcePubKey  [BLSPubKeyLen]byte
	TargetPubKey  [BLSPubKeyLen]byte
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

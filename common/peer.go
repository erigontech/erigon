package common

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
)

type PeerID [64]byte

func BytesToPeerID(b []byte) PeerID {
	var h PeerID
	h.SetBytes(b)
	return h
}

// HexToHash sets byte representation of s to hash.
// If b is larger than len(h), b will be cropped from the left.
func HexToPeerID(s string) Hash { return BytesToHash(hexutility.FromHex(s)) }

// Bytes gets the byte representation of the underlying hash.
func (h PeerID) Bytes() []byte { return h[:] }

// Hex converts a hash to a hex string.
func (h PeerID) Hex() string { return hexutility.Encode(h[:]) }

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (h PeerID) TerminalString() string {
	return fmt.Sprintf("%x…%x", h[:3], h[61:])
}

// String implements the stringer interface and is used also by the logger when
// doing full logging into a file.
func (h PeerID) String() string {
	return h.Hex()[:8]
}

// SetBytes sets the hash to the value of b.
// If b is larger than len(h), b will be cropped from the left.
func (h *PeerID) SetBytes(b []byte) {
	if len(b) > len(h) {
		b = b[len(b)-length.PeerID:]
	}

	copy(h[length.PeerID-len(b):], b)
}

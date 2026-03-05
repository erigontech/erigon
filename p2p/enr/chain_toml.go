package enr

import (
	"io"

	"github.com/erigontech/erigon/execution/rlp"
)

// ChainToml is the "chain-toml" ENR key. It advertises the node's current
// snapshot manifest (chain.toml) as a torrent info-hash and frozen transaction count.
// This enables decentralized discovery of snapshot info-hashes via discv5.
type ChainToml struct {
	FrozenTx uint64   // max frozen transaction number (monotonically increasing)
	InfoHash [20]byte // BitTorrent V1 info-hash (SHA1) of the chain.toml torrent
}

func (v ChainToml) ENRKey() string { return "chain-toml" }

// EncodeRLP implements rlp.Encoder.
func (v ChainToml) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &chainTomlRLP{FrozenTx: v.FrozenTx, InfoHash: v.InfoHash})
}

// DecodeRLP implements rlp.Decoder.
func (v *ChainToml) DecodeRLP(s *rlp.Stream) error {
	var dec chainTomlRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	v.FrozenTx = dec.FrozenTx
	v.InfoHash = dec.InfoHash
	return nil
}

// chainTomlRLP is the RLP encoding helper for ChainToml.
type chainTomlRLP struct {
	FrozenTx uint64
	InfoHash [20]byte
}

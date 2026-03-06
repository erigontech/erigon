package enr

import (
	"io"

	"github.com/erigontech/erigon/execution/rlp"
)

// ChainToml is the "chain-toml" ENR key. It advertises the node's current
// snapshot manifest (chain.toml) as a torrent info-hash with metadata about
// the entries it contains. This enables decentralized discovery of snapshot
// info-hashes via discv5.
//
// AuthoritativeTx and KnownTx distinguish two classes of entries:
//   - Authoritative: entries the node can vouch for (local disk + preverified.toml)
//   - Known: all entries including those heard from peers (≥ AuthoritativeTx)
//
// These ranges are non-interleaved: authoritative entries cover tx 0..AuthoritativeTx,
// known entries extend to KnownTx. The receiver decides its trust policy.
type ChainToml struct {
	AuthoritativeTx uint64   // max tx for entries from local disk + preverified.toml
	KnownTx         uint64   // max tx for all entries (≥ AuthoritativeTx)
	InfoHash        [20]byte // BitTorrent V1 info-hash (SHA1) of the chain.toml torrent
}

func (v ChainToml) ENRKey() string { return "chain-toml" }

// EncodeRLP implements rlp.Encoder.
func (v ChainToml) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &chainTomlRLP{AuthoritativeTx: v.AuthoritativeTx, KnownTx: v.KnownTx, InfoHash: v.InfoHash})
}

// DecodeRLP implements rlp.Decoder.
func (v *ChainToml) DecodeRLP(s *rlp.Stream) error {
	var dec chainTomlRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	v.AuthoritativeTx = dec.AuthoritativeTx
	v.KnownTx = dec.KnownTx
	v.InfoHash = dec.InfoHash
	return nil
}

// chainTomlRLP is the RLP encoding helper for ChainToml.
type chainTomlRLP struct {
	AuthoritativeTx uint64
	KnownTx         uint64
	InfoHash        [20]byte
}

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
// AuthoritativeBlocks and KnownBlocks distinguish two classes of entries,
// in block-count units:
//   - Authoritative: entries the node can vouch for (local disk + preverified.toml).
//     Matches snapcfg.Cfg.ExpectBlocks for the registry portion.
//   - Known: all entries including those heard from peers (≥ AuthoritativeBlocks).
//
// These ranges are non-interleaved: authoritative entries cover blocks
// 0..AuthoritativeBlocks, known entries extend to KnownBlocks. The receiver
// decides its trust policy.
//
// RLP encoding is positional — field names can change without breaking wire
// compatibility, so the earlier AuthoritativeBlocks/KnownBlocks names (which were
// misleading — they were always populated from block counts, never txNums)
// were renamed in-place.
type ChainToml struct {
	AuthoritativeBlocks uint64   // max block for entries from local disk + preverified.toml
	KnownBlocks         uint64   // max block for all entries (≥ AuthoritativeBlocks)
	InfoHash            [20]byte // BitTorrent V1 info-hash (SHA1) of the chain.toml torrent
}

func (v ChainToml) ENRKey() string { return "chain-toml" }

// EncodeRLP implements rlp.Encoder.
func (v ChainToml) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &chainTomlRLP{AuthoritativeBlocks: v.AuthoritativeBlocks, KnownBlocks: v.KnownBlocks, InfoHash: v.InfoHash})
}

// DecodeRLP implements rlp.Decoder.
func (v *ChainToml) DecodeRLP(s *rlp.Stream) error {
	var dec chainTomlRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	v.AuthoritativeBlocks = dec.AuthoritativeBlocks
	v.KnownBlocks = dec.KnownBlocks
	v.InfoHash = dec.InfoHash
	return nil
}

// chainTomlRLP is the RLP encoding helper for ChainToml. Field order must
// match ChainToml exactly — RLP encoding is positional.
type chainTomlRLP struct {
	AuthoritativeBlocks uint64
	KnownBlocks         uint64
	InfoHash            [20]byte
}

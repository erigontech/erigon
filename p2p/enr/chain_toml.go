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
// DomainSteps and MergeDepth are V2 extensions that advertise state-domain
// coverage. Old nodes that decode only the first 3 fields will ignore the
// extras (RLP tolerates trailing elements in lists). New nodes decoding an
// old 3-field entry will see DomainSteps=0 and MergeDepth=0 (zero values).
//
// RLP encoding is positional — field order must be preserved across versions.
type ChainToml struct {
	AuthoritativeBlocks uint64   // max block for entries from local disk + preverified.toml
	KnownBlocks         uint64   // max block for all entries (≥ AuthoritativeBlocks)
	InfoHash            [20]byte // BitTorrent V1 info-hash (SHA1) of the chain.toml torrent
	DomainSteps         uint64   // total domain steps covered (0 = V1-only, no domain data)
	MergeDepth          uint64   // largest canonical file size in steps (0 = unknown)
}

func (v ChainToml) ENRKey() string { return "chain-toml" }

// EncodeRLP implements rlp.Encoder.
func (v ChainToml) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &chainTomlRLP{
		AuthoritativeBlocks: v.AuthoritativeBlocks,
		KnownBlocks:         v.KnownBlocks,
		InfoHash:            v.InfoHash,
		DomainSteps:         v.DomainSteps,
		MergeDepth:          v.MergeDepth,
	})
}

// DecodeRLP implements rlp.Decoder. Tolerates old 3-field entries by
// defaulting DomainSteps and MergeDepth to zero when absent.
func (v *ChainToml) DecodeRLP(s *rlp.Stream) error {
	var dec chainTomlRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	v.AuthoritativeBlocks = dec.AuthoritativeBlocks
	v.KnownBlocks = dec.KnownBlocks
	v.InfoHash = dec.InfoHash
	v.DomainSteps = dec.DomainSteps
	v.MergeDepth = dec.MergeDepth
	return nil
}

// chainTomlRLP is the RLP encoding helper for ChainToml. Field order must
// match ChainToml exactly — RLP encoding is positional. New fields are
// appended at the end so old decoders can skip them gracefully.
type chainTomlRLP struct {
	AuthoritativeBlocks uint64
	KnownBlocks         uint64
	InfoHash            [20]byte
	DomainSteps         uint64
	MergeDepth          uint64
}

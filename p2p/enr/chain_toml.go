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
// ContentUCANHash is the BitTorrent info-hash of the Content UCAN
// sidecar paired with the manifest InfoHash points at. The Content UCAN
// is its own single-file torrent; a consumer fetches it by this
// info-hash to run the two-UCAN verification chain
// (docs/plans/20260520-chaintoml-ucan-flow-spec.md). Zero when the
// publisher mints no Content UCAN (no operator key, or not opted into
// the trust system). It is an info-hash, not a counter — a passive
// discv5 scraper reads nothing about republish cadence from it.
//
// V2InfoHash and MinStep are V2-manifest extensions that advertise
// the typed `chain.v2.<enr-fp>.<seq>.toml` separately from the legacy
// flat-map `chain.toml`. A V2-capable consumer prefers V2InfoHash when
// non-zero (the typed manifest carries per-entry trust + proof_root and
// the typed Blocks list); a V1-only consumer falls back to InfoHash.
// MinStep is the prune-window floor in step units — peers whose floor
// exceeds the consumer's requested step are skipped (the publisher
// no longer holds the older history). Trailing RLP fields; spec
// invariant — order is positional, new fields append, never reorder.
//
// RLP encoding is positional — field order must be preserved across versions.
type ChainToml struct {
	AuthoritativeBlocks uint64   // max block for entries from local disk + preverified.toml
	KnownBlocks         uint64   // max block for all entries (≥ AuthoritativeBlocks)
	InfoHash            [20]byte // BitTorrent V1 info-hash (SHA1) of the chain.toml torrent
	DomainSteps         uint64   // total domain steps covered (0 = V1-only, no domain data)
	MergeDepth          uint64   // largest canonical file size in steps (0 = unknown)
	ContentUCANHash     [20]byte // info-hash of the paired Content UCAN torrent; zero = none
	V2InfoHash          [20]byte // info-hash of the V2 typed manifest; zero = no V2 advertised
	MinStep             uint64   // prune-window floor in step units; 0 = full history
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
		ContentUCANHash:     v.ContentUCANHash,
		V2InfoHash:          v.V2InfoHash,
		MinStep:             v.MinStep,
	})
}

// DecodeRLP implements rlp.Decoder. Tolerates older entries with fewer
// trailing fields by leaving the missing fields at their zero values:
//   - 3-field encoders skip DomainSteps + MergeDepth + ContentUCANHash +
//     V2InfoHash + MinStep
//   - older 6-field encoders skip V2InfoHash + MinStep
//
// RLP lists permit trailing elements to be absent, so a forward-only
// extension is safe.
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
	v.ContentUCANHash = dec.ContentUCANHash
	v.V2InfoHash = dec.V2InfoHash
	v.MinStep = dec.MinStep
	return nil
}

// chainTomlRLP is the RLP encoding helper for ChainToml. Field order must
// match ChainToml exactly — RLP encoding is positional. New fields are
// appended at the end and tagged `rlp:"optional"` so older encoders that
// emit a shorter list decode cleanly into the newer struct (missing
// trailing fields default to their zero value).
type chainTomlRLP struct {
	AuthoritativeBlocks uint64
	KnownBlocks         uint64
	InfoHash            [20]byte
	DomainSteps         uint64   `rlp:"optional"`
	MergeDepth          uint64   `rlp:"optional"`
	ContentUCANHash     [20]byte `rlp:"optional"`
	V2InfoHash          [20]byte `rlp:"optional"`
	MinStep             uint64   `rlp:"optional"`
}

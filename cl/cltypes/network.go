package cltypes

import (
	"encoding/json"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type Metadata struct {
	SeqNumber uint64
	Attnets   [8]byte
	Syncnets  *[1]byte
}

func (m *Metadata) EncodeSSZ(buf []byte) ([]byte, error) {
	if m.Syncnets == nil {
		return ssz2.MarshalSSZ(buf, m.SeqNumber, m.Attnets[:])
	}
	return ssz2.MarshalSSZ(buf, m.SeqNumber, m.Attnets[:], m.Syncnets[:])
}

func (m *Metadata) EncodingSizeSSZ() (ret int) {
	ret = 8 * 2
	if m.Syncnets != nil {
		ret += 1
	}
	return
}

func (m *Metadata) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < 16 {
		return ssz.ErrLowBufferSize
	}
	m.SeqNumber = ssz.UnmarshalUint64SSZ(buf)
	copy(m.Attnets[:], buf[8:])
	if len(buf) < 17 {
		return nil
	}
	m.Syncnets = new([1]byte)
	copy(m.Syncnets[:], buf[16:17])
	return nil
}

func (m *Metadata) MarshalJSON() ([]byte, error) {
	out := map[string]interface{}{
		"seq_number": strconv.FormatUint(m.SeqNumber, 10),
		"attnets":    hexutility.Bytes(m.Attnets[:]),
	}
	if m.Syncnets != nil {
		out["syncnets"] = hexutility.Bytes(m.Syncnets[:])
	}
	// Attnets and syncnets are hex encoded
	return json.Marshal(out)
}

// Ping is a test P2P message, used to test out liveness of our peer/signaling disconnection.
type Ping struct {
	Id uint64
}

func (p *Ping) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, ssz.Uint64SSZ(p.Id)...), nil
}

func (p *Ping) EncodingSizeSSZ() int {
	return 8
}

func (p *Ping) DecodeSSZ(buf []byte, _ int) error {
	p.Id = ssz.UnmarshalUint64SSZ(buf)
	return nil
}

// Root is a SSZ wrapper around a Hash
type Root struct {
	Root libcommon.Hash
}

func (r *Root) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, r.Root[:]...), nil
}

func (r *Root) DecodeSSZ(buf []byte, _ int) error {
	copy(r.Root[:], buf)
	return nil
}

func (r *Root) EncodingSizeSSZ() int {
	return 32
}

type LightClientUpdatesByRangeRequest struct {
	StartPeriod uint64
	Count       uint64
}

func (l *LightClientUpdatesByRangeRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, &l.StartPeriod, &l.Count)
}

func (l *LightClientUpdatesByRangeRequest) DecodeSSZ(buf []byte, _ int) error {
	return ssz2.UnmarshalSSZ(buf, 0, &l.StartPeriod, &l.Count)
}

func (l *LightClientUpdatesByRangeRequest) EncodingSizeSSZ() int {
	return 16
}

/*
 * BeaconBlocksByRangeRequest is the request for getting a range of blocks.
 */
type BeaconBlocksByRangeRequest struct {
	StartSlot uint64
	Count     uint64
	Step      uint64 // Deprecated, must be set to 1
}

func (b *BeaconBlocksByRangeRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.StartSlot, b.Count, b.Step)
}

func (b *BeaconBlocksByRangeRequest) DecodeSSZ(buf []byte, v int) error {
	return ssz2.UnmarshalSSZ(buf, v, &b.StartSlot, &b.Count, &b.Step)
}

func (b *BeaconBlocksByRangeRequest) EncodingSizeSSZ() int {
	return 3 * 8
}

func (*BeaconBlocksByRangeRequest) Clone() clonable.Clonable {
	return &BeaconBlocksByRangeRequest{}
}

/*
 * Status is a P2P Message we exchange when connecting to a new Peer.
 * It contains network information about the other peer and if mismatching we drop it.
 */
type Status struct {
	ForkDigest     [4]byte
	FinalizedRoot  [32]byte
	FinalizedEpoch uint64
	HeadRoot       [32]byte
	HeadSlot       uint64
}

func (s *Status) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.ForkDigest[:], s.FinalizedRoot[:], s.FinalizedEpoch, s.HeadRoot[:], s.HeadSlot)
}

func (s *Status) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, s.ForkDigest[:], s.FinalizedRoot[:], &s.FinalizedEpoch, s.HeadRoot[:], &s.HeadSlot)
}

func (s *Status) EncodingSizeSSZ() int {
	return 84
}

type BlobsByRangeRequest struct {
	StartSlot uint64
	Count     uint64
}

func (l *BlobsByRangeRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, &l.StartSlot, &l.Count)
}

func (l *BlobsByRangeRequest) DecodeSSZ(buf []byte, _ int) error {
	return ssz2.UnmarshalSSZ(buf, 0, &l.StartSlot, &l.Count)
}

func (l *BlobsByRangeRequest) EncodingSizeSSZ() int {
	return 16
}

func (*BlobsByRangeRequest) Clone() clonable.Clonable {
	return &BlobsByRangeRequest{}
}

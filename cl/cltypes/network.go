package cltypes

import (
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/common"
)

type Metadata struct {
	SeqNumber uint64
	Attnets   uint64
	Syncnets  *uint64
}

func (m *Metadata) EncodeSSZ(buf []byte) ([]byte, error) {
	ret := buf
	ret = append(ret, ssz.Uint64SSZ(m.SeqNumber)...)
	ret = append(ret, ssz.Uint64SSZ(m.Attnets)...)
	if m.Syncnets == nil {
		return ret, nil
	}
	ret = append(ret, ssz.Uint64SSZ(*m.Syncnets)...)

	return ret, nil
}

func (m *Metadata) EncodingSizeSSZ() (ret int) {
	ret = common.BlockNumberLength * 2
	if m.Syncnets != nil {
		ret += 8
	}
	return
}

func (m *Metadata) DecodeSSZ(buf []byte, _ int) error {
	m.SeqNumber = ssz.UnmarshalUint64SSZ(buf)
	m.Attnets = ssz.UnmarshalUint64SSZ(buf[8:])
	if len(buf) < 24 {
		return nil
	}
	m.Syncnets = new(uint64)
	*m.Syncnets = ssz.UnmarshalUint64SSZ(buf[16:])
	return nil
}

// Ping is a test P2P message, used to test out liveness of our peer/signaling disconnection.
type Ping struct {
	Id uint64
}

func (p *Ping) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, ssz.Uint64SSZ(p.Id)...), nil
}

func (p *Ping) EncodingSizeSSZ() int {
	return common.BlockNumberLength
}

func (p *Ping) DecodeSSZ(buf []byte, _ int) error {
	p.Id = ssz.UnmarshalUint64SSZ(buf)
	return nil
}

// P2P Message for bootstrap
type SingleRoot struct {
	Root [32]byte
}

func (s *SingleRoot) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, s.Root[:]...), nil
}

func (s *SingleRoot) EncodingSizeSSZ() int {
	return length.Hash
}

func (s *SingleRoot) DecodeSSZ(buf []byte, _ int) error {
	copy(s.Root[:], buf)
	return nil
}

func (*SingleRoot) Clone() clonable.Clonable {
	return &SingleRoot{}
}

/*
 * LightClientUpdatesByRangeRequest that helps syncing to chain tip from a past point.
 * It takes the Period of the starting update and the amount of updates we want (MAX: 128).
 */
type LightClientUpdatesByRangeRequest struct {
	Period uint64
	Count  uint64
}

func (*LightClientUpdatesByRangeRequest) Clone() clonable.Clonable {
	return &LightClientUpdatesByRangeRequest{}
}

func (l *LightClientUpdatesByRangeRequest) DecodeSSZ(buf []byte, _ int) error {
	l.Period = ssz.UnmarshalUint64SSZ(buf)
	l.Count = ssz.UnmarshalUint64SSZ(buf[8:])
	return nil
}

func (l *LightClientUpdatesByRangeRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, append(ssz.Uint64SSZ(l.Period), ssz.Uint64SSZ(l.Count)...)...), nil
}

func (l *LightClientUpdatesByRangeRequest) EncodingSizeSSZ() int {
	return 2 * common.BlockNumberLength
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
	dst := buf
	dst = append(dst, ssz.Uint64SSZ(b.StartSlot)...)
	dst = append(dst, ssz.Uint64SSZ(b.Count)...)
	dst = append(dst, ssz.Uint64SSZ(b.Step)...)
	return dst, nil
}

func (b *BeaconBlocksByRangeRequest) DecodeSSZ(buf []byte, _ int) error {
	b.StartSlot = ssz.UnmarshalUint64SSZ(buf)
	b.Count = ssz.UnmarshalUint64SSZ(buf[8:])
	b.Step = ssz.UnmarshalUint64SSZ(buf[16:])
	return nil
}

func (b *BeaconBlocksByRangeRequest) EncodingSizeSSZ() int {
	return 3 * common.BlockNumberLength
}

func (*BeaconBlocksByRangeRequest) Clone() clonable.Clonable {
	return &BeaconBlocksByRangeRequest{}
}

/*
 * Status is a P2P Message we exchange when connecting to a new Peer.
 * It contains network information about the other peer and if mismatching we drop it.
 */
type Status struct {
	ForkDigest     [4]byte  `ssz-size:"4"`
	FinalizedRoot  [32]byte `ssz-size:"32"`
	FinalizedEpoch uint64
	HeadRoot       [32]byte `ssz-size:"32"`
	HeadSlot       uint64
}

func (s *Status) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	dst = append(dst, s.ForkDigest[:]...)
	dst = append(dst, s.FinalizedRoot[:]...)
	dst = append(dst, ssz.Uint64SSZ(s.FinalizedEpoch)...)
	dst = append(dst, s.HeadRoot[:]...)
	dst = append(dst, ssz.Uint64SSZ(s.HeadSlot)...)
	return dst, nil
}

func (s *Status) DecodeSSZ(buf []byte, _ int) error {
	copy(s.ForkDigest[:], buf)
	copy(s.FinalizedRoot[:], buf[4:])
	s.FinalizedEpoch = ssz.UnmarshalUint64SSZ(buf[36:])
	copy(s.HeadRoot[:], buf[44:])
	s.HeadSlot = ssz.UnmarshalUint64SSZ(buf[76:])
	return nil
}

func (s *Status) EncodingSizeSSZ() int {
	return 84
}

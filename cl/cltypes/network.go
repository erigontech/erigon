package cltypes

import (
	"github.com/ledgerwatch/erigon/common"
)

type Metadata struct {
	SeqNumber uint64
	Attnets   uint64
	Syncnets  *uint64
}

func (m *Metadata) MarshalSSZ() ([]byte, error) {
	ret := make([]byte, 24)
	copy(ret, MarshalUint64SSZ(m.SeqNumber))
	copy(ret[8:], MarshalUint64SSZ(m.Attnets))
	if m.Syncnets == nil {
		return ret[:16], nil
	}
	copy(ret[16:], MarshalUint64SSZ(*m.Syncnets))
	return ret, nil
}

func (m *Metadata) UnmarshalSSZ(buf []byte) error {
	m.SeqNumber = UnmarshalUint64SSZ(buf)
	m.Attnets = UnmarshalUint64SSZ(buf[8:])
	if len(buf) < 24 {
		return nil
	}
	m.Syncnets = new(uint64)
	*m.Syncnets = UnmarshalUint64SSZ(buf[16:])
	return nil
}

func (m *Metadata) SizeSSZ() (ret int) {
	ret = common.BlockNumberLength * 2
	if m.Syncnets != nil {
		ret += 8
	}
	return
}

// Ping is a test P2P message, used to test out liveness of our peer/signaling disconnection.
type Ping struct {
	Id uint64
}

func (p *Ping) MarshalSSZ() ([]byte, error) {
	ret := make([]byte, p.SizeSSZ())
	copy(ret, MarshalUint64SSZ(p.Id))
	return ret, nil
}

func (p *Ping) UnmarshalSSZ(buf []byte) error {
	p.Id = UnmarshalUint64SSZ(buf)
	return nil
}

func (p *Ping) SizeSSZ() int {
	return common.BlockNumberLength
}

// P2P Message for bootstrap
type SingleRoot struct {
	Root [32]byte
}

func (s *SingleRoot) MarshalSSZ() ([]byte, error) {
	return s.Root[:], nil
}

func (s *SingleRoot) UnmarshalSSZ(buf []byte) error {
	copy(s.Root[:], buf)
	return nil
}

func (s *SingleRoot) SizeSSZ() int {
	return common.HashLength
}

/*
 * LightClientUpdatesByRangeRequest that helps syncing to chain tip from a past point.
 * It takes the Period of the starting update and the amount of updates we want (MAX: 128).
 */
type LightClientUpdatesByRangeRequest struct {
	Period uint64
	Count  uint64
}

func (l *LightClientUpdatesByRangeRequest) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, l.SizeSSZ())
	copy(buf, MarshalUint64SSZ(l.Period))
	copy(buf[8:], MarshalUint64SSZ(l.Count))
	return buf, nil
}

func (l *LightClientUpdatesByRangeRequest) UnmarshalSSZ(buf []byte) error {
	l.Period = UnmarshalUint64SSZ(buf)
	l.Count = UnmarshalUint64SSZ(buf[8:])
	return nil
}

func (l *LightClientUpdatesByRangeRequest) SizeSSZ() int {
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

func (b *BeaconBlocksByRangeRequest) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, b.SizeSSZ())
	copy(buf, MarshalUint64SSZ(b.StartSlot))
	copy(buf[8:], MarshalUint64SSZ(b.Count))
	copy(buf[16:], MarshalUint64SSZ(b.Step))
	return buf, nil
}

func (b *BeaconBlocksByRangeRequest) UnmarshalSSZ(buf []byte) error {
	b.StartSlot = UnmarshalUint64SSZ(buf)
	b.Count = UnmarshalUint64SSZ(buf[8:])
	b.Step = UnmarshalUint64SSZ(buf[16:])
	return nil
}

func (b *BeaconBlocksByRangeRequest) SizeSSZ() int {
	return 3 * common.BlockNumberLength
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

func (s *Status) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, s.SizeSSZ())
	copy(buf, s.ForkDigest[:])
	copy(buf[4:], s.FinalizedRoot[:])
	copy(buf[36:], MarshalUint64SSZ(s.FinalizedEpoch))
	copy(buf[44:], s.HeadRoot[:])
	copy(buf[76:], MarshalUint64SSZ(s.HeadSlot))
	return buf, nil
}

func (s *Status) UnmarshalSSZ(buf []byte) error {
	copy(s.ForkDigest[:], buf)
	copy(s.FinalizedRoot[:], buf[4:])
	s.FinalizedEpoch = UnmarshalUint64SSZ(buf[36:])
	copy(s.HeadRoot[:], buf[44:])
	s.HeadSlot = UnmarshalUint64SSZ(buf[76:])
	return nil
}

func (s *Status) SizeSSZ() int {
	return 84
}

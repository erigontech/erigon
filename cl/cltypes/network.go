package cltypes

import (
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/ledgerwatch/erigon/common"
)

type Metadata struct {
	SeqNumber uint64
	Attnets   uint64
	Syncnets  *uint64
}

func (m *Metadata) EncodeSSZ(buf []byte) ([]byte, error) {
	if m.Syncnets == nil {
		return ssz2.MarshalSSZ(buf, m.SeqNumber, m.Attnets)
	}
	return ssz2.MarshalSSZ(buf, m.SeqNumber, m.Attnets, *m.Syncnets)
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

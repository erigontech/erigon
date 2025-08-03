// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cltypes

import (
	"encoding/json"
	"strconv"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"

	"github.com/erigontech/erigon/cl/clparams"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

type Metadata struct {
	SeqNumber         uint64
	Attnets           [8]byte
	Syncnets          *[1]byte
	CustodyGroupCount *uint64
}

func (m *Metadata) EncodeSSZ(buf []byte) ([]byte, error) {
	schema := []interface{}{
		m.SeqNumber,
		m.Attnets[:],
	}
	if m.Syncnets != nil {
		schema = append(schema, m.Syncnets[:])
	}
	if m.CustodyGroupCount != nil {
		schema = append(schema, m.CustodyGroupCount)
	}

	return ssz2.MarshalSSZ(buf, schema...)
}

func (m *Metadata) EncodingSizeSSZ() (ret int) {
	ret = 8 * 2
	if m.Syncnets != nil {
		ret += 1
	}
	if m.CustodyGroupCount != nil {
		ret += 8
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

	if len(buf) < 25 {
		// less than fulu
		return nil
	}
	m.CustodyGroupCount = new(uint64)
	*m.CustodyGroupCount = ssz.UnmarshalUint64SSZ(buf[17:25])
	return nil
}

func (m *Metadata) MarshalJSON() ([]byte, error) {
	out := map[string]interface{}{
		"seq_number": strconv.FormatUint(m.SeqNumber, 10),
		"attnets":    hexutil.Bytes(m.Attnets[:]),
	}
	if m.Syncnets != nil {
		out["syncnets"] = hexutil.Bytes(m.Syncnets[:])
	}
	if m.CustodyGroupCount != nil {
		out["custody_group_count"] = *m.CustodyGroupCount
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
	Root common.Hash
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
	ForkDigest            [4]byte
	FinalizedRoot         [32]byte
	FinalizedEpoch        uint64
	HeadRoot              [32]byte
	HeadSlot              uint64
	EarliestAvailableSlot *uint64 // Fulu:EIP7594
}

func (s *Status) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.schema()...)
}

func (s *Status) DecodeSSZ(buf []byte, version int) error {
	schema := []interface{}{
		s.ForkDigest[:],
		s.FinalizedRoot[:],
		&s.FinalizedEpoch,
		s.HeadRoot[:],
		&s.HeadSlot,
	}
	if version >= int(clparams.FuluVersion) {
		if s.EarliestAvailableSlot == nil {
			s.EarliestAvailableSlot = new(uint64)
		}
		schema = append(schema, s.EarliestAvailableSlot)
	}
	return ssz2.UnmarshalSSZ(buf, version, schema...)
}

func (s *Status) schema() []interface{} {
	schema := []interface{}{
		s.ForkDigest[:],
		s.FinalizedRoot[:],
		&s.FinalizedEpoch,
		s.HeadRoot[:],
		&s.HeadSlot,
	}
	if s.EarliestAvailableSlot != nil {
		schema = append(schema, s.EarliestAvailableSlot)
	}
	return schema
}

func (s *Status) EncodingSizeSSZ() int {
	size := 84
	if s.EarliestAvailableSlot != nil {
		size += 8
	}
	return size
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

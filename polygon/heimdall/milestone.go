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

package heimdall

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
)

type MilestoneId uint64

// Milestone defines a response object type of bor milestone
type Milestone struct {
	Id          MilestoneId // numerical one that we assign in heimdall client
	MilestoneId string      // string based in original json response
	Fields      WaypointFields
}

var _ Entity = &Milestone{}
var _ Waypoint = &Milestone{}

func (m *Milestone) RawId() uint64 {
	return uint64(m.Id)
}

func (m *Milestone) SetRawId(_ uint64) {
	panic("unimplemented")
}

func (m *Milestone) StartBlock() *big.Int {
	return m.Fields.StartBlock
}

func (m *Milestone) EndBlock() *big.Int {
	return m.Fields.EndBlock
}

func (m *Milestone) BlockNumRange() ClosedRange {
	return ClosedRange{
		Start: m.StartBlock().Uint64(),
		End:   m.EndBlock().Uint64(),
	}
}

func (m *Milestone) RootHash() libcommon.Hash {
	return m.Fields.RootHash
}

func (m *Milestone) Timestamp() uint64 {
	return m.Fields.Timestamp
}

func (m *Milestone) Length() uint64 {
	return m.Fields.Length()
}

func (m *Milestone) CmpRange(n uint64) int {
	return m.Fields.CmpRange(n)
}

func (m *Milestone) String() string {
	return fmt.Sprintf(
		"Milestone {%v (%d:%d) %v %v %v}",
		m.Fields.Proposer.String(),
		m.Fields.StartBlock,
		m.Fields.EndBlock,
		m.Fields.RootHash.Hex(),
		m.Fields.ChainID,
		m.Fields.Timestamp,
	)
}

func (m *Milestone) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Id          MilestoneId       `json:"id"`
		MilestoneId string            `json:"milestone_id"`
		Proposer    libcommon.Address `json:"proposer"`
		StartBlock  *big.Int          `json:"start_block"`
		EndBlock    *big.Int          `json:"end_block"`
		RootHash    libcommon.Hash    `json:"hash"`
		ChainID     string            `json:"bor_chain_id"`
		Timestamp   uint64            `json:"timestamp"`
	}{
		m.Id,
		m.MilestoneId,
		m.Fields.Proposer,
		m.Fields.StartBlock,
		m.Fields.EndBlock,
		m.Fields.RootHash,
		m.Fields.ChainID,
		m.Fields.Timestamp,
	})
}

func (m *Milestone) UnmarshalJSON(b []byte) error {
	dto := struct {
		WaypointFields
		RootHash    libcommon.Hash `json:"hash"`
		Id          MilestoneId    `json:"id"`
		MilestoneId string         `json:"milestone_id"`
	}{}

	if err := json.Unmarshal(b, &dto); err != nil {
		return err
	}

	m.Id = dto.Id
	m.MilestoneId = dto.MilestoneId
	m.Fields = dto.WaypointFields
	m.Fields.RootHash = dto.RootHash

	return nil
}

type MilestoneResponse struct {
	Height string    `json:"height"`
	Result Milestone `json:"result"`
}

type MilestoneCount struct {
	Count int64 `json:"count"`
}

type MilestoneCountResponse struct {
	Height string         `json:"height"`
	Result MilestoneCount `json:"result"`
}

type MilestoneLastNoAck struct {
	Result string `json:"result"`
}

type MilestoneLastNoAckResponse struct {
	Height string             `json:"height"`
	Result MilestoneLastNoAck `json:"result"`
}

type MilestoneNoAck struct {
	Result bool `json:"result"`
}

type MilestoneNoAckResponse struct {
	Height string         `json:"height"`
	Result MilestoneNoAck `json:"result"`
}

type MilestoneID struct {
	Result bool `json:"result"`
}

type MilestoneIDResponse struct {
	Height string      `json:"height"`
	Result MilestoneID `json:"result"`
}

type Milestones []*Milestone

func (ms Milestones) Waypoints() Waypoints {
	waypoints := make(Waypoints, len(ms))
	for i, m := range ms {
		waypoints[i] = m
	}
	return waypoints
}

var ErrMilestoneNotFound = errors.New("milestone not found")

func MilestoneIdAt(tx kv.Tx, block uint64) (MilestoneId, error) {
	var id uint64

	c, err := tx.Cursor(kv.BorMilestoneEnds)

	if err != nil {
		return 0, err
	}

	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], block)

	k, v, err := c.Seek(blockNumBuf[:])

	if err != nil {
		return 0, err
	}

	if k == nil {
		return 0, fmt.Errorf("%d: %w", block, ErrMilestoneNotFound)
	}

	id = binary.BigEndian.Uint64(v)

	return MilestoneId(id), err
}

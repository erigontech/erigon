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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"github.com/erigontech/erigon-lib/common"
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

func (m *Milestone) RootHash() common.Hash {
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
		Id          MilestoneId    `json:"id"`
		MilestoneId string         `json:"milestone_id"`
		Proposer    common.Address `json:"proposer"`
		StartBlock  *big.Int       `json:"start_block"`
		EndBlock    *big.Int       `json:"end_block"`
		RootHash    common.Hash    `json:"hash"`
		ChainID     string         `json:"bor_chain_id"`
		Timestamp   uint64         `json:"timestamp"`
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
		RootHash    common.Hash `json:"hash"`
		Id          MilestoneId `json:"id"`
		MilestoneId string      `json:"milestone_id"`
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

type MilestoneResponseV1 struct {
	Height string    `json:"height"`
	Result Milestone `json:"result"`
}

type MilestoneResponseV2 struct {
	Milestone struct {
		MilestoneID string         `json:"milestone_id"`
		Proposer    common.Address `json:"proposer"`
		StartBlock  string         `json:"start_block"`
		EndBlock    string         `json:"end_block"`
		Hash        string         `json:"hash"`
		ChainID     string         `json:"bor_chain_id"`
		Timestamp   string         `json:"timestamp"`
	} `json:"milestone"`
}

func (v *MilestoneResponseV2) ToMilestone(id int64) (*Milestone, error) {
	r := Milestone{
		Id:          MilestoneId(id),
		MilestoneId: v.Milestone.MilestoneID,
		Fields: WaypointFields{
			Proposer: v.Milestone.Proposer,
			ChainID:  v.Milestone.ChainID,
		},
	}

	decoded, err := base64.StdEncoding.DecodeString(v.Milestone.Hash)
	if err != nil {
		return nil, err
	}

	startBlock, err := strconv.Atoi(v.Milestone.StartBlock)
	if err != nil {
		return nil, err
	}

	endBlock, err := strconv.Atoi(v.Milestone.EndBlock)
	if err != nil {
		return nil, err
	}

	r.Fields.RootHash = common.BytesToHash(decoded)
	r.Fields.StartBlock = big.NewInt(int64(startBlock))
	r.Fields.EndBlock = big.NewInt(int64(endBlock))

	timestamp, err := strconv.Atoi(v.Milestone.Timestamp)
	if err != nil {
		return nil, err
	}

	r.Fields.Timestamp = uint64(timestamp)

	return &r, nil
}

type MilestoneCount struct {
	Count int64 `json:"count"`
}

type MilestoneCountResponseV1 struct {
	Height string         `json:"height"`
	Result MilestoneCount `json:"result"`
}

type MilestoneCountResponseV2 struct {
	Count string `json:"count"`
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

var ErrMilestoneNotFound = errors.New("milestone not found")

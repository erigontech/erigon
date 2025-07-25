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

type CheckpointId uint64

// Checkpoint defines a response object type of bor checkpoint
type Checkpoint struct {
	Id     CheckpointId
	Fields WaypointFields
}

var _ Entity = &Checkpoint{}
var _ Waypoint = &Checkpoint{}

func (c *Checkpoint) RawId() uint64 {
	return uint64(c.Id)
}

func (c *Checkpoint) SetRawId(id uint64) {
	c.Id = CheckpointId(id)
}

func (c *Checkpoint) StartBlock() *big.Int {
	return c.Fields.StartBlock
}

func (c *Checkpoint) EndBlock() *big.Int {
	return c.Fields.EndBlock
}

func (c *Checkpoint) BlockNumRange() ClosedRange {
	return ClosedRange{
		Start: c.StartBlock().Uint64(),
		End:   c.EndBlock().Uint64(),
	}
}

func (c *Checkpoint) RootHash() common.Hash {
	return c.Fields.RootHash
}

func (c *Checkpoint) Timestamp() uint64 {
	return c.Fields.Timestamp
}

func (c *Checkpoint) Length() uint64 {
	return c.Fields.Length()
}

func (c *Checkpoint) CmpRange(n uint64) int {
	return c.Fields.CmpRange(n)
}

func (c *Checkpoint) String() string {
	return fmt.Sprintf(
		"Checkpoint {%v (%d:%d) %v %v %v}",
		c.Fields.Proposer.String(),
		c.Fields.StartBlock,
		c.Fields.EndBlock,
		c.Fields.RootHash.Hex(),
		c.Fields.ChainID,
		c.Fields.Timestamp,
	)
}

func (c *Checkpoint) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Id         CheckpointId   `json:"id"`
		Proposer   common.Address `json:"proposer"`
		StartBlock *big.Int       `json:"start_block"`
		EndBlock   *big.Int       `json:"end_block"`
		RootHash   common.Hash    `json:"root_hash"`
		ChainID    string         `json:"bor_chain_id"`
		Timestamp  uint64         `json:"timestamp"`
	}{
		c.Id,
		c.Fields.Proposer,
		c.Fields.StartBlock,
		c.Fields.EndBlock,
		c.Fields.RootHash,
		c.Fields.ChainID,
		c.Fields.Timestamp,
	})
}

func (c *Checkpoint) UnmarshalJSON(b []byte) error {
	dto := struct {
		WaypointFields
		RootHash common.Hash  `json:"root_hash"`
		Id       CheckpointId `json:"id"`
	}{}

	if err := json.Unmarshal(b, &dto); err != nil {
		return err
	}

	c.Id = dto.Id
	c.Fields = dto.WaypointFields
	c.Fields.RootHash = dto.RootHash

	return nil
}

type checkpoints []*Checkpoint

func (cs checkpoints) Len() int {
	return len(cs)
}

func (cs checkpoints) Less(i, j int) bool {
	return cs[i].StartBlock().Uint64() < cs[j].StartBlock().Uint64()
}

func (cs checkpoints) Swap(i, j int) {
	cs[i], cs[j] = cs[j], cs[i]
}

type CheckpointResponseV1 struct {
	Height string     `json:"height"`
	Result Checkpoint `json:"result"`
}

type CheckpointResponseV2 struct {
	Checkpoint struct {
		Proposer   common.Address `json:"proposer"`
		StartBlock string         `json:"start_block"`
		EndBlock   string         `json:"end_block"`
		RootHash   string         `json:"root_hash"`
		ChainID    string         `json:"bor_chain_id"`
		Timestamp  string         `json:"timestamp"`
	} `json:"checkpoint"`
}

func (v *CheckpointResponseV2) ToCheckpoint(id int64) (*Checkpoint, error) {
	r := Checkpoint{
		Id: CheckpointId(id),
		Fields: WaypointFields{
			Proposer: v.Checkpoint.Proposer,
			ChainID:  v.Checkpoint.ChainID,
		},
	}

	decoded, err := base64.StdEncoding.DecodeString(v.Checkpoint.RootHash)
	if err != nil {
		return nil, err
	}

	startBlock, err := strconv.Atoi(v.Checkpoint.StartBlock)
	if err != nil {
		return nil, err
	}

	endBlock, err := strconv.Atoi(v.Checkpoint.EndBlock)
	if err != nil {
		return nil, err
	}

	r.Fields.RootHash = common.BytesToHash(decoded)
	r.Fields.StartBlock = big.NewInt(int64(startBlock))
	r.Fields.EndBlock = big.NewInt(int64(endBlock))

	timestamp, err := strconv.Atoi(v.Checkpoint.Timestamp)
	if err != nil {
		return nil, err
	}

	r.Fields.Timestamp = uint64(timestamp)

	return &r, nil
}

type CheckpointCount struct {
	Result int64 `json:"result"`
}

type CheckpointCountResponseV1 struct {
	Height string          `json:"height"`
	Result CheckpointCount `json:"result"`
}

type CheckpointCountResponseV2 struct {
	AckCount string `json:"ack_count"`
}

type CheckpointListResponseV1 struct {
	Height string      `json:"height"`
	Result checkpoints `json:"result"`
}

type CheckpointListResponseV2 struct {
	CheckpointList []struct {
		ID         string         `json:"id"`
		Proposer   common.Address `json:"proposer"`
		StartBlock string         `json:"start_block"`
		EndBlock   string         `json:"end_block"`
		RootHash   string         `json:"root_hash"`
		ChainID    string         `json:"bor_chain_id"`
		Timestamp  string         `json:"timestamp"`
	} `json:"checkpoint_list"`
}

func (v *CheckpointListResponseV2) ToList() ([]*Checkpoint, error) {
	checkpoints := make([]*Checkpoint, 0, len(v.CheckpointList))

	for i := range v.CheckpointList {
		r := Checkpoint{
			Fields: WaypointFields{
				Proposer: v.CheckpointList[i].Proposer,
				ChainID:  v.CheckpointList[i].ChainID,
			},
		}

		id, err := strconv.Atoi(v.CheckpointList[i].ID)
		if err != nil {
			return nil, err
		}

		decoded, err := base64.StdEncoding.DecodeString(v.CheckpointList[i].RootHash)
		if err != nil {
			return nil, err
		}

		startBlock, err := strconv.Atoi(v.CheckpointList[i].StartBlock)
		if err != nil {
			return nil, err
		}

		endBlock, err := strconv.Atoi(v.CheckpointList[i].EndBlock)
		if err != nil {
			return nil, err
		}

		r.Id = CheckpointId(id)
		r.Fields.RootHash = common.BytesToHash(decoded)
		r.Fields.RootHash = common.BytesToHash(decoded)
		r.Fields.StartBlock = big.NewInt(int64(startBlock))
		r.Fields.EndBlock = big.NewInt(int64(endBlock))

		timestamp, err := strconv.Atoi(v.CheckpointList[i].Timestamp)
		if err != nil {
			return nil, err
		}

		r.Fields.Timestamp = uint64(timestamp)

		checkpoints = append(checkpoints, &r)
	}

	return checkpoints, nil
}

var ErrCheckpointNotFound = errors.New("checkpoint not found")

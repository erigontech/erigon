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

func (c *Checkpoint) RootHash() libcommon.Hash {
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
		Id         CheckpointId      `json:"id"`
		Proposer   libcommon.Address `json:"proposer"`
		StartBlock *big.Int          `json:"start_block"`
		EndBlock   *big.Int          `json:"end_block"`
		RootHash   libcommon.Hash    `json:"root_hash"`
		ChainID    string            `json:"bor_chain_id"`
		Timestamp  uint64            `json:"timestamp"`
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
		RootHash libcommon.Hash `json:"root_hash"`
		Id       CheckpointId   `json:"id"`
	}{}

	if err := json.Unmarshal(b, &dto); err != nil {
		return err
	}

	c.Id = dto.Id
	c.Fields = dto.WaypointFields
	c.Fields.RootHash = dto.RootHash

	return nil
}

type Checkpoints []*Checkpoint

func (cs Checkpoints) Len() int {
	return len(cs)
}

func (cs Checkpoints) Less(i, j int) bool {
	return cs[i].StartBlock().Uint64() < cs[j].StartBlock().Uint64()
}

func (cs Checkpoints) Swap(i, j int) {
	cs[i], cs[j] = cs[j], cs[i]
}

func (cs Checkpoints) Waypoints() Waypoints {
	waypoints := make(Waypoints, len(cs))
	for i, c := range cs {
		waypoints[i] = c
	}
	return waypoints
}

type CheckpointResponse struct {
	Height string     `json:"height"`
	Result Checkpoint `json:"result"`
}

type CheckpointCount struct {
	Result int64 `json:"result"`
}

type CheckpointCountResponse struct {
	Height string          `json:"height"`
	Result CheckpointCount `json:"result"`
}

type CheckpointListResponse struct {
	Height string      `json:"height"`
	Result Checkpoints `json:"result"`
}

var ErrCheckpointNotFound = errors.New("checkpoint not found")

func CheckpointIdAt(tx kv.Tx, block uint64) (CheckpointId, error) {
	var id uint64

	c, err := tx.Cursor(kv.BorCheckpointEnds)

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
		return 0, fmt.Errorf("%d: %w", block, ErrCheckpointNotFound)
	}

	id = binary.BigEndian.Uint64(v)

	return CheckpointId(id), err
}

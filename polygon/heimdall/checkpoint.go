package heimdall

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

var _ Waypoint = Checkpoint{}

type CheckpointId uint64

// Checkpoint defines a response object type of bor checkpoint
type Checkpoint struct {
	Id     CheckpointId
	Fields WaypointFields
}

func (c Checkpoint) StartBlock() *big.Int {
	return c.Fields.StartBlock
}

func (c Checkpoint) EndBlock() *big.Int {
	return c.Fields.EndBlock
}

func (c Checkpoint) RootHash() libcommon.Hash {
	return c.Fields.RootHash
}

func (c Checkpoint) Timestamp() uint64 {
	return c.Fields.Timestamp
}

func (c Checkpoint) Length() uint64 {
	return c.Fields.Length()
}

func (c Checkpoint) CmpRange(n uint64) int {
	return c.Fields.CmpRange(n)
}

func (m Checkpoint) String() string {
	return fmt.Sprintf(
		"Checkpoint {%v (%d:%d) %v %v %v}",
		m.Fields.Proposer.String(),
		m.Fields.StartBlock,
		m.Fields.EndBlock,
		m.Fields.RootHash.Hex(),
		m.Fields.ChainID,
		m.Fields.Timestamp,
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

var ErrCheckpointNotFound = fmt.Errorf("checkpoint not found")

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

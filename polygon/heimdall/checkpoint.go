package heimdall

import (
	"encoding/json"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

var _ Waypoint = Checkpoint{}

type CheckpointId uint64

// Checkpoint defines a response object type of bor checkpoint
type Checkpoint struct {
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

func (c Checkpoint) Length() int {
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

func (c Checkpoint) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Fields)
}

func (c *Checkpoint) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &c.Fields)
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

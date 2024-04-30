package heimdall

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

var _ Waypoint = Milestone{}

type MilestoneId uint64

// milestone defines a response object type of bor milestone
type Milestone struct {
	Id     MilestoneId
	Fields WaypointFields
}

func (m Milestone) StartBlock() *big.Int {
	return m.Fields.StartBlock
}

func (m Milestone) EndBlock() *big.Int {
	return m.Fields.EndBlock
}

func (m Milestone) RootHash() libcommon.Hash {
	return m.Fields.RootHash
}

func (m Milestone) Timestamp() uint64 {
	return m.Fields.Timestamp
}

func (m Milestone) Length() uint64 {
	return m.Fields.Length()
}

func (m Milestone) CmpRange(n uint64) int {
	return m.Fields.CmpRange(n)
}

func (m Milestone) String() string {
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
		Id         MilestoneId       `json:"milestone_id"`
		Proposer   libcommon.Address `json:"proposer"`
		StartBlock *big.Int          `json:"start_block"`
		EndBlock   *big.Int          `json:"end_block"`
		RootHash   libcommon.Hash    `json:"hash"`
		ChainID    string            `json:"bor_chain_id"`
		Timestamp  uint64            `json:"timestamp"`
	}{
		m.Id,
		m.Fields.Proposer,
		m.Fields.StartBlock,
		m.Fields.EndBlock,
		m.Fields.RootHash,
		m.Fields.ChainID,
		m.Fields.Timestamp,
	})
}

func (m *Milestone) UnmarshalJSON(b []byte) error {

	// TODO - do we want to handle milestone_id ?
	// (example format: 043353d6-d83f-47f8-a38f-f5062e82a6d4 - 0x142987cad41cf7111b2f186da6ab89e460037f7f)
	dto := struct {
		WaypointFields
		RootHash libcommon.Hash `json:"hash"`
		Id       MilestoneId    `json:"id"`
	}{}

	if err := json.Unmarshal(b, &dto); err != nil {
		return err
	}

	m.Id = dto.Id
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

var ErrMilestoneNotFound = fmt.Errorf("milestone not found")

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

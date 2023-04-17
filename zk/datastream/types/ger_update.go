package types

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
)

const (
	gerUpdateDataLength = 102

	// EntryTypeL2Block represents a L2 block
	EntryTypeGerUpdate EntryType = 4
)

// StartL2Block represents a zkEvm block
type GerUpdate struct {
	BatchNumber    uint64         // 8 bytes
	Timestamp      uint64         // 8 bytes
	GlobalExitRoot common.Hash    // 32 bytes
	Coinbase       common.Address // 20 bytes
	ForkId         uint16         // 2 bytes
	StateRoot      common.Hash    // 32 bytes
}

func (g *GerUpdate) EncodeToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, g.BatchNumber)
	binary.Write(buf, binary.LittleEndian, g.Timestamp)
	buf.Write(g.GlobalExitRoot.Bytes())
	buf.Write(g.Coinbase.Bytes())
	binary.Write(buf, binary.LittleEndian, g.ForkId)
	buf.Write(g.StateRoot.Bytes())

	return buf.Bytes()
}

// decodes a StartL2Block from a byte array
func DecodeGerUpdate(data []byte) (*GerUpdate, error) {
	if len(data) != gerUpdateDataLength {
		return &GerUpdate{}, fmt.Errorf("expected data length: %d, got: %d", gerUpdateDataLength, len(data))
	}

	var ts uint64
	buf := bytes.NewBuffer(data[8:16])
	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return &GerUpdate{}, err
	}

	return &GerUpdate{
		BatchNumber:    binary.LittleEndian.Uint64(data[:8]),
		Timestamp:      ts,
		GlobalExitRoot: common.BytesToHash(data[16:48]),
		Coinbase:       common.BytesToAddress(data[48:68]),
		ForkId:         binary.LittleEndian.Uint16(data[68:70]),
		StateRoot:      common.BytesToHash(data[70:102]),
	}, nil
}

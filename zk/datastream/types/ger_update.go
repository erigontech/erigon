package types

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"google.golang.org/protobuf/proto"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
)

const (
	gerUpdateDataLength         = 106
	gerUpdateDataLengthPreEtrog = 102
)

type GerUpdateProto struct {
	*datastream.UpdateGER
}

func (g *GerUpdateProto) Marshal() ([]byte, error) {
	return proto.Marshal(g.UpdateGER)
}

func (g *GerUpdateProto) Type() EntryType {
	return EntryTypeGerUpdate
}

func ConvertGerUpdateToProto(g GerUpdate) GerUpdateProto {
	return GerUpdateProto{
		UpdateGER: &datastream.UpdateGER{
			BatchNumber:    g.BatchNumber,
			Timestamp:      g.Timestamp,
			GlobalExitRoot: g.GlobalExitRoot.Bytes(),
			Coinbase:       g.Coinbase.Bytes(),
			ForkId:         uint64(g.ForkId),
			ChainId:        uint64(g.ChainId),
			StateRoot:      g.StateRoot.Bytes(),
			Debug:          nil,
		},
	}
}

type GerUpdate struct {
	BatchNumber    uint64         // 8 bytes
	Timestamp      uint64         // 8 bytes
	GlobalExitRoot common.Hash    // 32 bytes
	Coinbase       common.Address // 20 bytes
	ForkId         uint16         // 2 bytes
	ChainId        uint32         // 4 bytes
	StateRoot      common.Hash    // 32 bytes
	Debug          Debug          // proto only
}

func (g *GerUpdate) EntryType() EntryType {
	return EntryTypeGerUpdate
}

func (g *GerUpdate) Bytes(bigEndian bool) []byte {
	if bigEndian {
		return g.EncodeToBytesBigEndian()
	}
	return g.EncodeToBytes()
}

func (g *GerUpdate) EncodeToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, g.BatchNumber)
	binary.Write(buf, binary.LittleEndian, g.Timestamp)
	buf.Write(g.GlobalExitRoot.Bytes())
	buf.Write(g.Coinbase.Bytes())
	binary.Write(buf, binary.LittleEndian, g.ForkId)
	binary.Write(buf, binary.LittleEndian, g.ChainId)
	buf.Write(g.StateRoot.Bytes())

	return buf.Bytes()
}

func (g *GerUpdate) EncodeToBytesBigEndian() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, g.BatchNumber)
	binary.Write(buf, binary.BigEndian, g.Timestamp)
	buf.Write(g.GlobalExitRoot.Bytes())
	buf.Write(g.Coinbase.Bytes())
	binary.Write(buf, binary.BigEndian, g.ForkId)
	binary.Write(buf, binary.BigEndian, g.ChainId)
	buf.Write(g.StateRoot.Bytes())

	return buf.Bytes()
}

func DecodeGerUpdate(data []byte) (*GerUpdate, error) {
	if len(data) != gerUpdateDataLength {
		if len(data) == gerUpdateDataLengthPreEtrog {
			return decodeGerUpdatePreEtrog(data)
		}
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
		ChainId:        binary.LittleEndian.Uint32(data[70:74]),
		StateRoot:      common.BytesToHash(data[74:106]),
	}, nil
}

func decodeGerUpdatePreEtrog(data []byte) (*GerUpdate, error) {
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

func DecodeGerUpdateBigEndian(data []byte) (*GerUpdate, error) {
	if len(data) != gerUpdateDataLength {
		return &GerUpdate{}, fmt.Errorf("expected data length: %d, got: %d", gerUpdateDataLength, len(data))
	}

	var ts uint64
	buf := bytes.NewBuffer(data[8:16])
	if err := binary.Read(buf, binary.BigEndian, &ts); err != nil {
		return &GerUpdate{}, err
	}

	return &GerUpdate{
		BatchNumber:    binary.BigEndian.Uint64(data[:8]),
		Timestamp:      ts,
		GlobalExitRoot: common.BytesToHash(data[16:48]),
		Coinbase:       common.BytesToAddress(data[48:68]),
		ForkId:         binary.BigEndian.Uint16(data[68:70]),
		ChainId:        binary.LittleEndian.Uint32(data[70:74]),
		StateRoot:      common.BytesToHash(data[74:106]),
	}, nil
}

func DecodeGerUpdateProto(data []byte) (*GerUpdate, error) {
	ug := datastream.UpdateGER{}
	err := proto.Unmarshal(data, &ug)
	if err != nil {
		return nil, err
	}

	return &GerUpdate{
		BatchNumber:    ug.BatchNumber,
		Timestamp:      ug.Timestamp,
		GlobalExitRoot: common.BytesToHash(ug.GlobalExitRoot),
		Coinbase:       common.BytesToAddress(ug.Coinbase),
		ForkId:         uint16(ug.ForkId),
		ChainId:        uint32(ug.ChainId),
		StateRoot:      common.BytesToHash(ug.StateRoot),
		Debug:          ProcessDebug(ug.Debug),
	}, nil
}

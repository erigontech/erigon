package bridge

import "encoding/binary"

type ProcessedBlockInfo struct {
	BlockNum  uint64
	BlockTime uint64
}

func (info *ProcessedBlockInfo) MarshallBytes() (key []byte, value []byte) {
	key = make([]byte, 8)
	binary.BigEndian.PutUint64(key, info.BlockNum)
	value = make([]byte, 8)
	binary.BigEndian.PutUint64(value, info.BlockTime)
	return
}

func (info *ProcessedBlockInfo) UnmarshallBytes(key []byte, value []byte) {
	info.BlockNum = binary.BigEndian.Uint64(key)
	info.BlockTime = binary.BigEndian.Uint64(value)
}

package silkworm

import "unsafe"

type MemoryMappedRegion struct {
	FilePath   string
	DataHandle unsafe.Pointer
	Size       int64
}

type MappedHeaderSnapshot struct {
	Segment       *MemoryMappedRegion
	IdxHeaderHash *MemoryMappedRegion
}

type MappedBodySnapshot struct {
	Segment       *MemoryMappedRegion
	IdxBodyNumber *MemoryMappedRegion
}

type MappedTxnSnapshot struct {
	Segment             *MemoryMappedRegion
	IdxTxnHash          *MemoryMappedRegion
	IdxTxnHash2BlockNum *MemoryMappedRegion
}

type MappedChainSnapshot struct {
	Headers *MappedHeaderSnapshot
	Bodies  *MappedBodySnapshot
	Txs     *MappedTxnSnapshot
}

func NewMemoryMappedRegion(filePath string, dataHandle unsafe.Pointer, size int64) *MemoryMappedRegion {
	region := &MemoryMappedRegion{
		FilePath:   filePath,
		DataHandle: dataHandle,
		Size:       size,
	}
	return region
}

func NewMappedHeaderSnapshot(segment, idxHeaderHash *MemoryMappedRegion) *MappedHeaderSnapshot {
	snapshot := &MappedHeaderSnapshot{
		Segment:       segment,
		IdxHeaderHash: idxHeaderHash,
	}
	return snapshot
}

func NewMappedBodySnapshot(segment, idxBodyNumber *MemoryMappedRegion) *MappedBodySnapshot {
	snapshot := &MappedBodySnapshot{
		Segment:       segment,
		IdxBodyNumber: idxBodyNumber,
	}
	return snapshot
}

func NewMappedTxnSnapshot(segment, idxTxnHash, idxTxnHash2BlockNum *MemoryMappedRegion) *MappedTxnSnapshot {
	snapshot := &MappedTxnSnapshot{
		Segment:             segment,
		IdxTxnHash:          idxTxnHash,
		IdxTxnHash2BlockNum: idxTxnHash2BlockNum,
	}
	return snapshot
}

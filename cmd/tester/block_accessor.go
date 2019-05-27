package main

import (
	"fmt"
	"io"
	"math/big"
	"os"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type BlockAccessor struct {
	input               *os.File
	blockOffsetByHash   map[common.Hash]uint64
	blockOffsetByNumber map[uint64]uint64
	headersByHash       map[common.Hash]*types.Header
	headersByNumber     map[uint64]*types.Header
	lastBlock           *types.Block
	totalDifficulty     *big.Int
}

func (ba *BlockAccessor) Close() {
	ba.input.Close()
}

func (ba *BlockAccessor) GetHeaderByHash(hash common.Hash) *types.Header {
	return ba.headersByHash[hash]
}

func (ba *BlockAccessor) GetHeaderByNumber(number uint64) *types.Header {
	return ba.headersByNumber[number]
}

func (ba *BlockAccessor) readBlockFromOffset(offset uint64) (*types.Block, error) {
	ba.input.Seek(int64(offset), 0)
	stream := rlp.NewStream(ba.input, 0)
	var b types.Block
	if err := stream.Decode(&b); err != nil {
		return nil, err
	}
	return &b, nil
}

func (ba *BlockAccessor) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	if blockOffset, ok := ba.blockOffsetByHash[hash]; ok {
		return ba.readBlockFromOffset(blockOffset)
	}
	return nil, nil
}

func (ba *BlockAccessor) TotalDifficulty() *big.Int {
	return ba.totalDifficulty
}

func (ba *BlockAccessor) LastBlock() *types.Block {
	return ba.lastBlock
}

// Reads and indexes the file created by geth exportdb
func NewBlockAccessor(inputFile string) (*BlockAccessor, error) {
	input, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}
	ba := &BlockAccessor{
		input:               input,
		blockOffsetByHash:   make(map[common.Hash]uint64),
		blockOffsetByNumber: make(map[uint64]uint64),
		headersByHash:       make(map[common.Hash]*types.Header),
		headersByNumber:     make(map[uint64]*types.Header),
	}
	var reader io.Reader = input
	stream := rlp.NewStream(reader, 0)
	var b types.Block
	n := 0
	td := new(big.Int)
	var pos uint64
	for {
		blockOffset := pos
		var size uint64
		_, size, err = stream.Kind()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return nil, fmt.Errorf("at block %d: %v", n, err)
		}
		pos += 1 + size
		if size >= 56 {
			bytecount := 0
			for size > 0 {
				size >>= 8
				bytecount++
			}
			pos += uint64(bytecount)
		}
		if err = stream.Decode(&b); err == io.EOF {
			err = nil // Clear it
			break
		} else if err != nil {
			return nil, fmt.Errorf("at block %d: %v", n, err)
		}
		// don't import first block
		if b.NumberU64() == 0 {
			continue
		}
		h := b.Header()
		hash := h.Hash()
		ba.headersByHash[hash] = h
		ba.headersByNumber[b.NumberU64()] = h
		ba.blockOffsetByHash[hash] = blockOffset
		ba.blockOffsetByNumber[b.NumberU64()] = blockOffset
		td = new(big.Int).Add(td, b.Difficulty())
		n++
	}
	fmt.Printf("%d blocks read, bytes read %d\n", n, pos)
	ba.lastBlock = &b
	ba.totalDifficulty = td
	return ba, nil
}

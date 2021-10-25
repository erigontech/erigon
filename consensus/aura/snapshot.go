package aura

import (
	"bytes"
	"context"
	"errors"
	"math/big"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

// Snapshot is the state of the authorization voting at a given point in time.

const BLOCKS_FOR_CRITICAL_SNAPSHOT uint64 = 5000
const CHUNK_SIZE int64 = 4194304 // highest possible encoded chunk size

type Snapshot struct {
	config *params.AuRaConfig // Consensus engine parameters to fine tune behavior

	Number             uint64                      `json:"number"`  // Block number where the snapshot was created
	Hash               common.Hash                 `json:"hash"`    // Block hash where the snapshot was created
	Signers            map[common.Address]struct{} `json:"signers"` // Set of authorized signers at this moment
	encodedManifest    []byte
	encodedStateChunks []byte
}

type Manifest struct {
	version      uint64 //  snapshot format version. Must be set to 2
	root         common.Hash
	stateHashes  StateChunks // a list of all the state chunks in this snapshot
	blockChunks  BlockChunks // a list of all of the block chunks in this snapshot it is 32 bit longs
	block_number uint64      // the number of the best block in the snapshot; the one which the state coordinates to
	block_hash   common.Hash // the hash of the bst block in the snapshot
}

type StateChunks struct {
	accountEntries []StateChunk
}

type StateChunk struct {
	address     common.Address
	richAccount RichAccount
}

type BlockChunks struct {
	blockChunk []BlockChunk
}
type BlockChunk struct {
	firstBlockNumber *big.Int    // number of the first block in the chunk
	firstBlockHash   common.Hash // hash of the first block in the chunk
	totalDifficulty  *big.Int    // total difficulty of the first block in the chunk
	abAndRC          []Chunk     // The abridged RLP of all the blocks in the chunk and their receipts
}

type Chunk struct {
	ab AbridgedBlock
	rc common.Hash
}

type RichAccount struct {
	nonce   uint64
	balance uint64
	code    common.Hash
	storage [][]byte
}

type AbridgedBlock struct {
	author     common.Address
	root       common.Hash
	bloom      types.Bloom
	difficulty *big.Int
	gasLimit   uint64
	gasUsed    uint64
	timestamp  int64

	//seal fields
	mixHash common.Hash
	nonce   types.BlockNonce
}

// signersAscending implements the sort interface to allow sorting a list of addresses
type SignersAscending []common.Address

func (s SignersAscending) Len() int           { return len(s) }
func (s SignersAscending) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }
func (s SignersAscending) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func generateManifest(stateHashes StateChunks, blockHashes BlockChunks, header types.Header, bestBlock types.Block) (*Manifest, error) {
	manifest := &Manifest{
		version:      2,
		root:         header.Root,
		stateHashes:  stateHashes,
		blockChunks:  blockHashes,
		block_number: bestBlock.Number().Uint64(),
		block_hash:   bestBlock.Hash(),
	}

	rlpManifest, err := rlp.EncodeToBytes(manifest)

	if err != nil {
		return manifest, err
	}

	if int64(len(rlpManifest)) > CHUNK_SIZE {
		return manifest, errors.New("Chunk size of the encoded Manifest is too big")
	}

	return manifest, nil
}

func generateAbridgedBlock(header types.Header) *AbridgedBlock {
	ab := &AbridgedBlock{
		author:     header.Coinbase,
		root:       header.Root,
		bloom:      header.Bloom,
		difficulty: header.Difficulty,
		gasLimit:   header.GasLimit,
		gasUsed:    header.GasUsed,
		mixHash:    header.MixDigest,
		nonce:      header.Nonce,
	}

	return ab
}

// generates the block chunks structure for the snapshot P.S. it is not encoded
// some code in here could be refactored for better understanding like the generation of the chunk list from current block to target
func generateBlockChunks(ctx context.Context, chain ethereum.ChainReader, mostRecentBlock types.Header) (BlockChunks, error) {

	targetBlockNumber := mostRecentBlock.Number
	endingBlockNumber := mostRecentBlock.Number.Add(big.NewInt(-30000), big.NewInt(1))

	var blockChunks []BlockChunk
	currentChunkSize := int64(0)

	for i := int64(0); i > endingBlockNumber.Int64(); i-- {
		currentBlock, err := chain.BlockByNumber(ctx, big.NewInt(i))

		if err != nil {
			return BlockChunks{}, err // returns an empty struct of blockchunks if an error is found
		}

		// this part may not be correct since abridgedBlock is missing the list of transactions and the list of uncles
		abridgedBlock := generateAbridgedBlock(*currentBlock.Header())
		receipt := currentBlock.Header().ReceiptHash

		encodedChunk, error := rlp.EncodeToBytes(&Chunk{ab: *abridgedBlock, rc: receipt})

		if error != nil {
			return BlockChunks{}, nil
		}

		size := int64(len(encodedChunk))

		currentChunkSize += size

		// if currentChunkSize is bigger than the recommmended chunk size you have to walk forwards
		// from current block number until target block number and create a chunk list to add to the struct BlockChunk
		if currentChunkSize > CHUNK_SIZE {
			currentChunkSize -= CHUNK_SIZE

			// a structure to hold the rlp of the abridged blocks and receipts
			var chunks []Chunk

			for j := int64(i); j < targetBlockNumber.Int64(); j++ {
				nextBlock, err := chain.BlockByNumber(ctx, big.NewInt(j))

				if err != nil {
					return BlockChunks{}, err
				}

				nextAB := generateAbridgedBlock(*nextBlock.Header())
				receiptOfNextBlock := nextBlock.Header().ReceiptHash

				chunks = append(chunks, Chunk{*nextAB, receiptOfNextBlock})

			}
			// getting the block after the current block since that will be the first block in the chunk
			nextBlock, err := chain.BlockByNumber(ctx, big.NewInt(i+1))

			if err != nil {
				return BlockChunks{}, err
			}

			// create a blockChunk with the current chunks from current block to block target
			blockChunk := &BlockChunk{
				firstBlockNumber: nextBlock.Number(),
				firstBlockHash:   nextBlock.Hash(),
				totalDifficulty:  nextBlock.DeprecatedTd(),
				abAndRC:          chunks,
			}

			blockChunks = append(blockChunks, *blockChunk)

			// set the new target block number to the current block
			targetBlockNumber = big.NewInt(i)
		}

	}

	// if current chunk size is still greater than zero we are going to write a chunk from the ending block number to the target block number
	if currentChunkSize > 0 {

		var chunks []Chunk

		for i := endingBlockNumber.Int64(); i < targetBlockNumber.Int64(); i++ {
			nextBlock, err := chain.BlockByNumber(ctx, big.NewInt(i))

			if err != nil {
				return BlockChunks{}, err
			}

			nextAB := generateAbridgedBlock(*nextBlock.Header())
			receiptOfNextBlock := nextBlock.Header().ReceiptHash

			chunks = append(chunks, Chunk{*nextAB, receiptOfNextBlock})
		}

		nextBlock, err := chain.BlockByNumber(ctx, endingBlockNumber)

		if err != nil {
			return BlockChunks{}, err
		}

		// create a blockChunk with the current chunks from current block to block target
		blockChunk := &BlockChunk{
			firstBlockNumber: nextBlock.Number(),
			firstBlockHash:   nextBlock.Hash(),
			totalDifficulty:  nextBlock.DeprecatedTd(),
			abAndRC:          chunks,
		}

		blockChunks = append(blockChunks, *blockChunk)
	}

	return BlockChunks{blockChunk: blockChunks}, nil
}

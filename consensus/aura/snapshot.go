package aura

import (
	"bytes"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/params"
)

// Snapshot is the state of the authorization voting at a given point in time.
type Snapshot struct {
	config *params.CliqueConfig // Consensus engine parameters to fine tune behavior

	Number  uint64                      `json:"number"`  // Block number where the snapshot was created
	Hash    common.Hash                 `json:"hash"`    // Block hash where the snapshot was created
	Signers map[common.Address]struct{} `json:"signers"` // Set of authorized signers at this moment
	Recents map[uint64]common.Address   `json:"recents"` // Set of recent signers for spam protections
	encodedManifest []byte
	encodedStateChunks []byte
}

type Manifest struct{
	version uint64 //  snapshot format version. Must be set to 2
	stateHashes StateChunks // a list of all the state chunks in this snapshot
	blockChunks BlockChunk // a list of all of the block chunks in this snapshot it is 32 bit longs
	block_number uint64 // the number of the best block in the snapshot; the one which the state coordinates to
	block_hash common.Hash // the hash of the bst block in the snapshot
}

type StateChunks struct{
	accountEntries []StateChunk 
}

type StateChunk struct{
	address common.Address
	richAccount RichAccount
}


type BlockChunk struct{
	firstBlockNumber uint64 // number of the first block in the chunk
	firstBlockHash common.Hash // hash of the first block in the chunk
	totalDifficulty uint64 // total difficulty of the first block in the chunk
	abAndRC [][]byte // The abridged RLP of all the blocks in the chunk and their receipts
}

type RichAccount struct{
	nonce uint64
	balance uint64
	code common.Hash
	storage [][]byte
}

type AbridgedBlock struct{
	author common.Address
	state_root []byte
	log_bloom []byte
	difficulty uint64
	gas_limit []byte
	gas_used []byte
	timestamp int64 

	//seal fields
	mixHash common.Hash
	nonce uint64
}


// signersAscending implements the sort interface to allow sorting a list of addresses
type SignersAscending []common.Address

func (s SignersAscending) Len() int           { return len(s) }
func (s SignersAscending) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }
func (s SignersAscending) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }


func blockChunkGeneration() *BlockChunk{

}

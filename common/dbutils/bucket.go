package dbutils

import "github.com/ledgerwatch/turbo-geth/metrics"

// The fields below define the low level database schema prefixing.
var (
	AccountsBucket        = []byte("AT")
	AccountsHistoryBucket = []byte("hAT")
	StorageBucket         = []byte("ST")
	StorageHistoryBucket  = []byte("hST")
	CodeBucket            = []byte("CODE")

	ChangeSetBucket = []byte("ChangeSet")

	// databaseVerisionKey tracks the current database version.
	DatabaseVerisionKey = []byte("DatabaseVersion")

	// headHeaderKey tracks the latest know header's hash.
	HeadHeaderKey = []byte("LastHeader")

	// headBlockKey tracks the latest know full block's hash.
	HeadBlockKey = []byte("LastBlock")

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	HeadFastBlockKey = []byte("LastFast")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	FastTrieProgressKey = []byte("TrieSync")

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	HeaderPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	HeaderTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	HeaderHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	HeaderNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)

	BlockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	BlockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	TxLookupPrefix  = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata
	BloomBitsPrefix = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits

	PreimagePrefix = []byte("secure-key-")      // preimagePrefix + hash -> preimage
	ConfigPrefix   = []byte("ethereum-config-") // config prefix for the db

	// Chain index prefixes (use `i` + single byte to avoid mixing data types).
	BloomBitsIndexPrefix = []byte("iB") // BloomBitsIndexPrefix is the data table of a chain indexer to track its progress

	PreimageCounter    = metrics.NewRegisteredCounter("db/preimage/total", nil)
	PreimageHitCounter = metrics.NewRegisteredCounter("db/preimage/hits", nil)

	LastPrunedBlockKey = []byte("LastPrunedBlock")
)

package dbutils

import "github.com/ledgerwatch/turbo-geth/metrics"

// The fields below define the low level database schema prefixing.
var (
	// key - address hash
	// value - account encoded for storage
	AccountsBucket = []byte("AT")

	//current
	//key - key + encoded timestamp(block number)
	//value - account for storage(old/original value)
	//layout experiment
	//key - address hash
	//value - list of block where it's changed
	AccountsHistoryBucket = []byte("hAT")

	//key - address hash + incarnation + storage key hash
	//value - storage value(common.hash)
	StorageBucket = []byte("ST")

	//current
	//key - address hash + incarnation + storage key hash
	//value - storage value(common.hash)
	//layout experiment
	//key - address hash
	//value - list of block where it's changed
	StorageHistoryBucket = []byte("hST")

	//key - contract code hash
	//value - contract code
	CodeBucket = []byte("CODE")

	//key - addressHash+incarnation
	//value - code hash
	ContractCodeBucket = []byte("contractCode")

	//AccountChangeSetBucket keeps changesets of accounts
	// key - encoded timestamp(block number)
	// value - encoded ChangeSet{k - addrHash v - account(encoded).
	AccountChangeSetBucket = []byte("ACS")

	// StorageChangeSetBucket keeps changesets of storage
	// key - encoded timestamp(block number)
	// value - encoded ChangeSet{k - compositeKey(for storage) v - originalValue(common.Hash)}.
	StorageChangeSetBucket = []byte("SCS")

	// some_prefix_of(hash_of_address_of_account) => hash_of_subtrie
	IntermediateTrieHashBucket = []byte("iTh")

	// DatabaseInfoBucket is used to store information about data layout.
	DatabaseInfoBucket = []byte("DBINFO")

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

	// last block that was pruned
	// it's saved one in 5 minutes
	LastPrunedBlockKey = []byte("LastPrunedBlock")

	// LastAppliedMigration keep the name of tle last applied migration.
	LastAppliedMigration = []byte("lastAppliedMigration")

	//StorageModeHistory - does node save history.
	StorageModeHistory = []byte("smHistory")
	//StorageModeReceipts - does node save receipts.
	StorageModeReceipts = []byte("smReceipts")
	//StorageModeTxIndex - does node save transactions index.
	StorageModeTxIndex = []byte("smTxIndex")
	//StorageModePreImages - does node save hash to value mapping
	StorageModePreImages = []byte("smPreImages")
	//StorageModeThinHistory - does thin history mode enabled
	StorageModeThinHistory = []byte("smThinHistory")
	//StorageModeIntermediateTrieHash - does IntermediateTrieHash feature enabled
	StorageModeIntermediateTrieHash = []byte("smIntermediateTrieHash")
)

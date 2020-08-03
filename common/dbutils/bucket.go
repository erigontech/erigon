package dbutils

import (
	"bytes"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

// Buckets
var (
	// "Plain State". The same as CurrentStateBucket, but the keys arent' hashed.

	// Contains Accounts:
	//   key - address (unhashed)
	//   value - account encoded for storage
	// Contains Storage:
	//   key - address (unhashed) + incarnation + storage key (unhashed)
	//   value - storage value(common.hash)
	PlainStateBucket = []byte("PLAIN-CST")

	// "Plain State"
	//key - address+incarnation
	//value - code hash
	PlainContractCodeBucket = []byte("PLAIN-contractCode")

	// PlainAccountChangeSetBucket keeps changesets of accounts ("plain state")
	// key - encoded timestamp(block number)
	// value - encoded ChangeSet{k - address v - account(encoded).
	PlainAccountChangeSetBucket = []byte("PLAIN-ACS")

	// PlainStorageChangeSetBucket keeps changesets of storage ("plain state")
	// key - encoded timestamp(block number)
	// value - encoded ChangeSet{k - plainCompositeKey(for storage) v - originalValue(common.Hash)}.
	PlainStorageChangeSetBucket = []byte("PLAIN-SCS")

	// Contains Accounts:
	// key - address hash
	// value - account encoded for storage
	// Contains Storage:
	//key - address hash + incarnation + storage key hash
	//value - storage value(common.hash)
	CurrentStateBucket = []byte("CST")

	//current
	//key - key + encoded timestamp(block number)
	//value - account for storage(old/original value)
	//layout experiment
	//key - address hash
	//value - list of block where it's changed
	AccountsHistoryBucket = []byte("hAT")

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

	// Incarnations for deleted accounts
	//key - address
	//value - incarnation of account when it was last deleted
	IncarnationMapBucket = []byte("incarnationMap")

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

	// Progress of sync stages: stageName -> stageData
	SyncStageProgress     = []byte("SSP2")
	SyncStageProgressOld1 = []byte("SSP")
	// Position to where to unwind sync stages: stageName -> stageData
	SyncStageUnwind     = []byte("SSU2")
	SyncStageUnwindOld1 = []byte("SSU")

	CliqueBucket = []byte("clique-")

	// this bucket stored in separated database
	InodesBucket = []byte("inodes")

	// Transaction senders - stored separately from the block bodies
	Senders = []byte("txSenders")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	FastTrieProgressKey = []byte("TrieSync")
	// headBlockKey tracks the latest know full block's hash.
	HeadBlockKey = []byte("LastBlock")
	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	HeadFastBlockKey = []byte("LastFast")
	// headHeaderKey tracks the latest know header's hash.
	HeadHeaderKey = []byte("LastHeader")

	// migrationName -> serialized SyncStageProgress and SyncStageUnwind buckets
	// it stores stages progress to understand in which context was executed migration
	// in case of bug-report developer can ask content of this bucket
	Migrations = []byte("migrations")
)

// Keys
var (
	// last block that was pruned
	// it's saved one in 5 minutes
	LastPrunedBlockKey = []byte("LastPrunedBlock")
	//StorageModeHistory - does node save history.
	StorageModeHistory = []byte("smHistory")
	//StorageModeReceipts - does node save receipts.
	StorageModeReceipts = []byte("smReceipts")
	//StorageModeTxIndex - does node save transactions index.
	StorageModeTxIndex = []byte("smTxIndex")
)

// Metrics
var (
	PreimageCounter    = metrics.NewRegisteredCounter("db/preimage/total", nil)
	PreimageHitCounter = metrics.NewRegisteredCounter("db/preimage/hits", nil)
)

// Buckets - list of all buckets. App will panic if some bucket is not in this list.
// This list will be sorted in `init` method.
// BucketsCfg - can be used to find index in sorted version of Buckets list by name
var Buckets = [][]byte{
	CurrentStateBucket,
	AccountsHistoryBucket,
	StorageHistoryBucket,
	CodeBucket,
	ContractCodeBucket,
	AccountChangeSetBucket,
	StorageChangeSetBucket,
	IntermediateTrieHashBucket,
	DatabaseVerisionKey,
	HeaderPrefix,
	HeaderNumberPrefix,
	BlockBodyPrefix,
	BlockReceiptsPrefix,
	TxLookupPrefix,
	BloomBitsPrefix,
	PreimagePrefix,
	ConfigPrefix,
	BloomBitsIndexPrefix,
	DatabaseInfoBucket,
	IncarnationMapBucket,
	CliqueBucket,
	SyncStageProgress,
	SyncStageUnwind,
	PlainStateBucket,
	PlainContractCodeBucket,
	PlainAccountChangeSetBucket,
	PlainStorageChangeSetBucket,
	InodesBucket,
	Senders,
	FastTrieProgressKey,
	HeadBlockKey,
	HeadFastBlockKey,
	HeadHeaderKey,
	Migrations,
}

// DeprecatedBuckets - list of buckets which can be programmatically deleted - for example after migration
var DeprecatedBuckets = [][]byte{
	SyncStageProgressOld1,
	SyncStageUnwindOld1,
}

var BucketsCfg = map[string]*BucketConfigItem{}

type BucketConfigItem struct {
	ID         int
	IsDupsort  bool
	DupToLen   int
	DupFromLen int
}

type dupSortConfigEntry struct {
	Bucket  []byte
	ID      int
	FromLen int
	ToLen   int
}

var dupSortConfig = []dupSortConfigEntry{
	{
		Bucket:  CurrentStateBucket,
		ToLen:   40,
		FromLen: 72,
	},
	{
		Bucket:  PlainStateBucket,
		ToLen:   28,
		FromLen: 60,
	},
}

func init() {
	sort.SliceStable(Buckets, func(i, j int) bool {
		return bytes.Compare(Buckets[i], Buckets[j]) < 0
	})

	for i := range Buckets {
		BucketsCfg[string(Buckets[i])] = createBucketConfig(i, Buckets[i])
	}

	for i := range DeprecatedBuckets {
		BucketsCfg[string(DeprecatedBuckets[i])] = createBucketConfig(len(Buckets)+i, DeprecatedBuckets[i])
	}
}

func createBucketConfig(id int, name []byte) *BucketConfigItem {
	cfg := &BucketConfigItem{ID: id}

	for _, dupCfg := range dupSortConfig {
		if !bytes.Equal(dupCfg.Bucket, name) {
			continue
		}

		cfg.DupFromLen = dupCfg.FromLen
		cfg.DupToLen = dupCfg.ToLen

		if bytes.Equal(dupCfg.Bucket, CurrentStateBucket) {
			cfg.IsDupsort = debug.IsHashedStateDupsortEnabled()
		}
		if bytes.Equal(dupCfg.Bucket, PlainStateBucket) {
			cfg.IsDupsort = debug.IsPlainStateDupsortEnabled()
		}
	}

	return cfg
}

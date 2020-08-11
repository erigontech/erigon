package dbutils

import (
	"sort"
	"strings"

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
	PlainStateBucket = "PLAIN-CST"

	// "Plain State"
	//key - address+incarnation
	//value - code hash
	PlainContractCodeBucket = "PLAIN-contractCode"

	// PlainAccountChangeSetBucket keeps changesets of accounts ("plain state")
	// key - encoded timestamp(block number)
	// value - encoded ChangeSet{k - address v - account(encoded).
	PlainAccountChangeSetBucket = "PLAIN-ACS"

	// PlainStorageChangeSetBucket keeps changesets of storage ("plain state")
	// key - encoded timestamp(block number)
	// value - encoded ChangeSet{k - plainCompositeKey(for storage) v - originalValue(common.Hash)}.
	PlainStorageChangeSetBucket = "PLAIN-SCS"

	// Contains Accounts:
	// key - address hash
	// value - account encoded for storage
	// Contains Storage:
	//key - address hash + incarnation + storage key hash
	//value - storage value(common.hash)
	CurrentStateBucket = "CST"

	//current
	//key - key + encoded timestamp(block number)
	//value - account for storage(old/original value)
	//layout experiment
	//key - address hash
	//value - list of block where it's changed
	AccountsHistoryBucket = "hAT"

	//current
	//key - address hash + incarnation + storage key hash
	//value - storage value(common.hash)
	//layout experiment
	//key - address hash
	//value - list of block where it's changed
	StorageHistoryBucket = "hST"

	//key - contract code hash
	//value - contract code
	CodeBucket = "CODE"

	//key - addressHash+incarnation
	//value - code hash
	ContractCodeBucket = "contractCode"

	// Incarnations for deleted accounts
	//key - address
	//value - incarnation of account when it was last deleted
	IncarnationMapBucket = "incarnationMap"

	//AccountChangeSetBucket keeps changesets of accounts
	// key - encoded timestamp(block number)
	// value - encoded ChangeSet{k - addrHash v - account(encoded).
	AccountChangeSetBucket = "ACS"

	// StorageChangeSetBucket keeps changesets of storage
	// key - encoded timestamp(block number)
	// value - encoded ChangeSet{k - compositeKey(for storage) v - originalValue(common.Hash)}.
	StorageChangeSetBucket = "SCS"

	// some_prefix_of(hash_of_address_of_account) => hash_of_subtrie
	IntermediateTrieHashBucket = "iTh"

	// DatabaseInfoBucket is used to store information about data layout.
	DatabaseInfoBucket = "DBINFO"

	// databaseVerisionKey tracks the current database version.
	DatabaseVerisionKey = "DatabaseVersion"

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	HeaderPrefix       = "h"         // headerPrefix + num (uint64 big endian) + hash -> header
	HeaderTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	HeaderHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	HeaderNumberPrefix = "H"         // headerNumberPrefix + hash -> num (uint64 big endian)

	BlockBodyPrefix     = "b" // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	BlockReceiptsPrefix = "r" // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	TxLookupPrefix  = "l" // txLookupPrefix + hash -> transaction/receipt lookup metadata
	BloomBitsPrefix = "B" // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits

	PreimagePrefix = "secure-key-"      // preimagePrefix + hash -> preimage
	ConfigPrefix   = "ethereum-config-" // config prefix for the db

	// Chain index prefixes (use `i` + single byte to avoid mixing data types).
	BloomBitsIndexPrefix = "iB" // BloomBitsIndexPrefix is the data table of a chain indexer to track its progress

	// Progress of sync stages: stageName -> stageData
	SyncStageProgress     = "SSP2"
	SyncStageProgressOld1 = "SSP"
	// Position to where to unwind sync stages: stageName -> stageData
	SyncStageUnwind     = "SSU2"
	SyncStageUnwindOld1 = "SSU"

	CliqueBucket = "clique-"

	// this bucket stored in separated database
	InodesBucket = "inodes"

	// Transaction senders - stored separately from the block bodies
	Senders = "txSenders"

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	FastTrieProgressKey = "TrieSync"
	// headBlockKey tracks the latest know full block's hash.
	HeadBlockKey = "LastBlock"
	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	HeadFastBlockKey = "LastFast"

	// migrationName -> serialized SyncStageProgress and SyncStageUnwind buckets
	// it stores stages progress to understand in which context was executed migration
	// in case of bug-report developer can ask content of this bucket
	Migrations = "migrations"
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

	HeadHeaderKey = "LastHeader"
)

// Metrics
var (
	PreimageCounter    = metrics.NewRegisteredCounter("db/preimage/total", nil)
	PreimageHitCounter = metrics.NewRegisteredCounter("db/preimage/hits", nil)
)

// Buckets - list of all buckets. App will panic if some bucket is not in this list.
// This list will be sorted in `init` method.
// BucketsCfg - can be used to find index in sorted version of Buckets list by name
var Buckets = []string{
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
var DeprecatedBuckets = []string{
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
	Bucket  string
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
		return strings.Compare(Buckets[i], Buckets[j]) < 0
	})

	for i := range Buckets {
		BucketsCfg[string(Buckets[i])] = createBucketConfig(i, Buckets[i])
	}

	for i := range DeprecatedBuckets {
		BucketsCfg[string(DeprecatedBuckets[i])] = createBucketConfig(len(Buckets)+i, DeprecatedBuckets[i])
	}
}

func createBucketConfig(id int, name string) *BucketConfigItem {
	cfg := &BucketConfigItem{ID: id}

	for _, dupCfg := range dupSortConfig {
		if dupCfg.Bucket != name {
			continue
		}

		cfg.DupFromLen = dupCfg.FromLen
		cfg.DupToLen = dupCfg.ToLen

		if dupCfg.Bucket == CurrentStateBucket {
			cfg.IsDupsort = debug.IsHashedStateDupsortEnabled()
		}
		if dupCfg.Bucket == PlainStateBucket {
			cfg.IsDupsort = debug.IsPlainStateDupsortEnabled()
		}
	}

	return cfg
}

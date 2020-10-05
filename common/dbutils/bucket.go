package dbutils

import (
	"bytes"
	"sort"
	"strings"

	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

// Buckets
var (
	// "Plain State". The same as CurrentStateBucket, but the keys arent' hashed.

	/*
		Logical layout:
			Contains Accounts:
			  key - address (unhashed)
			  value - account encoded for storage
			Contains Storage:
			  key - address (unhashed) + incarnation + storage key (unhashed)
			  value - storage value(common.hash)

		Physical layout:
			PlainStateBucket and CurrentStateBucket utilises DupSort feature of LMDB (store multiple values inside 1 key).
		-------------------------------------------------------------
			   key              |            value
		-------------------------------------------------------------
		[acc_hash]              | [acc_value]
		[acc_hash]+[inc]        | [storage1_hash]+[storage1_value]
								| [storage2_hash]+[storage2_value] // this value has no own key. it's 2nd value of [acc_hash]+[inc] key.
								| [storage3_hash]+[storage3_value]
								| ...
		[acc_hash]+[old_inc]    | [storage1_hash]+[storage1_value]
								| ...
		[acc2_hash]             | [acc2_value]
								...
	*/
	PlainStateBucket     = "PLAIN-CST2"
	PlainStateBucketOld1 = "PLAIN-CST"

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
	CurrentStateBucket     = "CST2"
	CurrentStateBucketOld1 = "CST"

	//key - address hash
	//value - list of block where it's changed
	AccountsHistoryBucket = "hAT"

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
	IntermediateTrieHashBucket     = "iTh2"
	IntermediateTrieHashBucketOld1 = "iTh"

	// DatabaseInfoBucket is used to store information about data layout.
	DatabaseInfoBucket = "DBINFO"
	SnapshotInfoBucket = "SNINFO"

	// databaseVerisionKey tracks the current database version.
	DatabaseVerisionKey = "DatabaseVersion"

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	HeaderPrefix       = "h"         // headerPrefix + num (uint64 big endian) + hash -> header
	HeaderTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	HeaderHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	HeaderNumberPrefix = "H"         // headerNumberPrefix + hash -> num (uint64 big endian)

	BlockBodyPrefix     = "b" // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	BlockReceiptsPrefix = "r" // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	// Stores bitmap indices - in which block numbers saw logs of given 'address' or 'topic'
	// [addr or topic] + [2 bytes inverted shard number] -> bitmap(blockN)
	// indices are sharded - because some bitmaps are >1Mb and when new incoming blocks process it
	//	 updates ~300 of bitmaps - by append small amount new values. It cause much big writes (LMDB does copy-on-write).
	//
	// if last existing shard size merge it with delta
	// if serialized size of delta > ShardLimit - break down to multiple shards
	// shard number - it's biggest value in bitmap
	LogTopicIndex   = "log_topic_index"
	LogAddressIndex = "log_address_index"

	// Indices for call traces - have the same format as LogTopicIndex and LogAddressIndex
	// Store bitmap indices - in which block number we saw calls from (CallFromIndex) or to (CallToIndex) some addresses
	CallFromIndex = "call_from_index"
	CallToIndex   = "call_to_index"

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
	//StorageModeCallTraces - does not build index of call traces
	StorageModeCallTraces = []byte("smCallTraces")

	HeadHeaderKey = "LastHeader"

	SnapshotHeadersHeadNumber = "SnapshotLastHeaderNumber"
	SnapshotHeadersHeadHash   = "SnapshotLastHeaderHash"
	SnapshotBodyHeadNumber    = "SnapshotLastBodyNumber"
	SnapshotBodyHeadHash      = "SnapshotLastBodyHash"
)

// Metrics
var (
	PreimageCounter    = metrics.NewRegisteredCounter("db/preimage/total", nil)
	PreimageHitCounter = metrics.NewRegisteredCounter("db/preimage/hits", nil)
)

// Buckets - list of all buckets. App will panic if some bucket is not in this list.
// This list will be sorted in `init` method.
// BucketsConfigs - can be used to find index in sorted version of Buckets list by name
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
	Senders,
	FastTrieProgressKey,
	HeadBlockKey,
	HeadFastBlockKey,
	HeadHeaderKey,
	Migrations,
	LogTopicIndex,
	LogAddressIndex,
	SnapshotInfoBucket,
	CallFromIndex,
	CallToIndex,
	CompressionDictionary,
}

// DeprecatedBuckets - list of buckets which can be programmatically deleted - for example after migration
var DeprecatedBuckets = []string{
	SyncStageProgressOld1,
	SyncStageUnwindOld1,
	CurrentStateBucketOld1,
	PlainStateBucketOld1,
	IntermediateTrieHashBucketOld1,
}

type CustomComparator string

const (
	DefaultCmp     CustomComparator = ""
	DupCmpSuffix32 CustomComparator = "dup_cmp_suffix32"
)

type CmpFunc func(k1, k2, v1, v2 []byte) int

func DefaultCmpFunc(k1, k2, v1, v2 []byte) int { return bytes.Compare(k1, k2) }
func DefaultDupCmpFunc(k1, k2, v1, v2 []byte) int {
	cmp := bytes.Compare(k1, k2)
	if cmp == 0 {
		cmp = bytes.Compare(v1, v2)
	}
	return cmp
}

type BucketsCfg map[string]BucketConfigItem
type Bucket string

type BucketConfigItem struct {
	Flags uint
	// AutoDupSortKeysConversion - enables some keys transformation - to change db layout without changing app code.
	// Use it wisely - it helps to do experiments with DB format faster, but better reduce amount of Magic in app.
	// If good DB format found, push app code to accept this format and then disable this property.
	AutoDupSortKeysConversion bool
	IsDeprecated              bool
	DBI                       lmdb.DBI
	// DupFromLen - if user provide key of this length, then next transformation applied:
	// v = append(k[DupToLen:], v...)
	// k = k[:DupToLen]
	// And opposite at retrieval
	// Works only if AutoDupSortKeysConversion enabled
	DupFromLen          int
	DupToLen            int
	DupFixedSize        int
	CustomComparator    CustomComparator
	CustomDupComparator CustomComparator
}

var BucketsConfigs = BucketsCfg{
	CurrentStateBucket: {
		Flags:                     lmdb.DupSort,
		AutoDupSortKeysConversion: true,
		DupFromLen:                72,
		DupToLen:                  40,
	},
	PlainStateBucket: {
		Flags:                     lmdb.DupSort,
		AutoDupSortKeysConversion: true,
		DupFromLen:                60,
		DupToLen:                  28,
	},
	IntermediateTrieHashBucket: {
		Flags:               lmdb.DupSort,
		CustomDupComparator: DupCmpSuffix32,
	},
}

func sortBuckets() {
	sort.SliceStable(Buckets, func(i, j int) bool {
		return strings.Compare(Buckets[i], Buckets[j]) < 0
	})
}

func DefaultBuckets() BucketsCfg {
	return BucketsConfigs
}

func UpdateBucketsList(newBucketCfg BucketsCfg) {
	newBuckets := make([]string, 0)
	for k, v := range newBucketCfg {
		if !v.IsDeprecated {
			newBuckets = append(newBuckets, k)
		}
	}
	Buckets = newBuckets
	BucketsConfigs = newBucketCfg

	reinit()
}

func init() {
	reinit()
}

func reinit() {
	sortBuckets()

	for _, name := range Buckets {
		_, ok := BucketsConfigs[name]
		if !ok {
			BucketsConfigs[name] = BucketConfigItem{}
		}
	}

	for _, name := range DeprecatedBuckets {
		_, ok := BucketsConfigs[name]
		if !ok {
			BucketsConfigs[name] = BucketConfigItem{}
		}
		tmp := BucketsConfigs[name]
		tmp.IsDeprecated = true
		BucketsConfigs[name] = tmp
	}
}

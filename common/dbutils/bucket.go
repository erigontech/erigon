package dbutils

import (
	"bytes"
	"sort"
	"strings"
)

// Buckets

// Dictionary:
// "Plain State" - state where keys arent' hashed. "CurrentState" - same, but keys are hashed. "PlainState" used for blocks execution. "CurrentState" used mostly for Merkle root calculation.
// "incarnation" - uint64 number - how much times given account was SelfDestruct'ed.

/*PlainStateBucket
Logical layout:
	Contains Accounts:
	  key - address (unhashed)
	  value - account encoded for storage
	Contains Storage:
	  key - address (unhashed) + incarnation + storage key (unhashed)
	  value - storage value(common.hash)

Physical layout:
	PlainStateBucket and HashedStorageBucket utilises DupSort feature of LMDB (store multiple values inside 1 key).
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
var PlainStateBucket = "PLAIN-CST2"
var PlainStateBucketOld1 = "PLAIN-CST"

var (
	//PlainContractCodeBucket -
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

	//HashedAccountsBucket
	// key - address hash
	// value - account encoded for storage
	// Contains Storage:
	//key - address hash + incarnation + storage key hash
	//value - storage value(common.hash)
	HashedAccountsBucket   = "hashed_accounts"
	HashedStorageBucket    = "hashed_storage"
	CurrentStateBucketOld2 = "CST2"
	CurrentStateBucketOld1 = "CST"

	//key - address + shard_id_u64
	//value - roaring bitmap  - list of block where it changed
	AccountsHistoryBucket = "hAT"

	//key - address + storage_key + shard_id_u64
	//value - roaring bitmap - list of block where it changed
	StorageHistoryBucket = "hST"

	//key - contract code hash
	//value - contract code
	CodeBucket = "CODE"

	//key - addressHash+incarnation
	//value - code hash
	ContractCodeBucket = "contractCode"

	// IncarnationMapBucket for deleted accounts
	//key - address
	//value - incarnation of account when it was last deleted
	IncarnationMapBucket = "incarnationMap"
)

/*TrieOfAccountsBucket and TrieOfStorageBucket
hasState,groups - mark prefixes existing in hashed_account table
hasTree - mark prefixes existing in trie_account table (not related with branchNodes)
hasHash - mark prefixes which hashes are saved in current trie_account record (actually only hashes of branchNodes can be saved)
@see UnmarshalTrieNode
@see integrity.Trie

+-----------------------------------------------------------------------------------------------------+
| DB record: 0x0B, hasState: 0b1011, hasTree: 0b1001, hasHash: 0b1001, hashes: [x,x]                  |
+-----------------------------------------------------------------------------------------------------+
                |                                           |                               |
                v                                           |                               v
+---------------------------------------------+             |            +--------------------------------------+
| DB record: 0x0B00, hasState: 0b10001        |             |            | DB record: 0x0B03, hasState: 0b10010 |
| hasTree: 0, hasHash: 0b10000, hashes: [x]   |             |            | hasTree: 0, hasHash: 0, hashes: []   |
+---------------------------------------------+             |            +--------------------------------------+
        |                    |                              |                         |                  |
        v                    v                              v                         v                  v
+------------------+    +----------------------+     +---------------+        +---------------+  +---------------+
| Account:         |    | BranchNode: 0x0B0004 |     | Account:      |        | Account:      |  | Account:      |
| 0x0B0000...      |    | has no record in     |     | 0x0B01...     |        | 0x0B0301...   |  | 0x0B0304...   |
| in HashedAccount |    |     TrieAccount      |     |               |        |               |  |               |
+------------------+    +----------------------+     +---------------+        +---------------+  +---------------+
                           |                |
                           v                v
		           +---------------+  +---------------+
		           | Account:      |  | Account:      |
		           | 0x0B000400... |  | 0x0B000401... |
		           +---------------+  +---------------+
Invariants:
- hasTree is subset of hasState
- hasHash is subset of hasState
- first level in account_trie always exists if hasState>0
- TrieStorage record of account.root (length=40) must have +1 hash - it's account.root
- each record in TrieAccount table must have parent (may be not direct) and this parent must have correct bit in hasTree bitmap
- if hasState has bit - then HashedAccount table must have record according to this bit
- each TrieAccount record must cover some state (means hasState is always > 0)
- TrieAccount records with length=1 can satisfy (hasBranch==0&&hasHash==0) condition
- Other records in TrieAccount and TrieStorage must (hasTree!=0 || hasHash!=0)
*/
var TrieOfAccountsBucket = "trie_account"
var TrieOfStorageBucket = "trie_storage"
var IntermediateTrieHashBucketOld1 = "iTh"
var IntermediateTrieHashBucketOld2 = "iTh2"

var (
	// DatabaseInfoBucket is used to store information about data layout.
	DatabaseInfoBucket        = "DBINFO"
	SnapshotInfoBucket        = "SNINFO"
	HeadersSnapshotInfoBucket = "hSNINFO"
	BodiesSnapshotInfoBucket  = "bSNINFO"
	StateSnapshotInfoBucket   = "sSNINFO"

	// databaseVerisionKey tracks the current database version.
	DatabaseVerisionKey = "DatabaseVersion"

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	HeaderPrefixOld    = "h" // block_num_u64 + hash -> header
	HeaderNumberBucket = "H" // headerNumberPrefix + hash -> num (uint64 big endian)

	HeaderCanonicalBucket = "canonical_headers" // block_num_u64 -> header hash
	HeadersBucket         = "headers"           // block_num_u64 + hash -> header (RLP)
	HeaderTDBucket        = "header_to_td"      // block_num_u64 + hash -> td (RLP)

	BlockBodyPrefix     = "b"      // block_num_u64 + hash -> block body
	EthTx               = "eth_tx" // tbl_sequence_u64 -> rlp(tx)
	BlockReceiptsPrefix = "r"      // block_num_u64 + hash -> block receipts
	Log                 = "log"    // block_num_u64 + hash -> block receipts

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

	InvalidBlock    = "InvalidBlock"     // Inherited from go-ethereum, not used in turbo-geth yet
	UncleanShutdown = "unclean-shutdown" // Inherited from go-ethereum, not used in turbo-geth yet

	// migrationName -> serialized SyncStageProgress and SyncStageUnwind buckets
	// it stores stages progress to understand in which context was executed migration
	// in case of bug-report developer can ask content of this bucket
	Migrations = "migrations"

	Sequence = "sequence" // tbl_name -> seq_u64

)

// Keys
var (
	// last  block that was pruned
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
//var (
//PreimageCounter    = metrics.NewRegisteredCounter("db/preimage/total", nil)
//PreimageHitCounter = metrics.NewRegisteredCounter("db/preimage/hits", nil)
//)

// Buckets - list of all buckets. App will panic if some bucket is not in this list.
// This list will be sorted in `init` method.
// BucketsConfigs - can be used to find index in sorted version of Buckets list by name
var Buckets = []string{
	CurrentStateBucketOld2,
	AccountsHistoryBucket,
	StorageHistoryBucket,
	CodeBucket,
	ContractCodeBucket,
	DatabaseVerisionKey,
	HeaderNumberBucket,
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
	HeadersSnapshotInfoBucket,
	BodiesSnapshotInfoBucket,
	StateSnapshotInfoBucket,
	CallFromIndex,
	CallToIndex,
	Log,
	Sequence,
	EthTx,
	TrieOfAccountsBucket,
	TrieOfStorageBucket,
	HashedAccountsBucket,
	HashedStorageBucket,
	IntermediateTrieHashBucketOld2,

	HeaderCanonicalBucket,
	HeadersBucket,
	HeaderTDBucket,
}

// DeprecatedBuckets - list of buckets which can be programmatically deleted - for example after migration
var DeprecatedBuckets = []string{
	SyncStageProgressOld1,
	SyncStageUnwindOld1,
	CurrentStateBucketOld1,
	PlainStateBucketOld1,
	IntermediateTrieHashBucketOld1,
	HeaderPrefixOld,
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

type DBI uint
type BucketFlags uint

const (
	Default    BucketFlags = 0x00
	ReverseKey BucketFlags = 0x02
	DupSort    BucketFlags = 0x04
	IntegerKey BucketFlags = 0x08
	IntegerDup BucketFlags = 0x20
	ReverseDup BucketFlags = 0x40
)

type BucketConfigItem struct {
	Flags BucketFlags
	// AutoDupSortKeysConversion - enables some keys transformation - to change db layout without changing app code.
	// Use it wisely - it helps to do experiments with DB format faster, but better reduce amount of Magic in app.
	// If good DB format found, push app code to accept this format and then disable this property.
	AutoDupSortKeysConversion bool
	IsDeprecated              bool
	DBI                       DBI
	// DupFromLen - if user provide key of this length, then next transformation applied:
	// v = append(k[DupToLen:], v...)
	// k = k[:DupToLen]
	// And opposite at retrieval
	// Works only if AutoDupSortKeysConversion enabled
	DupFromLen          int
	DupToLen            int
	CustomComparator    CustomComparator
	CustomDupComparator CustomComparator
}

var BucketsConfigs = BucketsCfg{
	CurrentStateBucketOld2: {
		Flags:                     DupSort,
		AutoDupSortKeysConversion: true,
		DupFromLen:                72,
		DupToLen:                  40,
	},
	HashedStorageBucket: {
		Flags:                     DupSort,
		AutoDupSortKeysConversion: true,
		DupFromLen:                72,
		DupToLen:                  40,
	},
	PlainAccountChangeSetBucket: {
		Flags: DupSort,
	},
	PlainStorageChangeSetBucket: {
		Flags: DupSort,
	},
	PlainStateBucket: {
		Flags:                     DupSort,
		AutoDupSortKeysConversion: true,
		DupFromLen:                60,
		DupToLen:                  28,
	},
	IntermediateTrieHashBucketOld2: {
		Flags:               DupSort,
		CustomDupComparator: DupCmpSuffix32,
	},
	InvalidBlock: {},
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

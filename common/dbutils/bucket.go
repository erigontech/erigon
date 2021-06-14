package dbutils

import (
	"bytes"
	"sort"
	"strings"

	"github.com/ledgerwatch/erigon/gointerfaces/types"
)

// DBSchemaVersion
var DBSchemaVersion = types.VersionReply{Major: 2, Minor: 1, Patch: 0}

// Buckets

// Dictionary:
// "Plain State" - state where keys arent' hashed. "CurrentState" - same, but keys are hashed. "PlainState" used for blocks execution. "CurrentState" used mostly for Merkle root calculation.
// "incarnation" - uint64 number - how much times given account was SelfDestruct'ed.

/*
PlainStateBucket logical layout:
	Contains Accounts:
	  key - address (unhashed)
	  value - account encoded for storage
	Contains Storage:
	  key - address (unhashed) + incarnation + storage key (unhashed)
	  value - storage value(common.hash)

Physical layout:
	PlainStateBucket and HashedStorageBucket utilises DupSort feature of MDBX (store multiple values inside 1 key).
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
const PlainStateBucket = "PLAIN-CST2"
const PlainStateBucketOld1 = "PLAIN-CST"

//PlainContractCodeBucket -
//key - address+incarnation
//value - code hash
const PlainContractCodeBucket = "PLAIN-contractCode"

/*
AccountChangeSetBucket and StorageChangeSetBucket - of block N store values of state before block N changed them.
Because values "after" change stored in PlainState.
Logical format:
	key - blockNum_u64 + key_in_plain_state
	value - value_in_plain_state_before_blockNum_changes

Example: If block N changed account A from value X to Y. Then:
	AccountChangeSetBucket has record: bigEndian(N) + A -> X
	PlainStateBucket has record: A -> Y

See also: docs/programmers_guide/db_walkthrough.MD#table-history-of-accounts

As you can see if block N changes much accounts - then all records have repetitive prefix `bigEndian(N)`.
MDBX can store such prefixes only once - by DupSort feature (see `docs/programmers_guide/dupsort.md`).
Both buckets are DupSort-ed and have physical format:
AccountChangeSetBucket:
	key - blockNum_u64
	value - address + account(encoded)

StorageChangeSetBucket:
	key - blockNum_u64 + address + incarnation_u64
	value - plain_storage_key + value
*/
const AccountChangeSetBucket = "PLAIN-ACS"
const StorageChangeSetBucket = "PLAIN-SCS"

const (

	//HashedAccountsBucket
	// key - address hash
	// value - account encoded for storage
	// Contains Storage:
	//key - address hash + incarnation + storage key hash
	//value - storage value(common.hash)
	HashedAccountsBucket   = "hashed_accounts"
	HashedStorageBucket    = "hashed_storage"
	CurrentStateBucketOld2 = "CST2"
)

/*
AccountsHistoryBucket and StorageHistoryBucket - indices designed to serve next 2 type of requests:
1. what is smallest block number >= X where account A changed
2. get last shard of A - to append there new block numbers

Task 1. is part of "get historical state" operation (see `core/state:GetAsOf`):
If `db.Seek(A+bigEndian(X))` returns non-last shard -
		then get block number from shard value Y := RoaringBitmap(shard_value).GetGte(X)
		and with Y go to ChangeSets: db.Get(ChangeSets, Y+A)
If `db.Seek(A+bigEndian(X))` returns last shard -
		then we go to PlainState: db.Get(PlainState, A)

Format:
	- index split to shards by 2Kb - RoaringBitmap encoded sorted list of block numbers
			(to avoid performance degradation of popular accounts or look deep into history.
				Also 2Kb allows avoid Overflow pages inside DB.)
	- if shard is not last - then key has suffix 8 bytes = bigEndian(max_block_num_in_this_shard)
	- if shard is last - then key has suffix 8 bytes = 0xFF

It allows:
	- server task 1. by 1 db operation db.Seek(A+bigEndian(X))
	- server task 2. by 1 db operation db.Get(A+0xFF)

see also: docs/programmers_guide/db_walkthrough.MD#table-change-sets

AccountsHistoryBucket:
	key - address + shard_id_u64
	value - roaring bitmap  - list of block where it changed
StorageHistoryBucket
	key - address + storage_key + shard_id_u64
	value - roaring bitmap - list of block where it changed
*/
var AccountsHistoryBucket = "hAT"
var StorageHistoryBucket = "hST"

var (

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

	//TEVMCodeBucket -
	//key - contract code hash
	//value - contract TEVM code
	ContractTEVMCodeBucket = "TEVMCode"
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
const TrieOfAccountsBucket = "trie_account"
const TrieOfStorageBucket = "trie_storage"
const IntermediateTrieHashBucketOld2 = "iTh2"

const (
	// DatabaseInfoBucket is used to store information about data layout.
	DatabaseInfoBucket        = "DBINFO"
	SnapshotInfoBucket        = "SNINFO"
	BittorrentInfoBucket      = "BTINFO"
	HeadersSnapshotInfoBucket = "hSNINFO"
	BodiesSnapshotInfoBucket  = "bSNINFO"
	StateSnapshotInfoBucket   = "sSNINFO"

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	HeaderPrefixOld    = "h" // block_num_u64 + hash -> header
	HeaderNumberBucket = "H" // headerNumberPrefix + hash -> num (uint64 big endian)

	HeaderCanonicalBucket = "canonical_headers" // block_num_u64 -> header hash
	HeadersBucket         = "headers"           // block_num_u64 + hash -> header (RLP)
	HeaderTDBucket        = "header_to_td"      // block_num_u64 + hash -> td (RLP)

	BlockBodyPrefix     = "b"      // block_num_u64 + hash -> block body
	EthTx               = "eth_tx" // tbl_sequence_u64 -> rlp(tx)
	BlockReceiptsPrefix = "r"      // block_num_u64 -> canonical block receipts (non-canonical are not stored)
	Log                 = "log"    // block_num_u64 + txId -> logs of transaction

	// Stores bitmap indices - in which block numbers saw logs of given 'address' or 'topic'
	// [addr or topic] + [2 bytes inverted shard number] -> bitmap(blockN)
	// indices are sharded - because some bitmaps are >1Mb and when new incoming blocks process it
	//	 updates ~300 of bitmaps - by append small amount new values. It cause much big writes (MDBX does copy-on-write).
	//
	// if last existing shard size merge it with delta
	// if serialized size of delta > ShardLimit - break down to multiple shards
	// shard number - it's biggest value in bitmap
	LogTopicIndex   = "log_topic_index"
	LogAddressIndex = "log_address_index"

	// CallTraceSet is the name of the table that contain the mapping of block number to the set (sorted) of all accounts
	// touched by call traces. It is DupSort-ed table
	// 8-byte BE block number -> account address -> two bits (one for "from", another for "to")
	CallTraceSet = "call_trace_set"
	// Indices for call traces - have the same format as LogTopicIndex and LogAddressIndex
	// Store bitmap indices - in which block number we saw calls from (CallFromIndex) or to (CallToIndex) some addresses
	CallFromIndex = "call_from_index"
	CallToIndex   = "call_to_index"

	TxLookupPrefix  = "l" // hash -> transaction/receipt lookup metadata
	BloomBitsPrefix = "B" // bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits

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

	CliqueBucket             = "clique-"
	CliqueSeparateBucket     = "clique-snapshots-"
	CliqueSnapshotBucket     = "snap"
	CliqueLastSnapshotBucket = "lastSnap"

	// this bucket stored in separated database
	InodesBucket = "inodes"

	// Transaction senders - stored separately from the block bodies
	Senders = "txSenders" // block_num_u64 + blockHash -> sendersList (no serialization format, every 20 bytes is new sender)

	// headBlockKey tracks the latest know full block's hash.
	HeadBlockKey = "LastBlock"

	// migrationName -> serialized SyncStageProgress and SyncStageUnwind buckets
	// it stores stages progress to understand in which context was executed migration
	// in case of bug-report developer can ask content of this bucket
	Migrations = "migrations"

	Sequence      = "sequence" // tbl_name -> seq_u64
	HeadHeaderKey = "LastHeader"
)

var Rename = map[string]string{
	PlainStateBucket:          "PlainState",
	PlainContractCodeBucket:   "PlainCodeHash",
	AccountChangeSetBucket:    "AccountChangeSet",
	StorageChangeSetBucket:    "StorageChangeSet",
	HashedAccountsBucket:      "HashedAccount",
	HashedStorageBucket:       "HashedStorage",
	AccountsHistoryBucket:     "AccountHistory",
	StorageHistoryBucket:      "StorageHistory",
	CodeBucket:                "Code",
	ContractCodeBucket:        "HashedCodeHash",
	IncarnationMapBucket:      "IncarnationMap",
	TrieOfAccountsBucket:      "TrieAccount",
	TrieOfStorageBucket:       "TrieStorage",
	DatabaseInfoBucket:        "DbInfo",
	SnapshotInfoBucket:        "SnapshotInfo",
	BittorrentInfoBucket:      "BittorrentInfo",
	HeadersSnapshotInfoBucket: "HeadersSnapshotInfo",
	BodiesSnapshotInfoBucket:  "BodiesSnapshotInfo",
	StateSnapshotInfoBucket:   "StateSnapshotInfo",
	HeaderNumberBucket:        "HeaderNumber",
	HeaderCanonicalBucket:     "CanonicalHeader",
	HeadersBucket:             "Header",
	HeaderTDBucket:            "HeadersTotalDifficulty",
	BlockBodyPrefix:           "BlockBody",
	EthTx:                     "BlockTransaction",
	BlockReceiptsPrefix:       "Receipt",
	Log:                       "TransactionLog",
	LogTopicIndex:             "LogTopicIndex",
	LogAddressIndex:           "LogAddressIndex",
	CallTraceSet:              "CallTraceSet",
	CallFromIndex:             "CallFromIndex",
	CallToIndex:               "CallToIndex",
	TxLookupPrefix:            "BlockTransactionLookup",
	BloomBitsPrefix:           "BloomBits",
	PreimagePrefix:            "Preimage",
	ConfigPrefix:              "Config",
	BloomBitsIndexPrefix:      "BloomBitsIndex",
	SyncStageProgress:         "SyncStage",
	SyncStageUnwind:           "SyncStageUnwind",
	CliqueBucket:              "Clique",
	CliqueSeparateBucket:      "CliqueSeparate",
	CliqueSnapshotBucket:      "CliqueSnapshot",
	CliqueLastSnapshotBucket:  "CliqueLastSnapshot",
	InodesBucket:              "Inode",
	Senders:                   "TxSender",
	HeadBlockKey:              "LastBlock",
	Migrations:                "Migration",
	Sequence:                  "Sequence",
	HeadHeaderKey:             "LastHeader",
}

// Keys
var (
	//StorageModeHistory - does node save history.
	StorageModeHistory = []byte("smHistory")
	//StorageModeReceipts - does node save receipts.
	StorageModeReceipts = []byte("smReceipts")
	//StorageModeTxIndex - does node save transactions index.
	StorageModeTxIndex = []byte("smTxIndex")
	//StorageModeCallTraces - does not build index of call traces
	StorageModeCallTraces = []byte("smCallTraces")
	//StorageModeTEVM - does not translate EVM to TEVM
	StorageModeTEVM = []byte("smTEVM")

	DBSchemaVersionKey = []byte("dbVersion")

	SnapshotHeadersHeadNumber = "SnapshotLastHeaderNumber"
	SnapshotHeadersHeadHash   = "SnapshotLastHeaderHash"
	SnapshotBodyHeadNumber    = "SnapshotLastBodyNumber"
	SnapshotBodyHeadHash      = "SnapshotLastBodyHash"

	BittorrentPeerID            = "peerID"
	CurrentHeadersSnapshotHash  = []byte("CurrentHeadersSnapshotHash")
	CurrentHeadersSnapshotBlock = []byte("CurrentHeadersSnapshotBlock")
)

// Buckets - list of all buckets. App will panic if some bucket is not in this list.
// This list will be sorted in `init` method.
// BucketsConfigs - can be used to find index in sorted version of Buckets list by name
var Buckets = []string{
	AccountsHistoryBucket,
	StorageHistoryBucket,
	CodeBucket,
	ContractCodeBucket,
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
	ContractTEVMCodeBucket,
	CliqueSeparateBucket,
	CliqueLastSnapshotBucket,
	CliqueSnapshotBucket,
	SyncStageProgress,
	SyncStageUnwind,
	PlainStateBucket,
	PlainContractCodeBucket,
	AccountChangeSetBucket,
	StorageChangeSetBucket,
	Senders,
	HeadBlockKey,
	HeadHeaderKey,
	Migrations,
	LogTopicIndex,
	LogAddressIndex,
	SnapshotInfoBucket,
	HeadersSnapshotInfoBucket,
	BodiesSnapshotInfoBucket,
	StateSnapshotInfoBucket,
	CallTraceSet,
	CallFromIndex,
	CallToIndex,
	Log,
	Sequence,
	EthTx,
	TrieOfAccountsBucket,
	TrieOfStorageBucket,
	HashedAccountsBucket,
	HashedStorageBucket,
	BittorrentInfoBucket,
	HeaderCanonicalBucket,
	HeadersBucket,
	HeaderTDBucket,
}

// DeprecatedBuckets - list of buckets which can be programmatically deleted - for example after migration
var DeprecatedBuckets = []string{
	IntermediateTrieHashBucketOld2,
	CurrentStateBucketOld2,
	SyncStageProgressOld1,
	SyncStageUnwindOld1,
	PlainStateBucketOld1,
	HeaderPrefixOld,
	CliqueBucket,
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
	AccountChangeSetBucket: {
		Flags: DupSort,
	},
	StorageChangeSetBucket: {
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
	CallTraceSet: {
		Flags: DupSort,
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

/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package kv

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

// DBSchemaVersion versions list
// 5.0 - BlockTransaction table now has canonical ids (txs of non-canonical blocks moving to NonCanonicalTransaction table)
// 6.0 - BlockTransaction table now has system-txs before and after block (records are absent if block has no system-tx, but sequence increasing)
// 6.1 - Canonical/NonCanonical/BadBlock transations now stored in same table: kv.EthTx. Add kv.BadBlockNumber table
var DBSchemaVersion = types.VersionReply{Major: 6, Minor: 1, Patch: 0}

// ChaindataTables

// Dictionary:
// "Plain State" - state where keys arent' hashed. "CurrentState" - same, but keys are hashed. "PlainState" used for blocks execution. "CurrentState" used mostly for Merkle root calculation.
// "incarnation" - uint64 number - how much times given account was SelfDestruct'ed.

/*
PlainState logical layout:

	Contains Accounts:
	  key - address (unhashed)
	  value - account encoded for storage
	Contains Storage:
	  key - address (unhashed) + incarnation + storage key (unhashed)
	  value - storage value(common.hash)

Physical layout:

	PlainState and HashedStorage utilises DupSort feature of MDBX (store multiple values inside 1 key).

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
const PlainState = "PlainState"

// PlainContractCode -
// key - address+incarnation
// value - code hash
const PlainContractCode = "PlainCodeHash"

/*
AccountChangeSet and StorageChangeSet - of block N store values of state before block N changed them.
Because values "after" change stored in PlainState.
Logical format:

	key - blockNum_u64 + key_in_plain_state
	value - value_in_plain_state_before_blockNum_changes

Example: If block N changed account A from value X to Y. Then:

	AccountChangeSet has record: bigEndian(N) + A -> X
	PlainState has record: A -> Y

See also: docs/programmers_guide/db_walkthrough.MD#table-history-of-accounts

As you can see if block N changes much accounts - then all records have repetitive prefix `bigEndian(N)`.
MDBX can store such prefixes only once - by DupSort feature (see `docs/programmers_guide/dupsort.md`).
Both buckets are DupSort-ed and have physical format:
AccountChangeSet:

	key - blockNum_u64
	value - address + account(encoded)

StorageChangeSet:

	key - blockNum_u64 + address + incarnation_u64
	value - plain_storage_key + value
*/
const AccountChangeSet = "AccountChangeSet"
const StorageChangeSet = "StorageChangeSet"

const (

	//HashedAccounts
	// key - address hash
	// value - account encoded for storage
	// Contains Storage:
	//key - address hash + incarnation + storage key hash
	//value - storage value(common.hash)
	HashedAccounts = "HashedAccount"
	HashedStorage  = "HashedStorage"
)

/*
AccountsHistory and StorageHistory - indices designed to serve next 2 type of requests:
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

AccountsHistory:

	key - address + shard_id_u64
	value - roaring bitmap  - list of block where it changed

StorageHistory

	key - address + storage_key + shard_id_u64
	value - roaring bitmap - list of block where it changed
*/
const E2AccountsHistory = "AccountHistory"
const E2StorageHistory = "StorageHistory"

const (

	//key - contract code hash
	//value - contract code
	Code = "Code"

	//key - addressHash+incarnation
	//value - code hash
	ContractCode = "HashedCodeHash"

	// IncarnationMap for deleted accounts
	//key - address
	//value - incarnation of account when it was last deleted
	IncarnationMap = "IncarnationMap"

	//TEVMCode -
	//key - contract code hash
	//value - contract TEVM code
	ContractTEVMCode = "TEVMCode"
)

/*
TrieOfAccounts and TrieOfStorage
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
const TrieOfAccounts = "TrieAccount"
const TrieOfStorage = "TrieStorage"

// Mapping [block number] => [Verkle Root]
const VerkleRoots = "VerkleRoots"

// Mapping [Verkle Root] => [Rlp-Encoded Verkle Node]
const VerkleTrie = "VerkleTrie"

const (
	// DatabaseInfo is used to store information about data layout.
	DatabaseInfo = "DbInfo"

	// Naming:
	//   NeaderNumber - Ethereum-specific block number. All nodes have same BlockNum.
	//   NeaderID - auto-increment ID. Depends on order in which node see headers.
	//      Invariant: for all headers in snapshots Number == ID. It means no reason to store Num/ID for this headers in DB.
	//   Same about: TxNum/TxID, BlockNum/BlockID
	HeaderNumber    = "HeaderNumber"           // header_hash -> header_num_u64
	BadHeaderNumber = "BadHeaderNumber"        // header_hash -> header_num_u64
	HeaderCanonical = "CanonicalHeader"        // block_num_u64 -> header hash
	Headers         = "Header"                 // block_num_u64 + hash -> header (RLP)
	HeaderTD        = "HeadersTotalDifficulty" // block_num_u64 + hash -> td (RLP)

	BlockBody = "BlockBody" // block_num_u64 + hash -> block body

	// Naming:
	//  TxNum - Ethereum canonical transaction number - same across all nodes.
	//  TxnID - auto-increment ID - can be differrent across all nodes
	//  BlockNum/BlockID - same
	//
	// EthTx - stores all transactions of Canonical/NonCanonical/Bad blocks
	// TxnID (auto-increment ID) - means nodes in network will have different ID of same transactions
	// Snapshots (frozen data): using TxNum (not TxnID)
	//
	// During ReOrg - txs are not removed/updated
	//
	// Also this table has system-txs before and after block: if
	// block has no system-tx - records are absent, but TxnID increasing
	//
	// In Erigon3: table MaxTxNum storing TxNum (not TxnID). History/Indices are using TxNum (not TxnID).
	EthTx           = "BlockTransaction"        // tx_id_u64 -> rlp(tx)
	NonCanonicalTxs = "NonCanonicalTransaction" // tbl_sequence_u64 -> rlp(tx)
	MaxTxNum        = "MaxTxNum"                // block_number_u64 -> max_tx_num_in_block_u64

	Receipts = "Receipt"        // block_num_u64 -> canonical block receipts (non-canonical are not stored)
	Log      = "TransactionLog" // block_num_u64 + txId -> logs of transaction

	// Stores bitmap indices - in which block numbers saw logs of given 'address' or 'topic'
	// [addr or topic] + [2 bytes inverted shard number] -> bitmap(blockN)
	// indices are sharded - because some bitmaps are >1Mb and when new incoming blocks process it
	//	 updates ~300 of bitmaps - by append small amount new values. It cause much big writes (MDBX does copy-on-write).
	//
	// if last existing shard size merge it with delta
	// if serialized size of delta > ShardLimit - break down to multiple shards
	// shard number - it's biggest value in bitmap
	LogTopicIndex   = "LogTopicIndex"
	LogAddressIndex = "LogAddressIndex"

	// CallTraceSet is the name of the table that contain the mapping of block number to the set (sorted) of all accounts
	// touched by call traces. It is DupSort-ed table
	// 8-byte BE block number -> account address -> two bits (one for "from", another for "to")
	CallTraceSet = "CallTraceSet"
	// Indices for call traces - have the same format as LogTopicIndex and LogAddressIndex
	// Store bitmap indices - in which block number we saw calls from (CallFromIndex) or to (CallToIndex) some addresses
	CallFromIndex = "CallFromIndex"
	CallToIndex   = "CallToIndex"

	// Cumulative indexes for estimation of stage execution
	CumulativeGasIndex         = "CumulativeGasIndex"
	CumulativeTransactionIndex = "CumulativeTransactionIndex"

	TxLookup = "BlockTransactionLookup" // hash -> transaction/receipt lookup metadata

	ConfigTable = "Config" // config prefix for the db

	// Progress of sync stages: stageName -> stageData
	SyncStageProgress = "SyncStage"

	Clique             = "Clique"
	CliqueSeparate     = "CliqueSeparate"
	CliqueSnapshot     = "CliqueSnapshot"
	CliqueLastSnapshot = "CliqueLastSnapshot"

	// Proof-of-stake
	// Beacon chain head that is been executed at the current time
	CurrentExecutionPayload = "CurrentExecutionPayload"

	// Node database tables (see nodedb.go)

	// NodeRecords stores P2P node records (ENR)
	NodeRecords = "NodeRecord"
	// Inodes stores P2P discovery service info about the nodes
	Inodes = "Inode"

	// Transaction senders - stored separately from the block bodies
	Senders = "TxSender" // block_num_u64 + blockHash -> sendersList (no serialization format, every 20 bytes is new sender)

	// headBlockKey tracks the latest know full block's hash.
	HeadBlockKey = "LastBlock"

	HeadHeaderKey = "LastHeader"

	// headBlockHash, safeBlockHash, finalizedBlockHash of the latest Engine API forkchoice
	LastForkchoice = "LastForkchoice"

	// TransitionBlockKey tracks the last proof-of-work block
	TransitionBlockKey = "TransitionBlock"

	// migrationName -> serialized SyncStageProgress and SyncStageUnwind buckets
	// it stores stages progress to understand in which context was executed migration
	// in case of bug-report developer can ask content of this bucket
	Migrations = "Migration"

	Sequence = "Sequence" // tbl_name -> seq_u64

	Epoch        = "DevEpoch"        // block_num_u64+block_hash->transition_proof
	PendingEpoch = "DevPendingEpoch" // block_num_u64+block_hash->transition_proof

	Issuance = "Issuance" // block_num_u64->RLP(issuance+burnt[0 if < london])

	StateAccounts   = "StateAccounts"
	StateStorage    = "StateStorage"
	StateCode       = "StateCode"
	StateCommitment = "StateCommitment"

	// BOR

	BorReceipts  = "BorReceipt"
	BorFinality  = "BorFinality"
	BorTxLookup  = "BlockBorTransactionLookup" // transaction_hash -> block_num_u64
	BorSeparate  = "BorSeparate"               // persisted snapshots of the Validator Sets, with their proposer priorities
	BorEvents    = "BorEvents"                 // event_id -> event_payload
	BorEventNums = "BorEventNums"              // block_num -> event_id (first event_id in that block)
	BorSpans     = "BorSpans"                  // span_id -> span (in JSON encoding)

	// Downloader
	BittorrentCompletion = "BittorrentCompletion"
	BittorrentInfo       = "BittorrentInfo"

	// Domains/Histry/InvertedIndices
	// Contants have "Tbl" prefix, to avoid collision with actual Domain names
	// This constants is very rarely used in APP, but Domain/History/Idx names are widely used
	TblAccountKeys        = "AccountKeys"
	TblAccountVals        = "AccountVals"
	TblAccountHistoryKeys = "AccountHistoryKeys"
	TblAccountHistoryVals = "AccountHistoryVals"
	TblAccountIdx         = "AccountIdx"

	TblStorageKeys        = "StorageKeys"
	TblStorageVals        = "StorageVals"
	TblStorageHistoryKeys = "StorageHistoryKeys"
	TblStorageHistoryVals = "StorageHistoryVals"
	TblStorageIdx         = "StorageIdx"

	TblCodeKeys        = "CodeKeys"
	TblCodeVals        = "CodeVals"
	TblCodeHistoryKeys = "CodeHistoryKeys"
	TblCodeHistoryVals = "CodeHistoryVals"
	TblCodeIdx         = "CodeIdx"

	TblCommitmentKeys        = "CommitmentKeys"
	TblCommitmentVals        = "CommitmentVals"
	TblCommitmentHistoryKeys = "CommitmentHistoryKeys"
	TblCommitmentHistoryVals = "CommitmentHistoryVals"
	TblCommitmentIdx         = "CommitmentIdx"

	TblLogAddressKeys = "LogAddressKeys"
	TblLogAddressIdx  = "LogAddressIdx"
	TblLogTopicsKeys  = "LogTopicsKeys"
	TblLogTopicsIdx   = "LogTopicsIdx"

	TblTracesFromKeys = "TracesFromKeys"
	TblTracesFromIdx  = "TracesFromIdx"
	TblTracesToKeys   = "TracesToKeys"
	TblTracesToIdx    = "TracesToIdx"

	Snapshots = "Snapshots" // name -> hash

	//State Reconstitution
	RAccountKeys = "RAccountKeys"
	RAccountIdx  = "RAccountIdx"
	RStorageKeys = "RStorageKeys"
	RStorageIdx  = "RStorageIdx"
	RCodeKeys    = "RCodeKeys"
	RCodeIdx     = "RCodeIdx"

	PlainStateR    = "PlainStateR"    // temporary table for PlainState reconstitution
	PlainStateD    = "PlainStateD"    // temporary table for PlainStare reconstitution, deletes
	CodeR          = "CodeR"          // temporary table for Code reconstitution
	CodeD          = "CodeD"          // temporary table for Code reconstitution, deletes
	PlainContractR = "PlainContractR" // temporary table for PlainContract reconstitution
	PlainContractD = "PlainContractD" // temporary table for PlainContract reconstitution, deletes

	// Erigon-CL Objects

	// [slot] => [Beacon state]
	BeaconState = "BeaconState"
	// [slot] => [signature + block without execution payload]
	BeaconBlocks = "BeaconBlock"
	// [slot] => [attestation list (custom encoding)]
	Attestetations = "Attestetations"
	// [slot] => [Finalized block root]
	FinalizedBlockRoots = "FinalizedBlockRoots"
	// [Root (block root/state root/eth1 root)] => Slot
	RootSlotIndex = "RootSlotIndex"

	// LightClientStore => LightClientStore object
	// LightClientFinalityUpdate => latest finality update
	// LightClientOptimisticUpdate => latest optimistic update
	LightClient = "LightClient"
	// Period (one every 27 hours) => LightClientUpdate
	LightClientUpdates = "LightClientUpdates"
)

// Keys
var (
	//StorageModeTEVM - does not translate EVM to TEVM
	StorageModeTEVM = []byte("smTEVM")

	PruneTypeOlder  = []byte("older")
	PruneTypeBefore = []byte("before")

	PruneHistory        = []byte("pruneHistory")
	PruneHistoryType    = []byte("pruneHistoryType")
	PruneReceipts       = []byte("pruneReceipts")
	PruneReceiptsType   = []byte("pruneReceiptsType")
	PruneTxIndex        = []byte("pruneTxIndex")
	PruneTxIndexType    = []byte("pruneTxIndexType")
	PruneCallTraces     = []byte("pruneCallTraces")
	PruneCallTracesType = []byte("pruneCallTracesType")

	DBSchemaVersionKey = []byte("dbVersion")

	BittorrentPeerID            = "peerID"
	CurrentHeadersSnapshotHash  = []byte("CurrentHeadersSnapshotHash")
	CurrentHeadersSnapshotBlock = []byte("CurrentHeadersSnapshotBlock")
	CurrentBodiesSnapshotHash   = []byte("CurrentBodiesSnapshotHash")
	CurrentBodiesSnapshotBlock  = []byte("CurrentBodiesSnapshotBlock")
	PlainStateVersion           = []byte("PlainStateVersion")

	LightClientStore            = []byte("LightClientStore")
	LightClientFinalityUpdate   = []byte("LightClientFinalityUpdate")
	LightClientOptimisticUpdate = []byte("LightClientOptimisticUpdate")
)

// ChaindataTables - list of all buckets. App will panic if some bucket is not in this list.
// This list will be sorted in `init` method.
// ChaindataTablesCfg - can be used to find index in sorted version of ChaindataTables list by name
var ChaindataTables = []string{
	E2AccountsHistory,
	E2StorageHistory,
	Code,
	ContractCode,
	HeaderNumber,
	BadHeaderNumber,
	BlockBody,
	Receipts,
	TxLookup,
	ConfigTable,
	CurrentExecutionPayload,
	DatabaseInfo,
	IncarnationMap,
	ContractTEVMCode,
	CliqueSeparate,
	CliqueLastSnapshot,
	CliqueSnapshot,
	SyncStageProgress,
	PlainState,
	PlainContractCode,
	AccountChangeSet,
	StorageChangeSet,
	Senders,
	HeadBlockKey,
	HeadHeaderKey,
	LastForkchoice,
	Migrations,
	LogTopicIndex,
	LogAddressIndex,
	CallTraceSet,
	CallFromIndex,
	CallToIndex,
	CumulativeGasIndex,
	CumulativeTransactionIndex,
	Log,
	Sequence,
	EthTx,
	NonCanonicalTxs,
	TrieOfAccounts,
	TrieOfStorage,
	HashedAccounts,
	HashedStorage,
	HeaderCanonical,
	Headers,
	HeaderTD,
	Epoch,
	PendingEpoch,
	Issuance,
	StateAccounts,
	StateStorage,
	StateCode,
	StateCommitment,
	BorReceipts,
	BorFinality,
	BorTxLookup,
	BorSeparate,
	BorEvents,
	BorEventNums,
	BorSpans,
	TblAccountKeys,
	TblAccountVals,
	TblAccountHistoryKeys,
	TblAccountHistoryVals,
	TblAccountIdx,

	TblStorageKeys,
	TblStorageVals,
	TblStorageHistoryKeys,
	TblStorageHistoryVals,
	TblStorageIdx,

	TblCodeKeys,
	TblCodeVals,
	TblCodeHistoryKeys,
	TblCodeHistoryVals,
	TblCodeIdx,

	TblCommitmentKeys,
	TblCommitmentVals,
	TblCommitmentHistoryKeys,
	TblCommitmentHistoryVals,
	TblCommitmentIdx,

	TblLogAddressKeys,
	TblLogAddressIdx,
	TblLogTopicsKeys,
	TblLogTopicsIdx,

	TblTracesFromKeys,
	TblTracesFromIdx,
	TblTracesToKeys,
	TblTracesToIdx,

	Snapshots,
	MaxTxNum,

	RAccountKeys,
	RAccountIdx,
	RStorageKeys,
	RStorageIdx,
	RCodeKeys,
	RCodeIdx,

	VerkleRoots,
	VerkleTrie,
	// Beacon stuff
	BeaconState,
	BeaconBlocks,
	FinalizedBlockRoots,
	RootSlotIndex,
	Attestetations,
	LightClient,
	LightClientUpdates,
}

const (
	RecentLocalTransaction = "RecentLocalTransaction" // sequence_u64 -> tx_hash
	PoolTransaction        = "PoolTransaction"        // txHash -> sender_id_u64+tx_rlp
	PoolInfo               = "PoolInfo"               // option_key -> option_value
)

var TxPoolTables = []string{
	RecentLocalTransaction,
	PoolTransaction,
	PoolInfo,
}
var SentryTables = []string{}
var DownloaderTables = []string{
	BittorrentCompletion,
	BittorrentInfo,
}
var ReconTables = []string{
	PlainStateR,
	PlainStateD,
	CodeR,
	CodeD,
	PlainContractR,
	PlainContractD,
}

// ChaindataDeprecatedTables - list of buckets which can be programmatically deleted - for example after migration
var ChaindataDeprecatedTables = []string{
	Clique,
	TransitionBlockKey,
}

type CmpFunc func(k1, k2, v1, v2 []byte) int

type TableCfg map[string]TableCfgItem
type Bucket string

type DBI uint
type TableFlags uint

const (
	Default    TableFlags = 0x00
	ReverseKey TableFlags = 0x02
	DupSort    TableFlags = 0x04
	IntegerKey TableFlags = 0x08
	IntegerDup TableFlags = 0x20
	ReverseDup TableFlags = 0x40
)

type TableCfgItem struct {
	Flags TableFlags
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
	DupFromLen int
	DupToLen   int
}

var ChaindataTablesCfg = TableCfg{
	HashedStorage: {
		Flags:                     DupSort,
		AutoDupSortKeysConversion: true,
		DupFromLen:                72,
		DupToLen:                  40,
	},
	AccountChangeSet: {Flags: DupSort},
	StorageChangeSet: {Flags: DupSort},
	PlainState: {
		Flags:                     DupSort,
		AutoDupSortKeysConversion: true,
		DupFromLen:                60,
		DupToLen:                  28,
	},
	CallTraceSet: {Flags: DupSort},

	TblAccountKeys:           {Flags: DupSort},
	TblAccountHistoryKeys:    {Flags: DupSort},
	TblAccountHistoryVals:    {Flags: DupSort},
	TblAccountIdx:            {Flags: DupSort},
	TblStorageKeys:           {Flags: DupSort},
	TblStorageHistoryKeys:    {Flags: DupSort},
	TblStorageHistoryVals:    {Flags: DupSort},
	TblStorageIdx:            {Flags: DupSort},
	TblCodeKeys:              {Flags: DupSort},
	TblCodeHistoryKeys:       {Flags: DupSort},
	TblCodeIdx:               {Flags: DupSort},
	TblCommitmentKeys:        {Flags: DupSort},
	TblCommitmentHistoryKeys: {Flags: DupSort},
	TblCommitmentIdx:         {Flags: DupSort},
	TblLogAddressKeys:        {Flags: DupSort},
	TblLogAddressIdx:         {Flags: DupSort},
	TblLogTopicsKeys:         {Flags: DupSort},
	TblLogTopicsIdx:          {Flags: DupSort},
	TblTracesFromKeys:        {Flags: DupSort},
	TblTracesFromIdx:         {Flags: DupSort},
	TblTracesToKeys:          {Flags: DupSort},
	TblTracesToIdx:           {Flags: DupSort},
	RAccountKeys:             {Flags: DupSort},
	RAccountIdx:              {Flags: DupSort},
	RStorageKeys:             {Flags: DupSort},
	RStorageIdx:              {Flags: DupSort},
	RCodeKeys:                {Flags: DupSort},
	RCodeIdx:                 {Flags: DupSort},
}

var TxpoolTablesCfg = TableCfg{}
var SentryTablesCfg = TableCfg{}
var DownloaderTablesCfg = TableCfg{}
var ReconTablesCfg = TableCfg{
	PlainStateD:    {Flags: DupSort},
	CodeD:          {Flags: DupSort},
	PlainContractD: {Flags: DupSort},
}

func TablesCfgByLabel(label Label) TableCfg {
	switch label {
	case ChainDB:
		return ChaindataTablesCfg
	case TxPoolDB:
		return TxpoolTablesCfg
	case SentryDB:
		return SentryTablesCfg
	case DownloaderDB:
		return DownloaderTablesCfg
	default:
		panic(fmt.Sprintf("unexpected label: %s", label))
	}
}
func sortBuckets() {
	sort.SliceStable(ChaindataTables, func(i, j int) bool {
		return strings.Compare(ChaindataTables[i], ChaindataTables[j]) < 0
	})
}

func init() {
	reinit()
}

func reinit() {
	sortBuckets()

	for _, name := range ChaindataTables {
		_, ok := ChaindataTablesCfg[name]
		if !ok {
			ChaindataTablesCfg[name] = TableCfgItem{}
		}
	}

	for _, name := range ChaindataDeprecatedTables {
		_, ok := ChaindataTablesCfg[name]
		if !ok {
			ChaindataTablesCfg[name] = TableCfgItem{}
		}
		tmp := ChaindataTablesCfg[name]
		tmp.IsDeprecated = true
		ChaindataTablesCfg[name] = tmp
	}

	for _, name := range TxPoolTables {
		_, ok := TxpoolTablesCfg[name]
		if !ok {
			TxpoolTablesCfg[name] = TableCfgItem{}
		}
	}

	for _, name := range SentryTables {
		_, ok := SentryTablesCfg[name]
		if !ok {
			SentryTablesCfg[name] = TableCfgItem{}
		}
	}

	for _, name := range DownloaderTables {
		_, ok := DownloaderTablesCfg[name]
		if !ok {
			DownloaderTablesCfg[name] = TableCfgItem{}
		}
	}

	for _, name := range ReconTables {
		_, ok := ReconTablesCfg[name]
		if !ok {
			ReconTablesCfg[name] = TableCfgItem{}
		}
	}
}

// Temporal

const (
	AccountsDomain Domain = "AccountsDomain"
	StorageDomain  Domain = "StorageDomain"
	CodeDomain     Domain = "CodeDomain"
)

const (
	AccountsHistory History = "AccountsHistory"
	StorageHistory  History = "StorageHistory"
	CodeHistory     History = "CodeHistory"
)

const (
	AccountsHistoryIdx InvertedIdx = "AccountsHistoryIdx"
	StorageHistoryIdx  InvertedIdx = "StorageHistoryIdx"
	CodeHistoryIdx     InvertedIdx = "CodeHistoryIdx"

	LogTopicIdx   InvertedIdx = "LogTopicIdx"
	LogAddrIdx    InvertedIdx = "LogAddrIdx"
	TracesFromIdx InvertedIdx = "TracesFromIdx"
	TracesToIdx   InvertedIdx = "TracesToIdx"
)

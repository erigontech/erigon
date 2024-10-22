// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package kv

import (
	"fmt"
	"sort"
	"strings"

	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
)

// DBSchemaVersion versions list
// 5.0 - BlockTransaction table now has canonical ids (txs of non-canonical blocks moving to NonCanonicalTransaction table)
// 6.0 - BlockTransaction table now has system-txs before and after block (records are absent if block has no system-tx, but sequence increasing)
// 6.1 - Canonical/NonCanonical/BadBlock transitions now stored in same table: kv.EthTx. Add kv.BadBlockNumber table
var DBSchemaVersion = types.VersionReply{Major: 7, Minor: 0, Patch: 0}

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
const ChangeSets3 = "ChangeSets3"

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
If `db.seekInFiles(A+bigEndian(X))` returns non-last shard -

	then get block number from shard value Y := RoaringBitmap(shard_value).GetGte(X)
	and with Y go to ChangeSets: db.Get(ChangeSets, Y+A)

If `db.seekInFiles(A+bigEndian(X))` returns last shard -

	then we go to PlainState: db.Get(PlainState, A)

Format:
  - index split to shards by 2Kb - RoaringBitmap encoded sorted list of block numbers
    (to avoid performance degradation of popular accounts or look deep into history.
    Also 2Kb allows avoid Overflow pages inside DB.)
  - if shard is not last - then key has suffix 8 bytes = bigEndian(max_block_num_in_this_shard)
  - if shard is last - then key has suffix 8 bytes = 0xFF

It allows:
  - server task 1. by 1 db operation db.seekInFiles(A+bigEndian(X))
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
	EthTx    = "BlockTransaction" // tx_id_u64 -> rlp(tx)
	MaxTxNum = "MaxTxNum"         // block_number_u64 -> max_tx_num_in_block_u64

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

	TxLookup = "BlockTransactionLookup" // hash -> transaction/receipt lookup metadata

	ConfigTable = "Config" // config prefix for the db

	// Progress of sync stages: stageName -> stageData
	SyncStageProgress = "SyncStage"

	CliqueSeparate     = "CliqueSeparate"
	CliqueLastSnapshot = "CliqueLastSnapshot"

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

	// migrationName -> serialized SyncStageProgress and SyncStageUnwind buckets
	// it stores stages progress to understand in which context was executed migration
	// in case of bug-report developer can ask content of this bucket
	Migrations = "Migration"

	Sequence = "Sequence" // tbl_name -> seq_u64

	Epoch        = "DevEpoch"        // block_num_u64+block_hash->transition_proof
	PendingEpoch = "DevPendingEpoch" // block_num_u64+block_hash->transition_proof

	// BOR
	BorFinality             = "BorFinality"
	BorTxLookup             = "BlockBorTransactionLookup" // transaction_hash -> block_num_u64
	BorSeparate             = "BorSeparate"               // persisted snapshots of the Validator Sets, with their proposer priorities
	BorEvents               = "BorEvents"                 // event_id -> event_payload
	BorEventNums            = "BorEventNums"              // block_num -> event_id (last event_id in that block)
	BorEventProcessedBlocks = "BorEventProcessedBlocks"   // block_num -> block_time, tracks processed blocks in the bridge, used for unwinds and restarts, gets pruned
	BorSpans                = "BorSpans"                  // span_id -> span (in JSON encoding)
	BorMilestones           = "BorMilestones"             // milestone_id -> milestone (in JSON encoding)
	BorMilestoneEnds        = "BorMilestoneEnds"          // start block_num -> milestone_id (first block of milestone)
	BorCheckpoints          = "BorCheckpoints"            // checkpoint_id -> checkpoint (in JSON encoding)
	BorCheckpointEnds       = "BorCheckpointEnds"         // start block_num -> checkpoint_id (first block of checkpoint)
	BorProducerSelections   = "BorProducerSelections"     // span_id -> span selection with accumulated proposer priorities (in JSON encoding)

	// Downloader
	BittorrentCompletion = "BittorrentCompletion"
	BittorrentInfo       = "BittorrentInfo"

	// Domains/History/InvertedIndices
	// Contants have "Tbl" prefix, to avoid collision with actual Domain names
	// This constants is very rarely used in APP, but Domain/History/Idx names are widely used
	TblAccountVals        = "AccountVals"
	TblAccountHistoryKeys = "AccountHistoryKeys"
	TblAccountHistoryVals = "AccountHistoryVals"
	TblAccountIdx         = "AccountIdx"

	TblStorageVals        = "StorageVals"
	TblStorageHistoryKeys = "StorageHistoryKeys"
	TblStorageHistoryVals = "StorageHistoryVals"
	TblStorageIdx         = "StorageIdx"

	TblCodeVals        = "CodeVals"
	TblCodeHistoryKeys = "CodeHistoryKeys"
	TblCodeHistoryVals = "CodeHistoryVals"
	TblCodeIdx         = "CodeIdx"

	TblCommitmentVals        = "CommitmentVals"
	TblCommitmentHistoryKeys = "CommitmentHistoryKeys"
	TblCommitmentHistoryVals = "CommitmentHistoryVals"
	TblCommitmentIdx         = "CommitmentIdx"

	TblReceiptVals        = "ReceiptVals"
	TblReceiptHistoryKeys = "ReceiptHistoryKeys"
	TblReceiptHistoryVals = "ReceiptHistoryVals"
	TblReceiptIdx         = "ReceiptIdx"

	TblLogAddressKeys = "LogAddressKeys"
	TblLogAddressIdx  = "LogAddressIdx"
	TblLogTopicsKeys  = "LogTopicsKeys"
	TblLogTopicsIdx   = "LogTopicsIdx"

	TblTracesFromKeys = "TracesFromKeys"
	TblTracesFromIdx  = "TracesFromIdx"
	TblTracesToKeys   = "TracesToKeys"
	TblTracesToIdx    = "TracesToIdx"

	// Prune progress of execution: tableName -> [8bytes of invStep]latest pruned key
	// Could use table constants `Tbl{Account,Storage,Code,Commitment}Keys` for domains
	// corresponding history tables `Tbl{Account,Storage,Code,Commitment}HistoryKeys` for history
	// and `Tbl{Account,Storage,Code,Commitment}Idx` for inverted indices
	TblPruningProgress = "PruningProgress"

	//State Reconstitution
	PlainStateR    = "PlainStateR"    // temporary table for PlainState reconstitution
	PlainStateD    = "PlainStateD"    // temporary table for PlainStare reconstitution, deletes
	CodeR          = "CodeR"          // temporary table for Code reconstitution
	CodeD          = "CodeD"          // temporary table for Code reconstitution, deletes
	PlainContractR = "PlainContractR" // temporary table for PlainContract reconstitution
	PlainContractD = "PlainContractD" // temporary table for PlainContract reconstitution, deletes

	// Erigon-CL Objects

	// [slot] => [signature + block without execution payload]
	BeaconBlocks = "BeaconBlock"

	EffectiveBalancesDump = "EffectiveBalancesDump"
	BalancesDump          = "BalancesDump"

	// [slot] => [Canonical block root]
	CanonicalBlockRoots = "CanonicalBlockRoots"
	// [Root (block root] => Slot
	BlockRootToSlot = "BlockRootToSlot"
	// [Block Root] => [State Root]
	BlockRootToStateRoot = "BlockRootToStateRoot"
	StateRootToBlockRoot = "StateRootToBlockRoot"

	BlockRootToBlockNumber = "BlockRootToBlockNumber"
	BlockRootToBlockHash   = "BlockRootToBlockHash"

	LastBeaconSnapshot    = "LastBeaconSnapshot"
	LastBeaconSnapshotKey = "LastBeaconSnapshotKey"

	BlockRootToKzgCommitments = "BlockRootToKzgCommitments"

	// [Block Root] => [Parent Root]
	BlockRootToParentRoot  = "BlockRootToParentRoot"
	ParentRootToBlockRoots = "ParentRootToBlockRoots"

	HighestFinalized = "HighestFinalized" // hash -> transaction/receipt lookup metadata

	// BlockRoot => Beacon Block Header
	BeaconBlockHeaders = "BeaconBlockHeaders"

	// Beacon historical data
	// ValidatorIndex => [Field]
	ValidatorPublicKeys         = "ValidatorPublickeys"
	InvertedValidatorPublicKeys = "InvertedValidatorPublickeys"
	// ValidatorIndex + Slot => [Field]
	ValidatorEffectiveBalance = "ValidatorEffectiveBalance"
	ValidatorSlashings        = "ValidatorSlashings"
	ValidatorBalance          = "ValidatorBalance"
	StaticValidators          = "StaticValidators"
	StateEvents               = "StateEvents"
	ActiveValidatorIndicies   = "ActiveValidatorIndicies"

	// External data
	StateRoot = "StateRoot"
	BlockRoot = "BlockRoot"
	// Differentiate data stored per-slot vs per-epoch
	SlotData  = "SlotData"
	EpochData = "EpochData"
	// State fields
	InactivityScores     = "InactivityScores"
	NextSyncCommittee    = "NextSyncCommittee"
	CurrentSyncCommittee = "CurrentSyncCommittee"
	HistoricalRoots      = "HistoricalRoots"
	HistoricalSummaries  = "HistoricalSummaries"
	Eth1DataVotes        = "Eth1DataVotes"

	IntraRandaoMixes = "IntraRandaoMixes" // [validator_index+slot] => [randao_mix]
	RandaoMixes      = "RandaoMixes"      // [validator_index+slot] => [randao_mix]
	Proposers        = "BlockProposers"   // epoch => proposers indicies

	StatesProcessingProgress = "StatesProcessingProgress"

	//Diagnostics tables
	DiagSystemInfo = "DiagSystemInfo"
	DiagSyncStages = "DiagSyncStages"
)

// Keys
var (
	PruneTypeOlder = []byte("older")
	PruneHistory   = []byte("pruneHistory")
	PruneBlocks    = []byte("pruneBlocks")

	DBSchemaVersionKey = []byte("dbVersion")
	GenesisKey         = []byte("genesis")

	BittorrentPeerID = "peerID"

	PlainStateVersion = []byte("PlainStateVersion")

	HighestFinalizedKey = []byte("HighestFinalized")
	LastNewBlockSeen    = []byte("LastNewBlockSeen") // last seen block hash

	StatesProcessingKey          = []byte("StatesProcessing")
	MinimumPrunableStepDomainKey = []byte("MinimumPrunableStepDomainKey")
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
	DatabaseInfo,
	IncarnationMap,
	CliqueSeparate,
	CliqueLastSnapshot,
	SyncStageProgress,
	PlainState,
	PlainContractCode,
	AccountChangeSet,
	StorageChangeSet,
	ChangeSets3,
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
	Log,
	Sequence,
	EthTx,
	TrieOfAccounts,
	TrieOfStorage,
	HashedAccounts,
	HashedStorage,
	HeaderCanonical,
	Headers,
	HeaderTD,
	Epoch,
	PendingEpoch,
	BorFinality,
	BorTxLookup,
	BorSeparate,
	BorEvents,
	BorEventNums,
	BorEventProcessedBlocks,
	BorSpans,
	BorMilestones,
	BorMilestoneEnds,
	BorCheckpoints,
	BorCheckpointEnds,
	BorProducerSelections,
	TblAccountVals,
	TblAccountHistoryKeys,
	TblAccountHistoryVals,
	TblAccountIdx,

	TblStorageVals,
	TblStorageHistoryKeys,
	TblStorageHistoryVals,
	TblStorageIdx,

	TblCodeVals,
	TblCodeHistoryKeys,
	TblCodeHistoryVals,
	TblCodeIdx,

	TblCommitmentVals,
	TblCommitmentHistoryKeys,
	TblCommitmentHistoryVals,
	TblCommitmentIdx,

	TblReceiptVals,
	TblReceiptHistoryKeys,
	TblReceiptHistoryVals,
	TblReceiptIdx,

	TblLogAddressKeys,
	TblLogAddressIdx,
	TblLogTopicsKeys,
	TblLogTopicsIdx,

	TblTracesFromKeys,
	TblTracesFromIdx,
	TblTracesToKeys,
	TblTracesToIdx,

	TblPruningProgress,

	MaxTxNum,

	VerkleRoots,
	VerkleTrie,
	// Beacon stuff
	BeaconBlocks,
	CanonicalBlockRoots,
	BlockRootToSlot,
	BlockRootToStateRoot,
	StateRootToBlockRoot,
	BlockRootToParentRoot,
	BeaconBlockHeaders,
	HighestFinalized,
	BlockRootToBlockHash,
	BlockRootToBlockNumber,
	LastBeaconSnapshot,
	ParentRootToBlockRoots,
	// Blob Storage
	BlockRootToKzgCommitments,
	// State Reconstitution
	ValidatorPublicKeys,
	InvertedValidatorPublicKeys,
	ValidatorEffectiveBalance,
	ValidatorBalance,
	ValidatorSlashings,
	StaticValidators,
	StateEvents,
	// Other stuff (related to state reconstitution)
	BlockRoot,
	StateRoot,
	SlotData,
	EpochData,
	RandaoMixes,
	Proposers,
	StatesProcessingProgress,
	InactivityScores,
	NextSyncCommittee,
	CurrentSyncCommittee,
	HistoricalRoots,
	HistoricalSummaries,
	Eth1DataVotes,
	IntraRandaoMixes,
	ActiveValidatorIndicies,
	EffectiveBalancesDump,
	BalancesDump,
}

const (
	RecentLocalTransaction = "RecentLocalTransaction" // sequence_u64 -> tx_hash
	PoolTransaction        = "PoolTransaction"        // txHash -> sender+tx_rlp
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
var ChaindataDeprecatedTables = []string{}

// Diagnostics tables
var DiagnosticsTables = []string{
	DiagSystemInfo,
	DiagSyncStages,
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

	TblAccountVals:           {Flags: DupSort},
	TblAccountHistoryKeys:    {Flags: DupSort},
	TblAccountHistoryVals:    {Flags: DupSort},
	TblAccountIdx:            {Flags: DupSort},
	TblStorageVals:           {Flags: DupSort},
	TblStorageHistoryKeys:    {Flags: DupSort},
	TblStorageHistoryVals:    {Flags: DupSort},
	TblStorageIdx:            {Flags: DupSort},
	TblCodeHistoryKeys:       {Flags: DupSort},
	TblCodeIdx:               {Flags: DupSort},
	TblCommitmentVals:        {Flags: DupSort},
	TblCommitmentHistoryKeys: {Flags: DupSort},
	TblCommitmentHistoryVals: {Flags: DupSort},
	TblCommitmentIdx:         {Flags: DupSort},
	TblReceiptVals:           {Flags: DupSort},
	TblReceiptHistoryKeys:    {Flags: DupSort},
	TblReceiptHistoryVals:    {Flags: DupSort},
	TblReceiptIdx:            {Flags: DupSort},
	TblLogAddressKeys:        {Flags: DupSort},
	TblLogAddressIdx:         {Flags: DupSort},
	TblLogTopicsKeys:         {Flags: DupSort},
	TblLogTopicsIdx:          {Flags: DupSort},
	TblTracesFromKeys:        {Flags: DupSort},
	TblTracesFromIdx:         {Flags: DupSort},
	TblTracesToKeys:          {Flags: DupSort},
	TblTracesToIdx:           {Flags: DupSort},
	TblPruningProgress:       {Flags: DupSort},
}

var BorTablesCfg = TableCfg{
	BorFinality:             {Flags: DupSort},
	BorTxLookup:             {Flags: DupSort},
	BorEvents:               {Flags: DupSort},
	BorEventNums:            {Flags: DupSort},
	BorEventProcessedBlocks: {Flags: DupSort},
	BorSpans:                {Flags: DupSort},
	BorCheckpoints:          {Flags: DupSort},
	BorCheckpointEnds:       {Flags: DupSort},
	BorMilestones:           {Flags: DupSort},
	BorMilestoneEnds:        {Flags: DupSort},
	BorProducerSelections:   {Flags: DupSort},
}

var TxpoolTablesCfg = TableCfg{}
var SentryTablesCfg = TableCfg{}
var DownloaderTablesCfg = TableCfg{}
var DiagnosticsTablesCfg = TableCfg{}
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
	case DiagnosticsDB:
		return DiagnosticsTablesCfg
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

	for _, name := range DiagnosticsTables {
		_, ok := DiagnosticsTablesCfg[name]
		if !ok {
			DiagnosticsTablesCfg[name] = TableCfgItem{}
		}
	}
}

// Temporal

const (
	AccountsDomain   Domain = 0
	StorageDomain    Domain = 1
	CodeDomain       Domain = 2
	CommitmentDomain Domain = 3
	ReceiptDomain    Domain = 4
	DomainLen        Domain = 5
)

const (
	AccountsHistory   History = "AccountsHistory"
	StorageHistory    History = "StorageHistory"
	CodeHistory       History = "CodeHistory"
	CommitmentHistory History = "CommitmentHistory"
	ReceiptHistory    History = "ReceiptHistory"
)

const (
	AccountsHistoryIdx   InvertedIdx = "AccountsHistoryIdx"
	StorageHistoryIdx    InvertedIdx = "StorageHistoryIdx"
	CodeHistoryIdx       InvertedIdx = "CodeHistoryIdx"
	CommitmentHistoryIdx InvertedIdx = "CommitmentHistoryIdx"
	ReceiptHistoryIdx    InvertedIdx = "ReceiptHistoryIdx"

	LogTopicIdx   InvertedIdx = "LogTopicIdx"
	LogAddrIdx    InvertedIdx = "LogAddrIdx"
	TracesFromIdx InvertedIdx = "TracesFromIdx"
	TracesToIdx   InvertedIdx = "TracesToIdx"

	LogAddrIdxPos    InvertedIdxPos = 0
	LogTopicIdxPos   InvertedIdxPos = 1
	TracesFromIdxPos InvertedIdxPos = 2
	TracesToIdxPos   InvertedIdxPos = 3
	StandaloneIdxLen InvertedIdxPos = 4
)

const (
	ReceiptsAppendable Appendable = 0
	AppendableLen      Appendable = 0
)

func (iip InvertedIdxPos) String() string {
	switch iip {
	case LogAddrIdxPos:
		return "logAddr"
	case LogTopicIdxPos:
		return "logTopic"
	case TracesFromIdxPos:
		return "traceFrom"
	case TracesToIdxPos:
		return "traceTo"
	default:
		return "unknown inverted index"
	}
}

func (d Domain) String() string {
	switch d {
	case AccountsDomain:
		return "accounts"
	case StorageDomain:
		return "storage"
	case CodeDomain:
		return "code"
	case CommitmentDomain:
		return "commitment"
	case ReceiptDomain:
		return "receipt"
	default:
		return "unknown domain"
	}
}

func String2Domain(in string) (Domain, error) {
	switch in {
	case "accounts":
		return AccountsDomain, nil
	case "storage":
		return StorageDomain, nil
	case "code":
		return CodeDomain, nil
	case "commitment":
		return CommitmentDomain, nil
	case "receipt":
		return ReceiptDomain, nil
	default:
		return Domain(MaxUint16), fmt.Errorf("unknown history name: %s", in)
	}
}

const MaxUint16 uint16 = 1<<16 - 1

func (iip Appendable) String() string {
	switch iip {
	case ReceiptsAppendable:
		return "receipts"
	default:
		return "unknown Appendable"
	}
}

func String2Appendable(in string) (Appendable, error) {
	switch in {
	case "receipts":
		return ReceiptsAppendable, nil
	default:
		return Appendable(MaxUint16), fmt.Errorf("unknown Appendable name: %s", in)
	}
}

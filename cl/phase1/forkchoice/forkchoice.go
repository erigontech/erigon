// Copyright 2024 The Erigon Authors
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

package forkchoice

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	statelru "github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/optimistic"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"
)

var currentStateCopy = metrics.GetOrCreateSummary("caplin_forkchoice_current_state_copy")

// ForkNode is a struct that represents a node in the fork choice tree.
type ForkNode struct {
	Slot           uint64      `json:"slot,string"`
	BlockRoot      common.Hash `json:"block_root"`
	ParentRoot     common.Hash `json:"parent_root"`
	JustifiedEpoch uint64      `json:"justified_epoch,string"`
	FinalizedEpoch uint64      `json:"finalized_epoch,string"`
	Weight         uint64      `json:"weight,string"`
	Validity       string      `json:"validity"`
	ExecutionBlock common.Hash `json:"execution_block_hash"`
}

const (
	checkpointsPerCache                = 1024
	maxCheckpointStates                = 10
	maxConcurrentTargetCommitteeBuilds = 2
	maxTargetCheckpointBlockAge        = 4
	allowedCachedStates                = 8
	queueCacheSize                     = 128
	pendingELPayloadsShrinkCap         = 256 // drain releases the backing array when cap exceeds this
	maxPendingELPayloads               = 1024
)

type randaoDelta struct {
	epoch uint64
	delta common.Hash
}

type finalityCheckpoints struct {
	finalizedCheckpoint         solid.Checkpoint
	currentJustifiedCheckpoint  solid.Checkpoint
	previousJustifiedCheckpoint solid.Checkpoint
}

type preverifiedAppendListsSizes struct {
	validatorLength           uint64
	historicalRootsLength     uint64
	historicalSummariesLength uint64
}

type ForkChoiceStore struct {
	timeMs           atomic.Uint64
	highestSeen      atomic.Uint64
	highestSeenRoot  atomic.Value // common.Hash
	blocksProcessing atomic.Int64
	// all of *solid.Checkpoint type
	justifiedCheckpoint           atomic.Value
	finalizedCheckpoint           atomic.Value
	unrealizedJustifiedCheckpoint atomic.Value
	unrealizedFinalizedCheckpoint atomic.Value

	proposerBoostRoot                  atomic.Value
	headHash                           common.Hash
	headSlot                           uint64
	headPayloadStatus                  cltypes.PayloadStatus
	headIsFallback                     bool                           // Gloas get_head found no viable node and fell back to the justified root
	selectedHead                       atomic.Pointer[ForkChoiceNode] // the head last published, for lock-free readers; nil after a payload decision changes
	genesisTime                        uint64
	genesisValidatorsRoot              common.Hash
	anchorExecutionPayloadBuilderIndex uint64
	anchorHasExecutionPayloadBid       bool
	weights                            map[common.Hash]uint64
	headSet                            map[common.Hash]struct{}
	hotSidecars                        map[common.Hash][]*cltypes.BlobSidecar // Set of sidecars that are not yet processed.
	verifiedExecutionPayload           *lru.Cache[common.Hash, struct{}]
	verifiedExecutionPayloadHashes     *lru.Cache[common.Hash, common.Hash]
	executionPayloadRoots              map[common.Hash]map[common.Hash]struct{}
	invalidatedExecutionPayloads       *sync.Map
	// Carries an invalid verdict until the caller makes it durable, since the bounded
	// caches below can evict in between. Not refcounted, so the writer clears its own key.
	inFlightInvalidPayloads *sync.Map
	// [New in Gloas:EIP7732] Track execution payload validation status by execution block hash.
	// Used to check if parent execution payload has been validated/invalidated for gossip validation.
	executionPayloadStatus   *lru.Cache[common.Hash, execution_client.PayloadStatus]
	payloadStatusByRoot      *lru.Cache[common.Hash, execution_client.PayloadStatus]
	pendingEnvelopeApplyGate sync.Map // blockRoot -> chan struct{}: admits one apply of a parked envelope per root, closed when it ends
	envelopeAppliesMu        sync.Mutex
	envelopeApplies          map[common.Hash]int // envelope applies past the signature check, by block root
	// [New in Gloas:EIP7732] Track execution payload gas_limit by execution block hash.
	// Used for the is_gas_limit_target_compatible IGNORE check in bid gossip validation.
	executionPayloadGasLimit *lru.Cache[common.Hash, uint64]
	// childrens
	childrens sync.Map

	// Use go map because this is actually an unordered set
	equivocatingIndicies []byte
	forkGraph            fork_graph.ForkGraph
	blobStorage          blob_storage.BlobStorage
	peerDas              das.PeerDas
	// Per-block unrealized checkpoints (spec: store.unrealized_justifications)
	unrealizedJustifications sync.Map // blockRoot -> solid.Checkpoint
	unrealizedFinalizations  sync.Map // blockRoot -> solid.Checkpoint
	// Serializes checkpointStates inserts and evictions with justified promotion,
	// so an eviction pass never deletes the current justified state.
	checkpointStatesMu sync.Mutex
	// I use the cache due to the convenient auto-cleanup feauture.
	checkpointStates   sync.Map // We keep ssz snappy of it as the full beacon state is full of rendundant data.
	publicKeysRegistry public_keys_registry.PublicKeyRegistry
	localValidators    *validator_params.ValidatorParams

	checkpointStateBuilds singleflight.Group
	targetStateBuildsMu   sync.Mutex
	targetStateBuilds     map[solid.Checkpoint]*targetStateBuild

	latestMessages    *latestMessagesStore
	voteTally         voteTally
	syncedDataManager *synced_data.SyncedDataManager
	// We keep track of them so that we can forkchoice with EL.
	eth2Roots *lru.Cache[common.Hash, common.Hash] // ETH2 root -> ETH1 hash
	// preverifid sizes and other data collection
	preverifiedSizes    *lru.Cache[common.Hash, preverifiedAppendListsSizes]
	finalityCheckpoints *lru.Cache[common.Hash, finalityCheckpoints]
	totalActiveBalances *lru.Cache[common.Hash, uint64]
	// Randao mixes
	randaoMixesLists *lru.Cache[common.Hash, solid.HashListSSZ] // limited randao mixes full list (only 16 elements)
	randaoDeltas     *lru.Cache[common.Hash, randaoDelta]       // small entry can be lots of elements.
	// participation tracking
	participation *lru.Cache[uint64, *solid.ParticipationBitList] // epoch -> [participation]

	// consolidations/pending deposits/withdrawals
	pendingConsolidations *lru.Cache[common.Hash, *solid.ListSSZ[*solid.PendingConsolidation]]
	pendingDeposits       *lru.Cache[common.Hash, *solid.ListSSZ[*solid.PendingDeposit]]
	partialWithdrawals    *lru.Cache[common.Hash, *solid.ListSSZ[*solid.PendingPartialWithdrawal]]

	proposerLookahead *lru.Cache[uint64, solid.Uint64VectorSSZ]

	mu                             sync.RWMutex
	payloadPreparation             PayloadPreparationImports
	executionPayloadValidationOnce sync.Once
	executionPayloadValidation     chan struct{}

	// EL
	engine execution_client.ExecutionEngine

	// operations pool
	operationsPool pool.OperationsPool
	beaconCfg      *clparams.BeaconChainConfig

	emitters *beaconevents.EventEmitter
	// event sends queued under f.mu, emitted after release (see queueEmit)
	queuedEmits             []func()
	queuedPrunes            []uint64
	queuedOperationPrunes   []solid.Checkpoint
	operationPruneMu        sync.Mutex
	operationPruneTarget    solid.Checkpoint
	operationPrunePending   bool
	operationPruneRunning   bool
	synced                  atomic.Bool
	ethClock                eth_clock.EthereumClock
	optimisticStore         optimistic.OptimisticStore
	probabilisticHeadGetter bool

	// [New in Gloas:EIP7732]
	ptcVoteMu                    sync.Mutex // protects live payload vote updates, paired reads, and first-valid gossip tracking
	payloadTimelinessVote        sync.Map   // map[common.Hash][clparams.PtcSize]int8 (0=unvoted, 1=true, -1=false)
	payloadDataAvailabilityVote  sync.Map   // map[common.Hash][clparams.PtcSize]int8 (0=unvoted, 1=true, -1=false)
	payloadAttestationSeen       map[payloadAttestationSeenKey]struct{}
	payloadAttestationPrunedSlot uint64
	payloadAttestationContexts   *payloadAttestationValidationContexts
	// [New in Gloas:EIP7732] Block timeliness tracking.
	// Pre-GLOAS: stores [block_timely, false] (only index 0 is meaningful).
	// Post-GLOAS: stores [block_timely, payload_timely] — two independent booleans.
	// Used by is_head_late and proposer boost reorg logic.
	blockTimeliness sync.Map // map[common.Hash][clparams.NumBlockTimelinessDeadlines]bool
	// [New in Gloas:EIP7732]
	gloasWeightTree *gloasWeightTree
	// [New in Gloas:EIP7732] Envelopes waiting for their corresponding block to arrive.
	// In GLOAS, BeaconBlock and ExecutionPayloadEnvelope are gossiped separately.
	// Due to network timing, the envelope may arrive before its corresponding block.
	// When this happens, OnExecutionPayload queues the envelope here (keyed by beacon_block_root).
	// Later, when OnBlock processes the block, it checks this cache and processes any pending envelope.
	pendingEnvelopes         *lru.Cache[common.Hash, *cltypes.SignedExecutionPayloadEnvelope]
	envelopeGossipAdmissions ExecutionPayloadEnvelopeAdmissions
	// Earliest time a retry of a pending envelope may run again, by block root. The background retry and
	// a proposal's retry of its parent share it.
	dataAvailableEnvelopeRetryAt *lru.Cache[common.Hash, time.Time]
	dataAvailableEnvelopeRetryMu sync.Mutex // makes the due check and the booking of a retry one step

	// [New in Gloas:EIP7732] Locally-produced self-build envelopes waiting for their block.
	// Separate from pendingEnvelopes so that OnBlock replay can distinguish local origin
	// (skip BLS) from gossip origin (full verification) without inspecting envelope contents.
	pendingLocalSelfBuildEnvelopes *lru.Cache[common.Hash, *cltypes.SignedExecutionPayloadEnvelope]
	parentBuilderExitsOnce         sync.Once
	parentBuilderExits             *lru.Cache[common.Hash, []solid.BuilderExitRequest]

	// [New in Gloas:EIP7732] Execution blocks whose CL state transition succeeded but
	// whose EL newPayload failed (e.g. because EL hasn't caught up after forward sync).
	// The stages layer drains these into blockCollector before each Flush() so EL
	// eventually receives the blocks.
	pendingELPayloadsMu         sync.Mutex
	pendingELPayloads           []PendingELPayload
	payloadValidationOnce       sync.Once
	payloadValidationAdmission  chan struct{}
	envelopeIndexWrites         sync.Map
	envelopeIndexRepairs        envelopeIndexRepairTracker
	executionPayloadFirstSeenMu sync.Mutex
	executionPayloadFirstSeen   map[common.Hash]executionPayloadArrival
	pendingEnvelopeArrivalOnce  sync.Once
	pendingEnvelopeArrivalMu    sync.Mutex
	pendingEnvelopeArrival      *statelru.Cache[pendingEnvelopeArrivalKey, time.Time]

	// db is used to persist execution payload indices (block number/hash) when an envelope
	// is accepted in OnExecutionPayload. May be nil (e.g. in tests), in which case the
	// index writes are skipped.
	db kv.RwDB
}

type envelopeIndexWrite struct {
	done     chan struct{}
	err      error
	envelope *cltypes.SignedExecutionPayloadEnvelope
}

type pendingEnvelopeArrivalKey struct {
	root     common.Hash
	envelope *cltypes.SignedExecutionPayloadEnvelope
}

type executionPayloadArrival struct {
	receivedAt time.Time
	slot       uint64
}

// PendingELPayload holds a block+envelope pair that needs to be fed to the EL.
type PendingELPayload struct {
	Block    *cltypes.SignedBeaconBlock
	Envelope *cltypes.SignedExecutionPayloadEnvelope
}

type childrens struct {
	childrenHashes []common.Hash
	parentSlot     uint64 // we keep this one for pruning
}

// NewForkChoiceStore initialize a new store from the given anchor state, either genesis or checkpoint sync state.
func NewForkChoiceStore(
	ethClock eth_clock.EthereumClock,
	anchorState *state.CachingBeaconState,
	engine execution_client.ExecutionEngine,
	operationsPool pool.OperationsPool,
	forkGraph fork_graph.ForkGraph,
	emitters *beaconevents.EventEmitter,
	syncedDataManager *synced_data.SyncedDataManager,
	blobStorage blob_storage.BlobStorage,
	publicKeysRegistry public_keys_registry.PublicKeyRegistry,
	localValidators *validator_params.ValidatorParams,
	probabilisticHeadGetter bool,
	db kv.RwDB,
) (*ForkChoiceStore, error) {
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		return nil, err
	}

	slot := anchorState.Slot()
	anchorEpoch := state.Epoch(anchorState.BeaconState)
	if slot%anchorState.BeaconConfig().SlotsPerEpoch != 0 && anchorState.LatestBlockHeader().Slot == slot {
		// A checkpoint block that is not at an epoch start is also the checkpoint at the next
		// epoch start. A state advanced past its block keeps the epoch of its own slot.
		anchorEpoch++
	}
	anchorCheckpoint := solid.Checkpoint{
		Root:  anchorRoot,
		Epoch: anchorEpoch,
	}

	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](65536)
	if err != nil {
		return nil, err
	}
	executionPayloadRoots := make(map[common.Hash]map[common.Hash]struct{})
	invalidatedExecutionPayloads := &sync.Map{}
	verifiedExecutionPayloadHashes, err := lru.NewWithEvict[common.Hash, common.Hash](65536, func(blockRoot, executionBlockHash common.Hash) {
		verifiedExecutionPayload.Remove(blockRoot)
		roots := executionPayloadRoots[executionBlockHash]
		delete(roots, blockRoot)
		if len(roots) == 0 {
			delete(executionPayloadRoots, executionBlockHash)
			invalidatedExecutionPayloads.Delete(executionBlockHash)
		}
	})
	if err != nil {
		return nil, err
	}

	eth2Roots, err := lru.New[common.Hash, common.Hash](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	randaoMixesLists, err := lru.New[common.Hash, solid.HashListSSZ](allowedCachedStates)
	if err != nil {
		return nil, err
	}

	randaoDeltas, err := lru.New[common.Hash, randaoDelta](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	finalityCheckpoints, err := lru.New[common.Hash, finalityCheckpoints](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	preverifiedSizes, err := lru.New[common.Hash, preverifiedAppendListsSizes](checkpointsPerCache * 10)
	if err != nil {
		return nil, err
	}
	preverifiedSizes.Add(anchorRoot, preverifiedAppendListsSizes{
		validatorLength:           uint64(anchorState.ValidatorLength()),
		historicalRootsLength:     anchorState.HistoricalRootsLength(),
		historicalSummariesLength: anchorState.HistoricalSummariesLength(),
	})

	totalActiveBalances, err := lru.New[common.Hash, uint64](checkpointsPerCache * 10)
	if err != nil {
		return nil, err
	}

	participation, err := lru.New[uint64, *solid.ParticipationBitList](16)
	if err != nil {
		return nil, err
	}

	partialWithdrawals, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingPartialWithdrawal]](queueCacheSize)
	if err != nil {
		return nil, err
	}

	pendingConsolidations, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingConsolidation]](queueCacheSize)
	if err != nil {
		return nil, err
	}

	pendingDeposits, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingDeposit]](queueCacheSize)
	if err != nil {
		return nil, err
	}
	proposerLookahead, err := lru.New[uint64, solid.Uint64VectorSSZ](queueCacheSize)
	if err != nil {
		return nil, err
	}

	// [New in Gloas:EIP7732] LRU cache for pending envelopes waiting for their block
	pendingEnvelopes, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	if err != nil {
		return nil, err
	}

	dataAvailableEnvelopeRetryAt, err := lru.New[common.Hash, time.Time](queueCacheSize)
	if err != nil {
		return nil, err
	}

	// [New in Gloas:EIP7732] Separate queue for locally-produced self-build envelopes
	pendingLocalSelfBuildEnvelopes, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	if err != nil {
		return nil, err
	}

	// [New in Gloas:EIP7732] Track execution payload validation status by execution block hash
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](checkpointsPerCache)
	if err != nil {
		return nil, err
	}
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	// [New in Gloas:EIP7732] Track execution payload gas_limit by execution block hash
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](checkpointsPerCache)
	if err != nil {
		return nil, err
	}
	payloadAttestationContexts, err := newPayloadAttestationValidationContexts()
	if err != nil {
		return nil, err
	}

	publicKeysRegistry.ResetAnchor(anchorState)
	participation.Add(state.Epoch(anchorState.BeaconState), anchorState.CurrentEpochParticipation().Copy())

	// The fork graph's current state is a rescanned anchor copy, so seeded copies must match it.
	anchorTotalActiveBalance := anchorState.GetTotalActiveBalance()
	if forkGraph != nil {
		if current := forkGraph.CurrentStateLocked(anchorRoot); current != nil {
			anchorTotalActiveBalance = current.GetTotalActiveBalance()
		}
	}
	totalActiveBalances.Add(anchorRoot, anchorTotalActiveBalance)
	r := solid.NewHashVector(int(anchorState.BeaconConfig().EpochsPerHistoricalVector))
	anchorState.RandaoMixes().CopyTo(r)
	randaoMixesLists.Add(anchorRoot, r)
	// Seed the eth2Root→eth1Hash mapping for the anchor block so that
	// fork choice can resolve the EL genesis hash at startup.
	var anchorExecutionHash common.Hash
	if anchorState.Version().AfterOrEqual(clparams.GloasVersion) {
		anchorExecutionHash = anchorState.GetLatestBlockHash()
	} else if header := anchorState.LatestExecutionPayloadHeader(); header != nil {
		anchorExecutionHash = header.BlockHash
	}
	if anchorExecutionHash != (common.Hash{}) {
		eth2Roots.Add(anchorRoot, anchorExecutionHash)
		// Also map the zero hash → EL genesis for the finalized checkpoint
		// which starts as zero at genesis.
		eth2Roots.Add(common.Hash{}, anchorExecutionHash)
	}

	headSet := make(map[common.Hash]struct{})
	headSet[anchorRoot] = struct{}{}
	f := &ForkChoiceStore{
		forkGraph:                      forkGraph,
		equivocatingIndicies:           make([]byte, anchorState.ValidatorLength(), anchorState.ValidatorLength()*2),
		latestMessages:                 newLatestMessagesStore(anchorState.ValidatorLength()),
		eth2Roots:                      eth2Roots,
		engine:                         engine,
		operationsPool:                 operationsPool,
		beaconCfg:                      anchorState.BeaconConfig(),
		preverifiedSizes:               preverifiedSizes,
		finalityCheckpoints:            finalityCheckpoints,
		totalActiveBalances:            totalActiveBalances,
		randaoMixesLists:               randaoMixesLists,
		randaoDeltas:                   randaoDeltas,
		headSet:                        headSet,
		weights:                        make(map[common.Hash]uint64),
		participation:                  participation,
		emitters:                       emitters,
		genesisTime:                    anchorState.GenesisTime(),
		syncedDataManager:              syncedDataManager,
		genesisValidatorsRoot:          anchorState.GenesisValidatorsRoot(),
		hotSidecars:                    make(map[common.Hash][]*cltypes.BlobSidecar),
		blobStorage:                    blobStorage,
		ethClock:                       ethClock,
		optimisticStore:                optimistic.NewOptimisticStore(),
		probabilisticHeadGetter:        probabilisticHeadGetter,
		publicKeysRegistry:             publicKeysRegistry,
		verifiedExecutionPayload:       verifiedExecutionPayload,
		verifiedExecutionPayloadHashes: verifiedExecutionPayloadHashes,
		executionPayloadRoots:          executionPayloadRoots,
		invalidatedExecutionPayloads:   invalidatedExecutionPayloads,
		inFlightInvalidPayloads:        &sync.Map{},
		localValidators:                localValidators,
		pendingConsolidations:          pendingConsolidations,
		pendingDeposits:                pendingDeposits,
		partialWithdrawals:             partialWithdrawals,
		proposerLookahead:              proposerLookahead,
		pendingEnvelopes:               pendingEnvelopes,
		dataAvailableEnvelopeRetryAt:   dataAvailableEnvelopeRetryAt,
		pendingLocalSelfBuildEnvelopes: pendingLocalSelfBuildEnvelopes,
		executionPayloadStatus:         executionPayloadStatus,
		payloadStatusByRoot:            payloadStatusByRoot,
		executionPayloadGasLimit:       executionPayloadGasLimit,
		payloadAttestationContexts:     payloadAttestationContexts,
		db:                             db,
	}
	if anchorState.Version() >= clparams.GloasVersion {
		if bid := anchorState.GetLatestExecutionPayloadBid(); bid != nil {
			f.anchorExecutionPayloadBuilderIndex = bid.BuilderIndex
			f.anchorHasExecutionPayloadBid = true
		}
	}
	f.justifiedCheckpoint.Store(anchorCheckpoint)
	f.finalizedCheckpoint.Store(anchorCheckpoint)
	f.unrealizedFinalizedCheckpoint.Store(anchorCheckpoint)
	f.unrealizedJustifiedCheckpoint.Store(anchorCheckpoint)
	// Store anchor root in per-block unrealized maps so filter_block_tree
	// lookups succeed for the anchor block without falling through to the fork graph.
	f.unrealizedJustifications.Store(common.Hash(anchorRoot), anchorCheckpoint)
	f.unrealizedFinalizations.Store(common.Hash(anchorRoot), anchorCheckpoint)
	f.proposerBoostRoot.Store(common.Hash{})

	f.highestSeen.Store(anchorState.Slot())
	f.highestSeenRoot.Store(common.Hash(anchorRoot))
	f.timeMs.Store(utils.SecondsToMilliseconds(anchorState.GenesisTime() + anchorState.BeaconConfig().SecondsPerSlot*anchorState.Slot()))

	var anchorTimelinessVotes [clparams.PtcSize]int8
	var anchorDataAvailabilityVotes [clparams.PtcSize]int8
	f.payloadTimelinessVote.Store(common.Hash(anchorRoot), anchorTimelinessVotes)
	f.payloadDataAvailabilityVote.Store(common.Hash(anchorRoot), anchorDataAvailabilityVotes)

	f.gloasWeightTree = newGloasWeightTree(f)

	return f, nil
}

func (f *ForkChoiceStore) InitPeerDas(peerDas das.PeerDas) {
	// this is a hack to inject the peer das
	f.peerDas = peerDas
}

func (f *ForkChoiceStore) GetPeerDas() das.PeerDas {
	return f.peerDas
}

// GetRecentExecutionPayloadStatus returns the validation status of a recently validated execution payload
// by its execution block hash. This is an LRU cache lookup; older payloads may not be found.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) GetRecentExecutionPayloadStatus(executionBlockHash common.Hash) (execution_client.PayloadStatus, bool) {
	return f.executionPayloadStatus.Get(executionBlockHash)
}

func (f *ForkChoiceStore) GetRecentExecutionPayloadStatusByRoot(blockRoot common.Hash) (execution_client.PayloadStatus, bool) {
	if f.payloadExecutionHashInvalidated(blockRoot) {
		return execution_client.PayloadStatusInvalidated, true
	}
	return f.payloadStatusAuthority(blockRoot)
}

func (f *ForkChoiceStore) GetCachedParentBuilderExitRequests(blockRoot common.Hash) ([]solid.BuilderExitRequest, bool) {
	f.initParentBuilderExitRequests()
	requests, ok := f.parentBuilderExits.Get(blockRoot)
	return slices.Clone(requests), ok
}

func (f *ForkChoiceStore) cacheParentBuilderExitRequests(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) {
	if envelope == nil || envelope.Message == nil || envelope.Message.ExecutionRequests == nil {
		return
	}
	exits := envelope.Message.ExecutionRequests.BuilderExits
	count := 0
	if exits != nil {
		count = exits.Len()
	}
	requests := make([]solid.BuilderExitRequest, count)
	for i := range count {
		request := exits.Get(i)
		if request == nil {
			return
		}
		requests[i] = *request
	}
	f.initParentBuilderExitRequests()
	f.parentBuilderExits.Add(blockRoot, requests)
}

func (f *ForkChoiceStore) initParentBuilderExitRequests() {
	f.parentBuilderExitsOnce.Do(func() {
		var err error
		f.parentBuilderExits, err = lru.New[common.Hash, []solid.BuilderExitRequest](checkpointsPerCache)
		if err != nil {
			panic(err)
		}
	})
}

// GetExecutionPayloadGasLimit returns the gas_limit of a recently validated execution payload.
func (f *ForkChoiceStore) GetExecutionPayloadGasLimit(executionBlockHash common.Hash) (uint64, bool) {
	return f.executionPayloadGasLimit.Get(executionBlockHash)
}

// IsBlobDataAvailable returns the local node's assessment of blob data availability
// for a given block. Used by the payload_attestation_data beacon API so PTC validators
// can independently determine the blob_data_available flag.
// Returns true when: (a) the envelope exists and the committed bid has zero blob
// commitments (trivially available), or (b) PeerDAS confirms custody columns are present.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) IsBlobDataAvailable(slot uint64, blockRoot common.Hash) bool {
	// Envelope must exist locally
	if !f.forkGraph.HasEnvelope(blockRoot) {
		return false
	}

	// Get the block to inspect its committed bid's blob commitments
	block, ok := f.forkGraph.GetBlock(blockRoot)
	if !ok || block == nil {
		return false
	}

	committedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if committedBid == nil || committedBid.Message == nil {
		// No bid → no blobs → trivially available
		return true
	}

	if committedBid.Message.BlobKzgCommitments.Len() == 0 {
		// No blob commitments → trivially available
		return true
	}

	// Has blob commitments — check PeerDAS
	if f.peerDas == nil {
		return false
	}
	available, err := f.peerDas.IsDataAvailable(slot, blockRoot)
	if err != nil {
		return false
	}
	return available
}

// Highest seen returns highest seen slot
func (f *ForkChoiceStore) HighestSeen() uint64 {
	return f.highestSeen.Load()
}

// HasImportedDescendantAfter reports whether a block imported on top of root, on a branch that
// passes get_head's justification and finalization filter and that an invalidation did not cut
// off, is past slot. A correct head has no such descendant, since get_head walks down to a leaf,
// so one means the head is stuck. When root is the selected head, its children count only on a
// payload variant get_head could have walked to. Viability is judged on blocks, not on get_head's
// payload nodes: a block whose children all sit on one variant and are all non-viable is dropped
// even when its other variant is a viable leaf. That needs a long non-finality period and errs
// toward serving duties.
func (f *ForkChoiceStore) HasImportedDescendantAfter(root common.Hash, slot uint64) bool {
	var pending []common.Hash
	for _, child := range f.children(root) {
		if !f.cutOffByInvalidation(child) && f.reachableFromSelectedHead(root, child) {
			pending = append(pending, child)
		}
	}
	if len(pending) == 0 {
		return false
	}
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	viable := make(map[common.Hash]*cltypes.BeaconBlockHeader)
	for _, child := range pending {
		f.markViableUncutBlocks(child, viable, currentEpoch, justified)
	}
	for len(pending) > 0 {
		block := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		header, ok := viable[block]
		if !ok {
			continue
		}
		if header.Slot > slot {
			return true
		}
		pending = append(pending, f.children(block)...)
	}
	return false
}

// markViableUncutBlocks is getFilterBlockTree without the children an invalidation cut off: a
// block whose other children are all cut off is judged as a leaf, as get_head judges its EMPTY
// node.
func (f *ForkChoiceStore) markViableUncutBlocks(blockRoot common.Hash, blocks map[common.Hash]*cltypes.BeaconBlockHeader, currentEpoch uint64, justified solid.Checkpoint) bool {
	header, has := f.forkGraph.GetHeader(blockRoot)
	if !has {
		return false
	}
	anyChild, anyViable := false, false
	for _, child := range f.children(blockRoot) {
		if f.cutOffByInvalidation(child) {
			continue
		}
		anyChild = true
		if f.markViableUncutBlocks(child, blocks, currentEpoch, justified) {
			anyViable = true
		}
	}
	if anyViable || (!anyChild && f.isViableLeaf(blockRoot, header, currentEpoch, justified)) {
		blocks[blockRoot] = header
		return true
	}
	return false
}

// BlockProcessing reports whether an OnBlock call is waiting for the store lock or is active.
// Blocks parked between network-service retries have not entered OnBlock and are not counted.
func (f *ForkChoiceStore) BlockProcessing() bool {
	return f.blocksProcessing.Load() > 0
}

// HighestSeenRoot returns the block root of the highest seen slot.
func (f *ForkChoiceStore) HighestSeenRoot() common.Hash {
	if v := f.highestSeenRoot.Load(); v != nil {
		return v.(common.Hash)
	}
	return common.Hash{}
}

func (f *ForkChoiceStore) children(parent common.Hash) []common.Hash {
	children, ok := f.childrens.Load(parent)
	if !ok {
		return nil
	}
	return children.(childrens).childrenHashes
}

// updateChildren adds a new child to the parent node hash.
func (f *ForkChoiceStore) updateChildren(parentSlot uint64, parent, child common.Hash) {
	cI, ok := f.childrens.Load(parent)
	var c childrens
	if ok {
		c = cI.(childrens)
	}
	c.parentSlot = parentSlot // can be inaccurate.
	if slices.Contains(c.childrenHashes, child) {
		return
	}
	c.childrenHashes = append(c.childrenHashes, child)
	f.childrens.Store(parent, c)
}

// Time returns current time
func (f *ForkChoiceStore) Time() uint64 {
	return f.timeMs.Load() / 1000
}

func (f *ForkChoiceStore) GenesisTime() uint64 {
	return f.genesisTime
}

// ProposerBoostRoot returns proposer boost root
func (f *ForkChoiceStore) ProposerBoostRoot() common.Hash {
	return f.proposerBoostRoot.Load().(common.Hash)
}

// JustifiedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) JustifiedCheckpoint() solid.Checkpoint {
	return f.justifiedCheckpoint.Load().(solid.Checkpoint)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) JustifiedSlot() uint64 {
	return f.computeStartSlotAtEpoch(f.justifiedCheckpoint.Load().(solid.Checkpoint).Epoch)
}

// getUnrealizedJustification returns the per-block unrealized justified checkpoint
// (spec: store.unrealized_justifications[block_root])
func (f *ForkChoiceStore) getUnrealizedJustification(blockRoot common.Hash) (solid.Checkpoint, bool) {
	obj, ok := f.unrealizedJustifications.Load(blockRoot)
	if !ok {
		return solid.Checkpoint{}, false
	}
	return obj.(solid.Checkpoint), true
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedCheckpoint() solid.Checkpoint {
	return f.finalizedCheckpoint.Load().(solid.Checkpoint)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedSlot() uint64 {
	return f.computeStartSlotAtEpoch(f.finalizedCheckpoint.Load().(solid.Checkpoint).Epoch) + (f.beaconCfg.SlotsPerEpoch - 1)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) Engine() execution_client.ExecutionEngine {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.engine
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) GetEth1Hash(eth2Root common.Hash) common.Hash {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ret, _ := f.eth2Roots.Get(eth2Root)
	return ret
}

// GetFinalizedExecutionHash returns the EL block hash for a finalized/justified checkpoint.
// For Gloas+ blocks, uses parent_block_hash from the bid instead of the payload header hash.
func (f *ForkChoiceStore) GetFinalizedExecutionHash(eth2Root common.Hash) common.Hash {
	f.mu.RLock()
	defer f.mu.RUnlock()
	block, ok := f.forkGraph.GetBlock(eth2Root)
	if ok && block != nil && block.Block.Body.Version >= clparams.GloasVersion {
		if bid := block.Block.Body.GetSignedExecutionPayloadBid(); bid != nil && bid.Message != nil {
			return bid.Message.ParentBlockHash
		}
	}
	ret, _ := f.eth2Roots.Get(eth2Root)
	return ret
}

// AnchorSlot returns the slot of the anchor state.
func (f *ForkChoiceStore) AnchorSlot() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.forkGraph.AnchorSlot()
}

// AnchorRoot returns the block root of the anchor state. [New in Gloas:EIP7732]
func (f *ForkChoiceStore) AnchorRoot() common.Hash {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.forkGraph.AnchorRoot()
}

func (f *ForkChoiceStore) AnchorExecutionPayloadBuilderIndex() (uint64, bool) {
	return f.anchorExecutionPayloadBuilderIndex, f.anchorHasExecutionPayloadBid
}

func (f *ForkChoiceStore) copyCurrentStateLocked(blockRoot common.Hash) (*state.CachingBeaconState, bool, error) {
	current := f.forkGraph.CurrentStateLocked(blockRoot)
	if current == nil {
		return nil, false, nil
	}
	start := time.Now()
	defer currentStateCopy.ObserveDuration(start)
	if f.totalActiveBalances != nil {
		if total, ok := f.totalActiveBalances.Get(blockRoot); ok {
			copied, err := current.CopyWithTotalActiveBalance(total)
			return copied, true, err
		}
	}
	copied, err := current.Copy()
	return copied, true, err
}

func (f *ForkChoiceStore) copyCurrentState(blockRoot common.Hash) (*state.CachingBeaconState, bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.copyCurrentStateLocked(blockRoot)
}

func (f *ForkChoiceStore) copyStateAtBlockRootLocked(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	copied, current, err := f.copyCurrentStateLocked(blockRoot)
	if current || err != nil {
		return copied, err
	}
	return f.forkGraph.ReplayState(blockRoot)
}

func (f *ForkChoiceStore) getStateLocked(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	if current := f.forkGraph.CurrentStateLocked(blockRoot); current != nil {
		return current, nil
	}
	return f.forkGraph.ReplayState(blockRoot)
}

func (f *ForkChoiceStore) GetStateAtBlockRoot(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	copied, current, err := f.copyCurrentState(blockRoot)
	if current || err != nil {
		return copied, err
	}
	return f.forkGraph.ReplayState(blockRoot)
}

// GetOwnedStateAtBlockRoot is GetStateAtBlockRoot, but it also copies a replayed
// state, because replay leaves pointers to stored block objects in it.
func (f *ForkChoiceStore) GetOwnedStateAtBlockRoot(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	copied, current, err := f.copyCurrentState(blockRoot)
	if current || err != nil {
		return copied, err
	}
	replayed, err := f.forkGraph.ReplayState(blockRoot)
	if replayed == nil || err != nil {
		return replayed, err
	}
	return replayed.Copy()
}

func (f *ForkChoiceStore) ViewStateAtBlockRoot(blockRoot common.Hash, fn func(*state.CachingBeaconState) error) error {
	f.mu.RLock()
	if current := f.forkGraph.CurrentStateLocked(blockRoot); current != nil {
		defer f.mu.RUnlock()
		return fn(current)
	}
	f.mu.RUnlock()
	blockState, err := f.forkGraph.ReplayState(blockRoot)
	if err != nil {
		return err
	}
	if blockState == nil {
		return fmt.Errorf("block state not found for root %v", blockRoot)
	}
	return fn(blockState)
}

func (f *ForkChoiceStore) PreverifiedValidator(blockRoot common.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.validatorLength
	}
	return 0
}

func (f *ForkChoiceStore) PreverifiedHistoricalRoots(blockRoot common.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.historicalRootsLength
	}
	return 0
}

func (f *ForkChoiceStore) PreverifiedHistoricalSummaries(blockRoot common.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.historicalSummariesLength
	}
	return 0
}

func (f *ForkChoiceStore) GetFinalityCheckpoints(blockRoot common.Hash) (solid.Checkpoint, solid.Checkpoint, solid.Checkpoint, bool) {
	if ret, ok := f.finalityCheckpoints.Get(blockRoot); ok {
		return ret.finalizedCheckpoint, ret.currentJustifiedCheckpoint, ret.previousJustifiedCheckpoint, true
	}
	return solid.Checkpoint{}, solid.Checkpoint{}, solid.Checkpoint{}, false
}

func (f *ForkChoiceStore) GetSyncCommittees(period uint64) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	return f.forkGraph.GetSyncCommittees(period)
}

func (f *ForkChoiceStore) BlockRewards(root common.Hash) (*eth2.BlockRewardsCollector, bool) {
	return f.forkGraph.GetBlockRewards(root)
}

func (f *ForkChoiceStore) TotalActiveBalance(root common.Hash) (uint64, bool) {
	return f.totalActiveBalances.Get(root)
}

func (f *ForkChoiceStore) LowestAvailableSlot() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.forkGraph.LowestAvailableSlot()
}

func (f *ForkChoiceStore) RandaoMixes(blockRoot common.Hash, out solid.HashListSSZ) bool {
	relevantDeltas := map[uint64]randaoDelta{}
	currentBlockRoot := blockRoot
	var currentSlot uint64
	for {
		h, ok := f.forkGraph.GetHeader(currentBlockRoot)
		if !ok {
			return false
		}
		currentSlot = h.Slot
		if f.randaoMixesLists.Contains(currentBlockRoot) {
			break
		}
		randaoDelta, ok := f.randaoDeltas.Get(currentBlockRoot)
		if !ok {
			return false
		}
		currentBlockRoot = h.ParentRoot
		if _, ok := relevantDeltas[currentSlot/f.beaconCfg.SlotsPerEpoch]; !ok {
			relevantDeltas[currentSlot/f.beaconCfg.SlotsPerEpoch] = randaoDelta
		}
	}
	randaoMixes, ok := f.randaoMixesLists.Get(currentBlockRoot)
	if !ok {
		return false
	}
	randaoMixes.CopyTo(out)
	for epoch, delta := range relevantDeltas {
		out.Set(int(epoch%f.beaconCfg.EpochsPerHistoricalVector), delta.delta)
	}
	return true
}

func (f *ForkChoiceStore) Participation(epoch uint64) (*solid.ParticipationBitList, bool) {
	return f.participation.Get(epoch)
}

func (f *ForkChoiceStore) ForkNodes() []ForkNode {
	trackGloasWeights := f.trackGloasWeights()
	if trackGloasWeights {
		_, _, _ = f.GetHeadNode()
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	weights := f.weights
	if trackGloasWeights {
		weights = f.gloasWeightTree.snapshotWeights()
	}
	forkNodes := make([]ForkNode, 0, len(weights))
	for blockRoot, weight := range weights {
		header, has := f.forkGraph.GetHeader(blockRoot)
		if !has {
			continue
		}
		justifiedCheckpoint, hasJustified := f.forkGraph.GetCurrentJustifiedCheckpoint(blockRoot)
		finalizedCheckpoint, hasFinalized := f.forkGraph.GetFinalizedCheckpoint(blockRoot)
		if !hasJustified || !hasFinalized {
			if blockRoot != f.forkGraph.AnchorRoot() || blockRoot != f.JustifiedCheckpoint().Root {
				continue
			}
			justifiedCheckpoint, finalizedCheckpoint = f.JustifiedCheckpoint(), f.FinalizedCheckpoint()
		}
		blockHash, _ := f.eth2Roots.Get(blockRoot)

		forkNodes = append(forkNodes, ForkNode{
			Weight:         weight,
			BlockRoot:      blockRoot,
			ParentRoot:     header.ParentRoot,
			JustifiedEpoch: justifiedCheckpoint.Epoch,
			FinalizedEpoch: finalizedCheckpoint.Epoch,
			Slot:           header.Slot,
			Validity:       "valid",
			ExecutionBlock: blockHash,
		})
	}
	slices.SortFunc(forkNodes, func(a, b ForkNode) int {
		return cmp.Compare(a.Slot, b.Slot)
	})
	return forkNodes
}

func (f *ForkChoiceStore) Synced() bool {
	return f.synced.Load()
}

func (f *ForkChoiceStore) SetSynced(s bool) {
	f.synced.Store(s)
}

func (f *ForkChoiceStore) GetLightClientBootstrap(blockRoot common.Hash) (*cltypes.LightClientBootstrap, bool) {
	return f.forkGraph.GetLightClientBootstrap(blockRoot)
}

func (f *ForkChoiceStore) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	return f.forkGraph.NewestLightClientUpdate()
}

func (f *ForkChoiceStore) GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool) {
	return f.forkGraph.GetLightClientUpdate(period)
}

func (f *ForkChoiceStore) GetHeader(blockRoot common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	return f.forkGraph.GetHeader(blockRoot)
}

func (f *ForkChoiceStore) GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return f.forkGraph.GetBlock(blockRoot)
}

func (f *ForkChoiceStore) HasBlockChildAtOrAfter(blockRoot common.Hash, slot uint64) bool {
	return f.forkGraph.HasBlockChildAtOrAfter(blockRoot, slot)
}

func (f *ForkChoiceStore) HasBlockEquivocation(slot, proposerIndex uint64, exceptRoot common.Hash) bool {
	return f.forkGraph.HasBlockEquivocation(slot, proposerIndex, exceptRoot)
}

// HasEnvelope delegates to forkGraph.HasEnvelope.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) HasEnvelope(blockRoot common.Hash) bool {
	return f.forkGraph.HasEnvelope(blockRoot)
}

// IsPayloadVerified returns whether the execution payload for the beacon block root
// has been accepted by the execution layer.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) IsPayloadVerified(blockRoot common.Hash) bool {
	if f.forkGraph == nil {
		return false
	}
	verified, accepted := f.forkGraph.PayloadAccepted(blockRoot)
	if !accepted || !verified {
		return false
	}
	return f.forkGraph.HasEnvelope(blockRoot) && !f.payloadExecutionHashInvalidated(blockRoot)
}

func (f *ForkChoiceStore) payloadExecutionHashInvalidated(blockRoot common.Hash) bool {
	var executionBlockHash common.Hash
	var ok bool
	if f.verifiedExecutionPayloadHashes != nil {
		executionBlockHash, ok = f.verifiedExecutionPayloadHashes.Get(blockRoot)
	}
	if !ok && f.eth2Roots != nil {
		executionBlockHash, ok = f.eth2Roots.Get(blockRoot)
	}
	if !ok {
		return false
	}
	if f.invalidatedExecutionPayloads != nil {
		if _, invalidated := f.invalidatedExecutionPayloads.Load(executionBlockHash); invalidated {
			return true
		}
	}
	if f.executionPayloadStatus == nil {
		return false
	}
	status, ok := f.executionPayloadStatus.Get(executionBlockHash)
	return ok && status == execution_client.PayloadStatusInvalidated
}

func (f *ForkChoiceStore) trackExecutionPayloadRootLocked(blockRoot, executionBlockHash common.Hash) {
	if f.executionPayloadRoots == nil {
		f.executionPayloadRoots = make(map[common.Hash]map[common.Hash]struct{})
	}
	if f.verifiedExecutionPayloadHashes != nil {
		if previousHash, ok := f.verifiedExecutionPayloadHashes.Get(blockRoot); ok && previousHash != executionBlockHash {
			previousRoots := f.executionPayloadRoots[previousHash]
			delete(previousRoots, blockRoot)
			if len(previousRoots) == 0 {
				delete(f.executionPayloadRoots, previousHash)
				if f.invalidatedExecutionPayloads != nil {
					f.invalidatedExecutionPayloads.Delete(previousHash)
				}
			}
		}
	}
	roots := f.executionPayloadRoots[executionBlockHash]
	if roots == nil {
		roots = make(map[common.Hash]struct{})
		f.executionPayloadRoots[executionBlockHash] = roots
	}
	roots[blockRoot] = struct{}{}
	if f.verifiedExecutionPayloadHashes != nil {
		f.verifiedExecutionPayloadHashes.Add(blockRoot, executionBlockHash)
	}
	if f.eth2Roots != nil {
		f.eth2Roots.Add(blockRoot, executionBlockHash)
	}
}

func (f *ForkChoiceStore) MarkPayloadVerified(blockRoot common.Hash, executionBlockHash common.Hash) {
	f.MarkPayloadStatus(blockRoot, executionBlockHash, execution_client.PayloadStatusValidated)
}

func (f *ForkChoiceStore) MarkPayloadStatus(blockRoot common.Hash, executionBlockHash common.Hash, status execution_client.PayloadStatus) execution_client.PayloadStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.markPayloadStatusLocked(blockRoot, executionBlockHash, status)
}

type retainedBlockGuard interface {
	WithRetainedBlock(common.Hash, func(func(common.Hash) bool)) bool
	IsBlockRetained(common.Hash) bool
}

func (f *ForkChoiceStore) MarkPayloadStatusAndGasLimitIfRetained(
	blockRoot common.Hash,
	executionBlockHash common.Hash,
	status execution_client.PayloadStatus,
	gasLimit uint64,
) (execution_client.PayloadStatus, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.markPayloadStatusIfRetainedLocked(blockRoot, executionBlockHash, status, &gasLimit)
}

func (f *ForkChoiceStore) markPayloadStatusIfRetainedLocked(
	blockRoot common.Hash,
	executionBlockHash common.Hash,
	status execution_client.PayloadStatus,
	gasLimit *uint64,
) (execution_client.PayloadStatus, bool) {
	guard, ok := f.forkGraph.(retainedBlockGuard)
	if !ok {
		effective := f.markPayloadStatusLocked(blockRoot, executionBlockHash, status)
		if gasLimit != nil {
			f.cacheExecutionPayloadGasLimit(executionBlockHash, *gasLimit)
		}
		return effective, true
	}
	effective := status
	retained := guard.WithRetainedBlock(blockRoot, func(isRetained func(common.Hash) bool) {
		effective = f.markPayloadStatus(blockRoot, executionBlockHash, status, isRetained)
		if gasLimit != nil {
			f.cacheExecutionPayloadGasLimit(executionBlockHash, *gasLimit)
		}
	})
	return effective, retained
}

func (f *ForkChoiceStore) cacheExecutionPayloadGasLimit(executionBlockHash common.Hash, gasLimit uint64) {
	if f.executionPayloadGasLimit != nil {
		f.executionPayloadGasLimit.Add(executionBlockHash, gasLimit)
	}
}

func (f *ForkChoiceStore) MarkPayloadInvalid(blockRoot common.Hash, executionBlockHash common.Hash) {
	f.MarkPayloadStatus(blockRoot, executionBlockHash, execution_client.PayloadStatusInvalidated)
}

func (f *ForkChoiceStore) markPayloadStatusLocked(blockRoot common.Hash, executionBlockHash common.Hash, status execution_client.PayloadStatus) execution_client.PayloadStatus {
	return f.markPayloadStatus(blockRoot, executionBlockHash, status, nil)
}

func (f *ForkChoiceStore) markPayloadStatus(
	blockRoot common.Hash,
	executionBlockHash common.Hash,
	status execution_client.PayloadStatus,
	isRetained func(common.Hash) bool,
) execution_client.PayloadStatus {
	f.trackExecutionPayloadRootLocked(blockRoot, executionBlockHash)
	effective := status
	if f.invalidatedExecutionPayloads != nil {
		if _, invalidated := f.invalidatedExecutionPayloads.Load(executionBlockHash); invalidated {
			effective = execution_client.PayloadStatusInvalidated
		}
	}
	if f.executionPayloadStatus != nil {
		if executionStatus, ok := f.executionPayloadStatus.Get(executionBlockHash); ok && executionStatus == execution_client.PayloadStatusInvalidated {
			effective = execution_client.PayloadStatusInvalidated
		}
	}
	current, known := f.payloadStatusAuthorityWithRetention(blockRoot, isRetained != nil)
	if known {
		switch current {
		case execution_client.PayloadStatusInvalidated:
			effective = execution_client.PayloadStatusInvalidated
		case execution_client.PayloadStatusValidated:
			if effective != execution_client.PayloadStatusInvalidated {
				effective = execution_client.PayloadStatusValidated
			}
		case execution_client.PayloadStatusNotValidated:
			if effective == execution_client.PayloadStatusNone {
				effective = execution_client.PayloadStatusNotValidated
			}
		}
	}
	if effective == execution_client.PayloadStatusInvalidated {
		if f.invalidatedExecutionPayloads == nil {
			f.invalidatedExecutionPayloads = &sync.Map{}
		}
		f.invalidatedExecutionPayloads.Store(executionBlockHash, struct{}{})
		for root := range f.executionPayloadRoots[executionBlockHash] {
			if root == blockRoot {
				continue
			}
			invalidate := func() {
				if f.verifiedExecutionPayload != nil {
					f.verifiedExecutionPayload.Remove(root)
				}
				if f.payloadStatusByRoot != nil {
					f.payloadStatusByRoot.Add(root, execution_client.PayloadStatusInvalidated)
				}
				if f.forkGraph != nil {
					f.forkGraph.MarkPayloadAvailable(root)
					f.forkGraph.ClearPayloadAccepted(root)
					f.forkGraph.MarkHeaderAsInvalid(root)
				}
			}
			if isRetained != nil {
				if isRetained(root) {
					invalidate()
				}
			} else if guard, ok := f.forkGraph.(retainedBlockGuard); ok {
				guard.WithRetainedBlock(root, func(func(common.Hash) bool) { invalidate() })
			} else {
				invalidate()
			}
		}
	}
	if f.verifiedExecutionPayload != nil {
		if effective == execution_client.PayloadStatusValidated {
			f.verifiedExecutionPayload.Add(blockRoot, struct{}{})
		} else {
			f.verifiedExecutionPayload.Remove(blockRoot)
		}
	}
	if f.eth2Roots != nil {
		f.eth2Roots.Add(blockRoot, executionBlockHash)
	}
	if f.verifiedExecutionPayloadHashes != nil {
		f.verifiedExecutionPayloadHashes.Add(blockRoot, executionBlockHash)
	}
	if f.executionPayloadStatus != nil {
		f.executionPayloadStatus.Add(executionBlockHash, effective)
	}
	if f.payloadStatusByRoot != nil {
		f.payloadStatusByRoot.Add(blockRoot, effective)
	}
	if f.forkGraph != nil {
		switch effective {
		case execution_client.PayloadStatusNone:
			f.forkGraph.ClearPayloadAccepted(blockRoot)
			f.forkGraph.MarkPayloadUnavailable(blockRoot)
		case execution_client.PayloadStatusNotValidated:
			f.forkGraph.MarkPayloadAvailable(blockRoot)
			f.forkGraph.MarkPayloadAccepted(blockRoot, false)
		case execution_client.PayloadStatusValidated:
			f.forkGraph.MarkPayloadAvailable(blockRoot)
			f.forkGraph.MarkPayloadAccepted(blockRoot, true)
		case execution_client.PayloadStatusInvalidated:
			f.forkGraph.MarkPayloadAvailable(blockRoot)
			f.forkGraph.ClearPayloadAccepted(blockRoot)
			f.forkGraph.MarkHeaderAsInvalid(blockRoot)
		}
	}
	if !known || current != effective {
		f.headHash = common.Hash{}
		f.headPayloadStatus = cltypes.PayloadStatusPending
		// The decision can rule out the published variant; get_head publishes again.
		f.selectedHead.Store(nil)
	}
	return effective
}

// reachableFromSelectedHead reports whether get_head could reach child from root when root is
// the selected head: a child on the other payload variant counts only while that variant is not
// a candidate, that is, an unavailable FULL. Without a published head every child counts.
func (f *ForkChoiceStore) reachableFromSelectedHead(root, child common.Hash) bool {
	head := f.selectedHead.Load()
	if head == nil || head.Root != root || (head.PayloadStatus != cltypes.PayloadStatusEmpty && head.PayloadStatus != cltypes.PayloadStatusFull) {
		return true
	}
	block, ok := f.forkGraph.GetBlock(child)
	if !ok || block == nil || block.Block == nil || block.Version() < clparams.GloasVersion {
		return true
	}
	variant := f.getParentPayloadStatus(block.Block)
	return variant == head.PayloadStatus || (variant == cltypes.PayloadStatusFull && !f.isPayloadAvailable(root))
}

// cutOffByInvalidation reports whether no fork-choice node of root survives the recorded
// invalidations: an invalid pre-Gloas block, a child of one (it has no EMPTY alternative), or a
// Gloas block built on an invalid parent payload.
func (f *ForkChoiceStore) cutOffByInvalidation(root common.Hash) bool {
	block, ok := f.forkGraph.GetBlock(root)
	if !ok || block == nil || block.Block == nil {
		return false
	}
	if block.Version() < clparams.GloasVersion {
		return f.forkGraph.IsBlockInvalid(root)
	}
	parentRoot := block.Block.ParentRoot
	if !f.forkGraph.IsBlockInvalid(parentRoot) {
		return false
	}
	if parent, ok := f.forkGraph.GetBlock(parentRoot); ok && parent != nil && parent.Version() < clparams.GloasVersion {
		return true
	}
	return f.getParentPayloadStatus(block.Block) == cltypes.PayloadStatusFull
}

func (f *ForkChoiceStore) payloadStatusAuthority(blockRoot common.Hash) (execution_client.PayloadStatus, bool) {
	return f.payloadStatusAuthorityWithRetention(blockRoot, false)
}

func (f *ForkChoiceStore) payloadStatusAuthorityWithRetention(blockRoot common.Hash, retained bool) (execution_client.PayloadStatus, bool) {
	if f.forkGraph != nil {
		if guard, ok := f.forkGraph.(retainedBlockGuard); ok && !retained && !guard.IsBlockRetained(blockRoot) {
			return execution_client.PayloadStatusNone, false
		}
		if f.forkGraph.IsBlockInvalid(blockRoot) {
			return execution_client.PayloadStatusInvalidated, true
		}
		if verified, accepted := f.forkGraph.PayloadAccepted(blockRoot); accepted {
			if verified {
				return execution_client.PayloadStatusValidated, true
			}
			return execution_client.PayloadStatusNotValidated, true
		}
		if f.forkGraph.IsPayloadUnavailable(blockRoot) {
			return execution_client.PayloadStatusNone, true
		}
	}
	if f.payloadStatusByRoot != nil {
		return f.payloadStatusByRoot.Get(blockRoot)
	}
	return execution_client.PayloadStatusNone, false
}

// ReadEnvelopeFromDisk delegates to forkGraph.ReadEnvelopeFromDisk.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) ReadEnvelopeFromDisk(blockRoot common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	return f.forkGraph.ReadEnvelopeFromDisk(blockRoot)
}

func (f *ForkChoiceStore) existingStateAtBlockRoot(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	st, err := f.GetStateAtBlockRoot(blockRoot)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, fork_graph.ErrStateNotFound
	}
	return st, nil
}

func (f *ForkChoiceStore) GetBalances(blockRoot common.Hash) (solid.Uint64ListSSZ, error) {
	st, err := f.existingStateAtBlockRoot(blockRoot)
	if err != nil {
		return nil, err
	}
	return st.Balances(), nil
}

func (f *ForkChoiceStore) GetInactivitiesScores(blockRoot common.Hash) (solid.Uint64ListSSZ, error) {
	st, err := f.existingStateAtBlockRoot(blockRoot)
	if err != nil {
		return nil, err
	}
	return st.InactivityScores(), nil
}

func (f *ForkChoiceStore) GetPreviousParticipationIndicies(blockRoot common.Hash) (*solid.ParticipationBitList, error) {
	header, ok := f.GetHeader(blockRoot)
	if !ok {
		return nil, nil
	}
	return f.forkGraph.GetPreviousParticipationIndicies(header.Slot / f.beaconCfg.SlotsPerEpoch)
}

func (f *ForkChoiceStore) GetValidatorSet(blockRoot common.Hash) (*solid.ValidatorSet, error) {
	st, err := f.existingStateAtBlockRoot(blockRoot)
	if err != nil {
		return nil, err
	}
	return st.ValidatorSet(), nil
}

func (f *ForkChoiceStore) GetCurrentParticipationIndicies(blockRoot common.Hash) (*solid.ParticipationBitList, error) {
	header, ok := f.GetHeader(blockRoot)
	if !ok {
		return nil, nil
	}
	return f.forkGraph.GetCurrentParticipationIndicies(header.Slot / f.beaconCfg.SlotsPerEpoch)
}

func (f *ForkChoiceStore) IsRootOptimistic(root common.Hash) bool {
	return f.optimisticStore.IsOptimistic(root)
}

func (f *ForkChoiceStore) IsHeadOptimistic() bool {
	if f.ethClock.GetCurrentEpoch() < f.beaconCfg.BellatrixForkEpoch {
		return false
	}

	return f.optimisticStore.IsOptimistic(f.syncedDataManager.HeadRoot())
}

func (f *ForkChoiceStore) DumpBeaconStateOnDisk(bs *state.CachingBeaconState) error {
	anchorRoot, err := bs.BlockRoot()
	if err != nil {
		return err
	}
	return f.forkGraph.DumpBeaconStateOnDisk(anchorRoot, bs, false)
}

func (f *ForkChoiceStore) addPendingConsolidations(blockRoot common.Hash, pendingConsolidations *solid.ListSSZ[*solid.PendingConsolidation]) error {
	// first check if we already have the same list in the parent node.
	header, ok := f.forkGraph.GetHeader(blockRoot)
	// If there is no header, make a copy.
	if !ok {
		pendingConsolidationsCopy := solid.NewPendingConsolidationList(f.beaconCfg)
		for i := 0; i < pendingConsolidations.Len(); i++ {
			pendingConsolidationsCopy.Append(pendingConsolidations.Get(i))
		}
		f.pendingConsolidations.Add(blockRoot, pendingConsolidationsCopy)
		return nil
	}
	parentRoot := header.ParentRoot
	parentConsolidations, ok := f.pendingConsolidations.Get(parentRoot)
	if !ok {
		pendingConsolidationsCopy := solid.NewPendingConsolidationList(f.beaconCfg)
		for i := 0; i < pendingConsolidations.Len(); i++ {
			pendingConsolidationsCopy.Append(pendingConsolidations.Get(i))
		}
		f.pendingConsolidations.Add(blockRoot, pendingConsolidationsCopy)
		return nil
	}

	// check if the two lists are equal via their hashes.
	pendingConsolidationsHash, err := pendingConsolidations.HashSSZ()
	if err != nil {
		return err
	}
	parentConsolidationsHash, err := parentConsolidations.HashSSZ()
	if err != nil {
		return err
	}
	if pendingConsolidationsHash == parentConsolidationsHash {
		// If they are equal, we can just store the parent consolidations.
		f.pendingConsolidations.Add(blockRoot, parentConsolidations)
		return nil
	}
	pendingConsolidationsCopy := solid.NewPendingConsolidationList(f.beaconCfg)
	for i := 0; i < pendingConsolidations.Len(); i++ {
		pendingConsolidationsCopy.Append(pendingConsolidations.Get(i))
	}
	f.pendingConsolidations.Add(blockRoot, pendingConsolidationsCopy)
	return nil
}

func (f *ForkChoiceStore) addPendingDeposits(blockRoot common.Hash, pendingDeposits *solid.ListSSZ[*solid.PendingDeposit]) error {
	// first check if we already have the same list in the parent node.
	header, ok := f.forkGraph.GetHeader(blockRoot)
	// If there is no header, make a copy.
	if !ok {
		pendingDepositsCopy := solid.NewPendingDepositList(f.beaconCfg)
		for i := 0; i < pendingDeposits.Len(); i++ {
			pendingDepositsCopy.Append(pendingDeposits.Get(i))
		}
		f.pendingDeposits.Add(blockRoot, pendingDepositsCopy)
		return nil
	}
	parentRoot := header.ParentRoot
	parentDeposits, ok := f.pendingDeposits.Get(parentRoot)
	if !ok {
		pendingDepositsCopy := solid.NewPendingDepositList(f.beaconCfg)
		for i := 0; i < pendingDeposits.Len(); i++ {
			pendingDepositsCopy.Append(pendingDeposits.Get(i))
		}
		f.pendingDeposits.Add(blockRoot, pendingDepositsCopy)
		return nil
	}

	// check if the two lists are equal via their hashes.
	pendingDepositsHash, err := pendingDeposits.HashSSZ()
	if err != nil {
		return err
	}
	parentDepositsHash, err := parentDeposits.HashSSZ()
	if err != nil {
		return err
	}
	if pendingDepositsHash == parentDepositsHash {
		// If they are equal, we can just store the parent deposits.
		f.pendingDeposits.Add(blockRoot, parentDeposits)
		return nil
	}
	pendingDepositsCopy := solid.NewPendingDepositList(f.beaconCfg)
	for i := 0; i < pendingDeposits.Len(); i++ {
		pendingDepositsCopy.Append(pendingDeposits.Get(i))
	}
	f.pendingDeposits.Add(blockRoot, pendingDepositsCopy)
	return nil
}

func (f *ForkChoiceStore) addPendingPartialWithdrawals(blockRoot common.Hash, pendingPartialWithdrawals *solid.ListSSZ[*solid.PendingPartialWithdrawal]) error {
	// first check if we already have the same list in the parent node.
	header, ok := f.forkGraph.GetHeader(blockRoot)
	// If there is no header, make a copy.
	if !ok {
		pendingPartialWithdrawalsCopy := solid.NewPendingWithdrawalList(f.beaconCfg)
		for i := 0; i < pendingPartialWithdrawals.Len(); i++ {
			pendingPartialWithdrawalsCopy.Append(pendingPartialWithdrawals.Get(i))
		}
		f.partialWithdrawals.Add(blockRoot, pendingPartialWithdrawalsCopy)
		return nil
	}
	parentRoot := header.ParentRoot
	parentWithdrawals, ok := f.partialWithdrawals.Get(parentRoot)
	if !ok {
		pendingPartialWithdrawalsCopy := solid.NewPendingWithdrawalList(f.beaconCfg)
		for i := 0; i < pendingPartialWithdrawals.Len(); i++ {
			pendingPartialWithdrawalsCopy.Append(pendingPartialWithdrawals.Get(i))
		}
		f.partialWithdrawals.Add(blockRoot, pendingPartialWithdrawalsCopy)
		return nil
	}

	// check if the two lists are equal via their hashes.
	pendingPartialWithdrawalsHash, err := pendingPartialWithdrawals.HashSSZ()
	if err != nil {
		return err
	}
	parentWithdrawalsHash, err := parentWithdrawals.HashSSZ()
	if err != nil {
		return err
	}
	if pendingPartialWithdrawalsHash == parentWithdrawalsHash {
		// If they are equal, we can just store the parent withdrawals.
		f.partialWithdrawals.Add(blockRoot, parentWithdrawals)
		return nil
	}
	pendingPartialWithdrawalsCopy := solid.NewPendingWithdrawalList(f.beaconCfg)
	for i := 0; i < pendingPartialWithdrawals.Len(); i++ {
		pendingPartialWithdrawalsCopy.Append(pendingPartialWithdrawals.Get(i))
	}
	f.partialWithdrawals.Add(blockRoot, pendingPartialWithdrawalsCopy)
	return nil
}

func (f *ForkChoiceStore) addProposerLookahead(slot uint64, proposerLookahead solid.Uint64VectorSSZ) error {
	epoch := slot / f.beaconCfg.SlotsPerEpoch
	if _, ok := f.proposerLookahead.Get(epoch); !ok {
		pl := solid.NewUint64VectorSSZ(proposerLookahead.Length())
		proposerLookahead.CopyTo(pl)
		f.proposerLookahead.Add(epoch, pl)
	}
	return nil
}

func (f *ForkChoiceStore) GetPendingConsolidations(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingConsolidation], bool) {
	return f.pendingConsolidations.Get(blockRoot)
}

func (f *ForkChoiceStore) GetPendingDeposits(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingDeposit], bool) {
	return f.pendingDeposits.Get(blockRoot)
}

func (f *ForkChoiceStore) GetPendingPartialWithdrawals(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingPartialWithdrawal], bool) {
	return f.partialWithdrawals.Get(blockRoot)
}

func (f *ForkChoiceStore) GetProposerLookahead(slot uint64) (solid.Uint64VectorSSZ, bool) {
	if f.proposerLookahead == nil {
		return nil, false
	}
	epoch := slot / f.beaconCfg.SlotsPerEpoch
	return f.proposerLookahead.Get(epoch)
}

// addPendingELPayload queues an execution block whose CL transition succeeded
// but whose EL newPayload failed (EL behind).  Thread-safe.
func (f *ForkChoiceStore) addPendingELPayload(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) {
	f.pendingELPayloadsMu.Lock()
	defer f.pendingELPayloadsMu.Unlock()
	root, ok := pendingELPayloadRoot(PendingELPayload{Block: block, Envelope: envelope})
	if ok {
		for _, p := range f.pendingELPayloads {
			if existingRoot, existingOk := pendingELPayloadRoot(p); existingOk && existingRoot == root {
				return
			}
		}
	}
	if len(f.pendingELPayloads) >= maxPendingELPayloads {
		log.Warn("addPendingELPayload: dropping oldest pending EL payload", "queueLen", len(f.pendingELPayloads))
		copy(f.pendingELPayloads, f.pendingELPayloads[1:])
		f.pendingELPayloads[len(f.pendingELPayloads)-1] = PendingELPayload{}
		f.pendingELPayloads = f.pendingELPayloads[:len(f.pendingELPayloads)-1]
	}
	f.pendingELPayloads = append(f.pendingELPayloads, PendingELPayload{
		Block:    block,
		Envelope: envelope,
	})
}

func pendingELPayloadRoot(p PendingELPayload) (common.Hash, bool) {
	if p.Envelope != nil && p.Envelope.Message != nil && p.Envelope.Message.BeaconBlockRoot != (common.Hash{}) {
		return p.Envelope.Message.BeaconBlockRoot, true
	}
	return common.Hash{}, false
}

// RequeuePendingELPayload queues a drained execution payload for another EL validation attempt.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) RequeuePendingELPayload(p PendingELPayload) {
	root, ok := pendingELPayloadRoot(p)
	if !ok {
		return
	}
	if guard, guarded := f.forkGraph.(retainedBlockGuard); guarded {
		guard.WithRetainedBlock(root, func(func(common.Hash) bool) { f.addPendingELPayload(p.Block, p.Envelope) })
		return
	}
	f.addPendingELPayload(p.Block, p.Envelope)
}

// DrainPendingELPayloads returns and clears all queued EL payloads.
// The stages layer calls this before Flush() to retry them with engine.NewPayload.
func (f *ForkChoiceStore) DrainPendingELPayloads() []PendingELPayload {
	return f.DrainPendingELPayloadsLimit(maxPendingELPayloads)
}

func (f *ForkChoiceStore) DrainPendingELPayloadsLimit(limit int) []PendingELPayload {
	if limit <= 0 {
		return nil
	}
	f.pendingELPayloadsMu.Lock()
	defer f.pendingELPayloadsMu.Unlock()
	if len(f.pendingELPayloads) == 0 {
		return nil
	}
	if len(f.pendingELPayloads) > limit {
		result := make([]PendingELPayload, limit)
		copy(result, f.pendingELPayloads[:limit])
		copy(f.pendingELPayloads, f.pendingELPayloads[limit:])
		clear(f.pendingELPayloads[len(f.pendingELPayloads)-limit:])
		f.pendingELPayloads = f.pendingELPayloads[:len(f.pendingELPayloads)-limit]
		return result
	}
	if cap(f.pendingELPayloads) > pendingELPayloadsShrinkCap {
		result := f.pendingELPayloads
		f.pendingELPayloads = nil
		return result
	}
	result := make([]PendingELPayload, len(f.pendingELPayloads))
	copy(result, f.pendingELPayloads)
	clear(f.pendingELPayloads)
	f.pendingELPayloads = f.pendingELPayloads[:0]
	return result
}

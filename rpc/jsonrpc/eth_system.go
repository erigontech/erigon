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

package jsonrpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/p2p/forkid"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// deleteStrategyWindow is the only currently defined deleteStrategy type in the
// execution-apis spec: a sliding window of RetentionBlocks blocks.
const deleteStrategyWindow = "window"

// DeleteStrategy describes how a node removes old data for a category.
// Currently only the "window" type is defined: the node keeps a sliding
// window of RetentionBlocks blocks and discards everything older.
// The field is omitted when data is kept indefinitely (archive nodes, or
// KeepPostMergeBlocksPruneMode which uses chain-specific history expiry).
type DeleteStrategy struct {
	Type            string         `json:"type"`
	RetentionBlocks hexutil.Uint64 `json:"retentionBlocks"`
}

// CapabilityField describes availability of a data category: when Disabled is true the node
// does not hold that data at all; otherwise OldestBlock is the lowest block number available.
// DeleteStrategy is set when the node uses a finite retention window.
type CapabilityField struct {
	Disabled       bool            `json:"disabled"`
	OldestBlock    *hexutil.Uint64 `json:"oldestBlock,omitempty"`
	DeleteStrategy *DeleteStrategy `json:"deleteStrategy,omitempty"`
}

// CapabilityHead identifies the canonical chain tip at the moment eth_capabilities was called.
type CapabilityHead struct {
	Number hexutil.Uint64 `json:"number"`
	Hash   common.Hash    `json:"hash"`
}

// CapabilitiesResult is the response type of eth_capabilities.
type CapabilitiesResult struct {
	Head        CapabilityHead  `json:"head"`
	State       CapabilityField `json:"state"`
	Tx          CapabilityField `json:"tx"`
	Logs        CapabilityField `json:"logs"`
	Receipts    CapabilityField `json:"receipts"`
	Blocks      CapabilityField `json:"blocks"`
	StateProofs CapabilityField `json:"stateproofs"`
}

// stricterRetention returns the policy that bounds availability more and
// widerRetention the one that bounds it less. The oldest blocks alone cannot rank
// them: a window that has not started pruning reports zero like keep-all, so a tie
// there is resolved on the retention the policies will apply.
func stricterRetention(oldestA uint64, a prune.BlockAmount, oldestB uint64, b prune.BlockAmount) (uint64, prune.BlockAmount) {
	if oldestA > oldestB || (oldestA == oldestB && retentionBlocks(a) <= retentionBlocks(b)) {
		return oldestA, a
	}
	return oldestB, b
}

func widerRetention(oldestA uint64, a prune.BlockAmount, oldestB uint64, b prune.BlockAmount) (uint64, prune.BlockAmount) {
	if oldestA < oldestB || (oldestA == oldestB && retentionBlocks(a) >= retentionBlocks(b)) {
		return oldestA, a
	}
	return oldestB, b
}

// retentionBlocks measures a policy by the window it keeps, every sentinel counting
// as unbounded.
func retentionBlocks(amount prune.BlockAmount) uint64 {
	if d, ok := amount.(prune.Distance); ok && d.Enabled() {
		return uint64(d)
	}
	return math.MaxUint64
}

// Capabilities implements eth_capabilities.
// stateproofs is only available when --prune.include-commitment-history was set at node startup;
// otherwise it is disabled regardless of prune mode.
func (api *APIImpl) Capabilities(ctx context.Context) (*CapabilitiesResult, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	pruneMode, err := api.pruneMode(tx)
	if err != nil {
		return nil, err
	}

	keepExecutionProofs, err := api.commitmentHistoryEnabled(tx)
	if err != nil {
		return nil, err
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	overlayTx := api.filters.WithOverlay(tx)
	headBlock, err := rpchelper.GetLatestBlockNumber(overlayTx)
	if err != nil {
		return nil, err
	}
	headHash, ok, err := api._blockReader.CanonicalHash(ctx, overlayTx, headBlock)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("canonical hash not found %d", headBlock)
	}

	// OldestBlock reports effective availability, while RetentionBlocks reports
	// the configured deletion policy. They can differ while a wider window refills.
	avail := func(oldest uint64, dist prune.BlockAmount) CapabilityField {
		o := hexutil.Uint64(oldest)
		f := CapabilityField{OldestBlock: &o}
		if d, ok := dist.(prune.Distance); ok && d.Enabled() {
			f.DeleteStrategy = &DeleteStrategy{Type: deleteStrategyWindow, RetentionBlocks: hexutil.Uint64(d)}
		}
		return f
	}

	stateOldest := pruneMode.History.PruneTo(headBlock)
	blocksOldest, err := api.blocksAvailableFrom(ctx, tx, headBlock)
	if err != nil {
		return nil, err
	}
	onDiskOldest, err := api.stateHistoryStartBlock(ctx, tx, headBlock)
	if err != nil {
		return nil, err
	}
	stateOldest = max(stateOldest, onDiskOldest)

	var stateproofs CapabilityField
	if keepExecutionProofs {
		stateproofs = avail(stateOldest, pruneMode.History)
	} else {
		stateproofs = CapabilityField{Disabled: true}
	}

	persistReceipts, err := kvcfg.PersistReceipts.Enabled(tx)
	if err != nil {
		return nil, err
	}

	stateField := avail(stateOldest, pruneMode.History)
	blocksField := avail(blocksOldest, pruneMode.Blocks)

	// The receipt cache exists on disk only with --prune.include-receipts, and enabling
	// it says nothing about how much is kept: RCacheDomain is retired on its own
	// --prune.receipts.distance window when one is set, and alongside history otherwise.
	// Below a window of its own the read falls back to re-execution, which reaches as far
	// as history, so the wider of the two decides. This mirrors checkReceiptsAvailable.
	receiptsOldest, receiptsAmount := stateOldest, pruneMode.History
	if persistReceipts && receipts.PersistedReceiptsServed() {
		switch amount := pruneMode.ReceiptsAmount(); {
		case amount == prune.KeepAllReceiptsPruneMode:
			receiptsOldest, receiptsAmount = 0, amount
		case !amount.Enabled():
		default:
			receiptsOldest, receiptsAmount = widerRetention(amount.PruneTo(headBlock), amount, stateOldest, pruneMode.History)
		}
	}
	// Below Byzantium the receipt carries a post state the cache does not store, so
	// those blocks are re-executed and reach only as far as history. This mirrors
	// postStateCalculated, down to the shape that computes the post state at all and
	// to a chain that never reaches the fork.
	byzantium := uint64(math.MaxUint64)
	if chainConfig.ByzantiumBlock != nil {
		byzantium = *chainConfig.ByzantiumBlock
	}
	if receipts.PostStateCalculated(chainConfig, receiptsOldest, keepExecutionProofs, api._blockReader) {
		if stateOldest < byzantium {
			receiptsOldest, receiptsAmount = stricterRetention(receiptsOldest, receiptsAmount, stateOldest, pruneMode.History)
		} else {
			// A fork height is not a window: keeping the amount would advertise a
			// retention whose head - retentionBlocks lands below this oldest block.
			receiptsOldest, receiptsAmount = byzantium, prune.KeepAllBlocksPruneMode
		}
	}
	// Reading the receipts of a block needs its body too: the stored receipt carries no
	// TxHash, so it is derived from the block's transaction.
	receiptsOldest, receiptsAmount = stricterRetention(receiptsOldest, receiptsAmount, blocksOldest, pruneMode.Blocks)
	receiptsField := avail(receiptsOldest, receiptsAmount)

	// A log query filtered by address or topic searches LogAddrIdx and LogTopicIdx,
	// standalone indices retired at the history cutoff whatever the receipt retention is.
	// The field takes that stricter form: an unfiltered query reads straight from the
	// receipts and reaches further back than advertised.
	logsOldest, logsAmount := stricterRetention(receiptsOldest, receiptsAmount, stateOldest, pruneMode.History)
	logsField := avail(logsOldest, logsAmount)

	return &CapabilitiesResult{
		Head:        CapabilityHead{Number: hexutil.Uint64(headBlock), Hash: headHash},
		State:       stateField,
		Tx:          blocksField, // tx-by-hash goes through block bodies; no independent tx-index pruning
		Logs:        logsField,
		Receipts:    receiptsField,
		Blocks:      blocksField,
		StateProofs: stateproofs,
	}, nil
}

// BlockNumber implements eth_blockNumber. Returns the block number of most recent block.
func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	blockNum, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(blockNum), nil
}

// Syncing implements eth_syncing. Returns a data object detailing the status of the sync process or false if not syncing.
func (api *APIImpl) Syncing(ctx context.Context) (any, error) {
	reply, err := api.ethBackend.Syncing(ctx)
	if err != nil {
		return false, err
	}
	if !reply.Syncing {
		return false, nil
	}

	// Still sync-ing, gather the block sync stats
	highestBlock := reply.LastNewBlockSeen
	currentBlock := reply.CurrentBlock

	return map[string]any{
		"startingBlock": "0x0", // 0x0 is a placeholder, I do not think it matters what we return here
		"currentBlock":  hexutil.Uint64(currentBlock),
		"highestBlock":  hexutil.Uint64(highestBlock),
		"stages":        stagesFromReply(reply.Stages),
	}, nil
}

// ChainId implements eth_chainId. Returns the current ethereum chainId.
func (api *APIImpl) ChainId(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(chainConfig.ChainID.Uint64()), nil
}

// ChainID alias of ChainId - just for convenience
func (api *APIImpl) ChainID(ctx context.Context) (hexutil.Uint64, error) {
	return api.ChainId(ctx)
}

// ProtocolVersion implements eth_protocolVersion. Returns the current ethereum protocol version.
func (api *APIImpl) ProtocolVersion(ctx context.Context) (hexutil.Uint, error) {
	ver, err := api.ethBackend.ProtocolVersion(ctx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint(ver), nil
}

// GasPrice implements eth_gasPrice. Returns the current price per gas in wei.
func (api *APIImpl) GasPrice(ctx context.Context) (*hexutil.U256, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := api.newGasOracle(tx)
	tipcap, err := oracle.SuggestTipCap(ctx)
	if err != nil {
		return nil, err
	}
	gasResult := uint256.NewInt(0)
	gasResult.Set(tipcap)
	if head := rawdb.ReadCurrentHeader(tx); head != nil && head.BaseFee != nil {
		gasResult.Add(tipcap, head.BaseFee)
	}

	return (*hexutil.U256)(gasResult), err
}

// MaxPriorityFeePerGas returns a suggestion for a gas tip cap for dynamic fee transactions.
func (api *APIImpl) MaxPriorityFeePerGas(ctx context.Context) (*hexutil.U256, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := api.newGasOracle(tx)
	tipcap, err := oracle.SuggestTipCap(ctx)
	if err != nil {
		return nil, err
	}
	return (*hexutil.U256)(new(uint256.Int).Set(tipcap)), err
}

type feeHistoryResult struct {
	OldestBlock      *hexutil.Big     `json:"oldestBlock"`
	Reward           [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee          []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio     []float64        `json:"gasUsedRatio"`
	BlobBaseFee      []*hexutil.Big   `json:"baseFeePerBlobGas,omitempty"`
	BlobGasUsedRatio []float64        `json:"blobGasUsedRatio,omitempty"`
}

func (api *APIImpl) FeeHistory(ctx context.Context, blockCount rpc.DecimalOrHex, lastBlock rpc.BlockNumber, rewardPercentiles []float64) (*feeHistoryResult, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := api.newGasOracle(tx)

	oldest, reward, baseFee, gasUsed, blobBaseFee, blobGasUsedRatio, err := oracle.FeeHistory(ctx, int(blockCount), lastBlock, rewardPercentiles)
	if err != nil {
		return nil, err
	}
	results := &feeHistoryResult{
		OldestBlock:  (*hexutil.Big)(oldest),
		GasUsedRatio: gasUsed,
	}
	if reward != nil {
		results.Reward = make([][]*hexutil.Big, len(reward))
		for i, w := range reward {
			results.Reward[i] = make([]*hexutil.Big, len(w))
			for j, v := range w {
				results.Reward[i][j] = (*hexutil.Big)(v)
			}
		}
	}
	if baseFee != nil {
		results.BaseFee = make([]*hexutil.Big, len(baseFee))
		for i, v := range baseFee {
			results.BaseFee[i] = (*hexutil.Big)(v.ToBig())
		}
	}
	if blobBaseFee != nil {
		results.BlobBaseFee = make([]*hexutil.Big, len(blobBaseFee))
		for i, v := range blobBaseFee {
			results.BlobBaseFee[i] = (*hexutil.Big)(v.ToBig())
		}
	}
	if blobGasUsedRatio != nil {
		results.BlobGasUsedRatio = blobGasUsedRatio
	}
	return results, nil
}

// BlobBaseFee returns the base fee for blob gas at the current head.
func (api *APIImpl) BlobBaseFee(ctx context.Context) (*hexutil.U256, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	header := rawdb.ReadCurrentHeader(tx)
	if header == nil || header.ExcessBlobGas == nil {
		return nil, nil
	}
	config, err := api.BaseAPI.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, nil
	}
	nextBlockTime := header.Time + config.SecondsPerSlot()
	ret256, err := misc.GetBlobGasPrice(config, *header.ExcessBlobGas, nextBlockTime)
	if err != nil {
		return nil, err
	}
	return (*hexutil.U256)(&ret256), nil
}

// BaseFee returns the base fee at the current head.
func (api *APIImpl) BaseFee(ctx context.Context) (*hexutil.U256, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	header := rawdb.ReadCurrentHeader(tx)
	if header == nil {
		return nil, nil
	}
	config, err := api.BaseAPI.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if config == nil || !config.IsLondon(header.Number.Uint64()+1) {
		return nil, nil
	}
	baseFee := misc.CalcBaseFee(config, header)
	return (*hexutil.U256)(baseFee), nil
}

// EthHardForkConfig represents config of a hard-fork
type EthHardForkConfig struct {
	ActivationTime  uint64                    `json:"activationTime"`
	BlobSchedule    *params.BlobConfig        `json:"blobSchedule"`
	ChainId         hexutil.Uint              `json:"chainId"`
	ForkId          hexutil.Bytes             `json:"forkId"`
	Precompiles     map[string]common.Address `json:"precompiles"`
	SystemContracts map[string]common.Address `json:"systemContracts"`
}

// EthConfigResp is the response type of eth_config
type EthConfigResp struct {
	Current *EthHardForkConfig `json:"current"`
	Next    *EthHardForkConfig `json:"next"`
	Last    *EthHardForkConfig `json:"last"`
}

// Config returns the HardFork config for current and upcoming forks:
// assuming linear fork progression and ethereum-like schedule
func (api *APIImpl) Config(ctx context.Context, blockTimeOverride *hexutil.Uint64) (*EthConfigResp, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var currentBlockTime uint64
	if blockTimeOverride != nil {
		// optional utility arg to aid with testing
		currentBlockTime = blockTimeOverride.Uint64()
	} else {
		h, err := api.headerByNumber(ctx, rpc.LatestBlockNumber, tx)
		if err != nil {
			return nil, err
		}
		if h == nil {
			return nil, errors.New("latest header not found")
		}
		currentBlockTime = h.Time
	}

	chainConfig, genesis, err := api.chainConfigWithGenesis(ctx, tx)
	if err != nil {
		return nil, err
	}
	gatherForksFrom := genesis.Time()
	if genesis.Time() >= currentBlockTime {
		// handle forks activated at genesis with activation time 0
		gatherForksFrom = 0
		currentBlockTime = 0
	}

	response := EthConfigResp{}
	forkBlockNums, forkTimes := forkid.GatherForks(chainConfig, gatherForksFrom)
	// current fork config
	currentForkId := forkid.NewIDFromForks(forkBlockNums, forkTimes, genesis.Hash(), math.MaxUint64, currentBlockTime)
	response.Current = fillForkConfig(chainConfig, currentForkId.Hash, currentForkId.Activation)

	// next fork config
	if currentForkId.Next == 0 {
		// means there are no later forks setup to be activated after the current one
		return &response, nil
	}

	nextForkId := forkid.NewIDFromForks(forkBlockNums, forkTimes, genesis.Hash(), math.MaxUint64, currentForkId.Next)
	response.Next = fillForkConfig(chainConfig, nextForkId.Hash, nextForkId.Activation)

	// last fork config
	lastForkId := forkid.NewIDFromForks(forkBlockNums, forkTimes, genesis.Hash(), math.MaxUint64, math.MaxUint64)
	response.Last = fillForkConfig(chainConfig, lastForkId.Hash, lastForkId.Activation)

	return &response, nil
}

func fillForkConfig(chainConfig *chain.Config, forkId [4]byte, activationTime uint64) *EthHardForkConfig {
	forkConfig := EthHardForkConfig{}
	forkConfig.ActivationTime = activationTime
	forkConfig.BlobSchedule = chainConfig.GetBlobConfig(activationTime)
	forkConfig.ChainId = hexutil.Uint(chainConfig.ChainID.Uint64())
	forkConfig.ForkId = forkId[:]
	blockContext := evmtypes.BlockContext{
		BlockNumber: math.MaxUint64,
		Time:        activationTime,
	}
	precompiles := vm.Precompiles(blockContext.Rules(chainConfig))
	forkConfig.Precompiles = make(map[string]common.Address, len(precompiles))
	for addr, precompile := range precompiles {
		forkConfig.Precompiles[precompile.Name()] = addr.Value()
	}
	systemContracts := chainConfig.SystemContracts(activationTime)
	forkConfig.SystemContracts = make(map[string]common.Address, len(systemContracts))
	for name, contract := range systemContracts {
		forkConfig.SystemContracts[name] = contract.Value()
	}
	return &forkConfig
}

type GasPriceOracleBackend struct {
	db      kv.TemporalRoDB // nil if Fork is not supported
	tx      kv.TemporalTx   // always a pinned view; carries the request's overlay resolution
	baseApi *BaseAPI

	parentViewID uint64
	parentTip    canonicalMarker
	parentTipErr error
	forkPrepared bool

	blocksFloorOnce sync.Once
	blocksFloor     uint64
	blocksFloorErr  error
}

// canonicalMarker is one entry of the canonical number-to-hash mapping.
type canonicalMarker struct {
	number uint64
	hash   common.Hash
}

// NewGasPriceOracleBackend requires a tx already pinned at acquisition (see
// rpchelper.BeginTemporalRoWithOverlay): resolving the overlay here, after
// the caller opened the tx, would re-open the torn (tx, overlay) window the
// pinned acquisition exists to close.
func NewGasPriceOracleBackend(db kv.TemporalRoDB, tx kv.TemporalTx, baseApi *BaseAPI) *GasPriceOracleBackend {
	if !membatchwithdb.CarriesOverlayView(tx) {
		panic("NewGasPriceOracleBackend: tx must be pinned via rpchelper.BeginTemporalRoWithOverlay or PinToOverlay")
	}
	return &GasPriceOracleBackend{db: db, tx: tx, baseApi: baseApi, parentViewID: tx.ViewID()}
}

// PrepareFork resolves the canonical marker the parent snapshot ends on, which
// Fork compares a fresh snapshot against. Resolving it lazily keeps the scan —
// a remote round trip in rpcdaemon mode — off the requests the tip cache answers.
func (b *GasPriceOracleBackend) PrepareFork(context.Context) error {
	if b.forkPrepared {
		return b.parentTipErr
	}
	b.parentTip, _, b.parentTipErr = edgeCanonicalMarker(b.tx, nil, order.Desc)
	b.forkPrepared = true
	return b.parentTipErr
}

// edgeCanonicalMarker reads the canonical marker the database snapshot starts
// (order.Asc) or ends (order.Desc) on, from key from onwards, bypassing the
// overlay: parent and fork share the overlay, so a view read would mask what
// these identities exist to detect.
func edgeCanonicalMarker(tx kv.TemporalTx, from []byte, asc order.By) (canonicalMarker, bool, error) {
	raw := tx
	if u, ok := tx.(interface{ UnderlyingTx() kv.TemporalTx }); ok {
		if under := u.UnderlyingTx(); under != nil {
			raw = under
		}
	}
	it, err := raw.Range(kv.HeaderCanonical, from, nil, asc, 1)
	if err != nil {
		return canonicalMarker{}, false, err
	}
	defer it.Close()
	if !it.HasNext() {
		return canonicalMarker{}, false, nil
	}
	k, v, err := it.Next()
	if err != nil {
		return canonicalMarker{}, false, err
	}
	if len(k) != 8 || len(v) != length.Hash {
		return canonicalMarker{}, false, nil
	}
	return canonicalMarker{number: binary.BigEndian.Uint64(k), hash: common.BytesToHash(v)}, true, nil
}

func canonicalHashAt(tx kv.Tx, number uint64) (common.Hash, error) {
	v, err := tx.GetOne(kv.HeaderCanonical, hexutil.EncodeTs(number))
	if err != nil {
		return common.Hash{}, err
	}
	if len(v) != length.Hash {
		return common.Hash{}, nil
	}
	return common.BytesToHash(v), nil
}

// keepsParentIdentities reports whether tx's snapshot still holds the canonical
// marker the parent's snapshot ended on. A block appended since then keeps it,
// while a reorg rewrites or truncates it — and rewriting a lower height implies
// the markers above were unwound first, so the tip alone covers the range.
func (b *GasPriceOracleBackend) keepsParentIdentities(tx kv.TemporalTx) (bool, error) {
	if b.parentTip.hash == (common.Hash{}) {
		return false, nil
	}
	hash, err := canonicalHashAt(tx, b.parentTip.number)
	if err != nil {
		return false, err
	}
	return hash == b.parentTip.hash, nil
}

func (b *GasPriceOracleBackend) Fork(ctx context.Context) (gasprice.OracleBackend, func(), error) {
	if b.db == nil {
		return nil, nil, nil // Fork not supported; caller falls back to sequential
	}
	if !b.forkPrepared {
		return nil, nil, errors.New("GasPriceOracleBackend.Fork: PrepareFork must run on the caller's goroutine first")
	}
	if b.parentTipErr != nil {
		return nil, nil, b.parentTipErr
	}
	tx, err := b.db.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, nil, err
	}
	// A fresh tx takes its own database snapshot, which can already carry a
	// reorg the parent never saw: the pin only aligns the overlay layer. Serving
	// one request from two chains is worse than losing the parallelism, so a
	// disagreeing snapshot degrades to sequential reads on the parent. An
	// identical view id means the same snapshot, which needs no marker lookup.
	if tx.ViewID() != b.parentViewID {
		keeps, err := b.keepsParentIdentities(tx)
		if err != nil || !keeps {
			tx.Rollback()
			return nil, nil, err
		}
	}
	// Reuse the parent's pin (rationale on rpchelper.PinToOverlay).
	overlay, _ := membatchwithdb.ViewOverlay(b.tx)
	return &GasPriceOracleBackend{db: b.db, tx: rpchelper.PinToOverlay(tx, overlay), baseApi: b.baseApi, parentViewID: tx.ViewID(), parentTip: b.parentTip, forkPrepared: true},
		func() { tx.Rollback() },
		nil
}

// CanonicalHashes scans the canonical markers of [from, to] on the pinned tx:
// resolving through the block reader would hit a live service in rpcdaemon
// mode, un-pinning the fee-history cache key, and a per-height read there is a
// round trip each. Callers only ask for unfrozen heights, whose markers are
// always in the db.
func (b *GasPriceOracleBackend) CanonicalHashes(_ context.Context, from, to uint64) ([]common.Hash, error) {
	hashes := make([]common.Hash, to-from+1)
	it, err := b.tx.Range(kv.HeaderCanonical, hexutil.EncodeTs(from), hexutil.EncodeTs(to+1), order.Asc, kv.Unlim)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return nil, err
		}
		if len(k) != 8 || len(v) != length.Hash {
			continue
		}
		hashes[binary.BigEndian.Uint64(k)-from] = common.BytesToHash(v)
	}
	return hashes, nil
}

// FrozenBlocks returns the boundary below which the canonical mapping can no
// longer change: those markers were pruned because their range is retired to
// snapshots, which is also why they cannot be resolved to a hash. Genesis is
// never pruned, so the scan starts above it. The Snapshots stage progress is
// not this boundary — it tracks min(Headers, Bodies, Senders, TxLookup), which
// on a synced node sits at the head and would make reorgable heights
// number-keyed.
func (b *GasPriceOracleBackend) FrozenBlocks() (uint64, error) {
	lowest, ok, err := edgeCanonicalMarker(b.tx, hexutil.EncodeTs(1), order.Asc)
	if err != nil || !ok || lowest.number == 0 {
		return 0, err
	}
	return lowest.number - 1, nil
}

func (b *GasPriceOracleBackend) HeaderByHashNumber(ctx context.Context, hash common.Hash, number uint64) (*types.Header, error) {
	return b.baseApi._blockReader.Header(ctx, b.tx, hash, number)
}

func (b *GasPriceOracleBackend) BlockByHashNumber(ctx context.Context, hash common.Hash, number uint64) (*types.Block, error) {
	available, err := b.isBlockAvailable(ctx, number)
	if err != nil || !available {
		return nil, err
	}
	return b.baseApi.blockWithSenders(ctx, b.tx, hash, number)
}

func (b *GasPriceOracleBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	header, err := b.baseApi.headerByNumber(ctx, number, b.tx)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	return header, nil
}

func (b *GasPriceOracleBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	available, err := b.isBlockAvailable(ctx, number.Uint64())
	if err != nil || !available {
		return nil, err
	}
	return b.baseApi.blockByNumberWithSenders(ctx, b.baseApi.filters.WithOverlay(b.tx), number.Uint64())
}

func (b *GasPriceOracleBackend) isBlockAvailable(ctx context.Context, number uint64) (bool, error) {
	// One backend serves one request through a pinned transaction, so its physical
	// block floor is stable and needs to be resolved only once.
	b.blocksFloorOnce.Do(func() {
		head, err := rpchelper.GetLatestBlockNumber(b.tx)
		if err != nil {
			b.blocksFloorErr = err
			return
		}
		b.blocksFloor, b.blocksFloorErr = b.baseApi.blocksAvailableFrom(ctx, b.tx, head)
	})
	if b.blocksFloorErr != nil {
		return false, b.blocksFloorErr
	}
	return number >= b.blocksFloor, nil
}

func (b *GasPriceOracleBackend) ChainConfig() *chain.Config {
	cc, _ := b.baseApi.chainConfig(context.Background(), b.tx)
	return cc
}

func (b *GasPriceOracleBackend) GetLatestBlockNumber() (uint64, error) {
	return rpchelper.GetLatestBlockNumber(b.tx)
}

func (b *GasPriceOracleBackend) GetReceipts(ctx context.Context, block *types.Block) (types.Receipts, error) {
	return b.baseApi.getReceipts(ctx, b.tx, block)
}

// PendingBlockAndReceipts returns the pending block and its receipts.
// It first tries the real pending block from the mining client (cached in filters),
// which is a block built on top of the current head and not yet finalised.
// When available, receipts are nil because the block has not been executed yet;
// callers that request reward percentiles will receive an empty entry for the
// pending slot, which is acceptable.
// If no pending block is available (e.g. no mining client configured), it falls
// back to the latest confirmed block with its receipts. This is a pragmatic
// workaround to avoid returning N-1 blocks instead of N when the caller requests
// "pending": baseFee and gasUsedRatio from the latest block are the best available
// approximation for the next block.
func (b *GasPriceOracleBackend) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	if block := b.baseApi.pendingBlock(); block != nil {
		return block, nil
	}
	latestNum, err := rpchelper.GetLatestBlockNumber(b.tx)
	if err != nil {
		return nil, nil
	}
	block, err := b.baseApi.blockByNumberWithSenders(context.Background(), b.baseApi.filters.WithOverlay(b.tx), latestNum)
	if err != nil || block == nil {
		return nil, nil
	}
	receipts, err := b.baseApi.getReceipts(context.Background(), b.tx, block)
	if err != nil {
		return nil, nil
	}
	return block, receipts
}

func (b *GasPriceOracleBackend) GetReceiptsGasUsed(ctx context.Context, block *types.Block) (types.Receipts, error) {
	return b.baseApi.getReceiptsGasUsed(ctx, b.tx, block)
}

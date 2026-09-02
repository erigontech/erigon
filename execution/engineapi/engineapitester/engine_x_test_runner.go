// Copyright 2026 The Erigon Authors
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

package engineapitester

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/trie"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/requests"
)

// NewEngineXTestRunner builds a runner that lazily creates engine-api testers
// per (fork, preAllocHash) tuple. The supplied ctx is forwarded to each tester
// at construction time. Options may be passed to customise the runner's
// behaviour (see EngineXTestRunnerOption). The caller must call Close on the
// returned runner to release the underlying testers and temp directories.
func NewEngineXTestRunner(ctx context.Context, logger log.Logger, preAllocsDir string, opts ...EngineXTestRunnerOption) (*EngineXTestRunner, error) {
	preAllocs := make(map[PreAllocHash]*PreAlloc)
	err := filepath.WalkDir(preAllocsDir, func(path string, info os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var preAlloc PreAlloc
		err = jsoniter.ConfigFastest.Unmarshal(b, &preAlloc)
		if err != nil {
			return err
		}
		preAllocs[PreAllocHash(strings.TrimSuffix(info.Name(), filepath.Ext(info.Name())))] = &preAlloc
		return nil
	})
	if err != nil {
		return nil, err
	}
	runner := &EngineXTestRunner{
		ctx:       ctx,
		logger:    logger,
		preAllocs: preAllocs,
		testers:   make(map[Fork]map[PreAllocHash]testerEntry),
	}
	for _, opt := range opts {
		opt(runner)
	}
	return runner, nil
}

// EngineXTestRunnerOption customises an EngineXTestRunner at construction time.
type EngineXTestRunnerOption func(*EngineXTestRunner)

// WithRequestProfileHook installs a hook called around each engine API request
// the runner makes. See RequestProfileHook for the contract.
func WithRequestProfileHook(hook RequestProfileHook) EngineXTestRunnerOption {
	return func(r *EngineXTestRunner) {
		r.profileHook = hook
	}
}

func WithWarmupKzgCtxOnInit(warmup bool) EngineXTestRunnerOption {
	return func(r *EngineXTestRunner) {
		r.warmupKzgCtxOnInit = warmup
	}
}

type EngineXTestRunner struct {
	ctx                context.Context
	logger             log.Logger
	preAllocs          map[PreAllocHash]*PreAlloc
	mu                 sync.Mutex
	testers            map[Fork]map[PreAllocHash]testerEntry
	profileHook        RequestProfileHook
	warmupKzgCtxOnInit bool
}

// RequestProfileHook is invoked immediately before each engine API request the
// runner makes (NewPayload, FCU). The returned stop function is invoked after
// the request returns. `kind` is "newpayload" or "fcu"; `id` is a stable
// per-request suffix (test name + height/hash) suitable for use in a filename.
// The hook is shared across all goroutines invoking Run on this runner; the
// hook implementation is responsible for synchronisation if needed (process-
// global pprof state typically requires serialising profile starts).
type RequestProfileHook func(kind, id string) (stop func())

// testerEntry pairs a cached EngineApiTester with the temp directory created
// for it, so eviction can close the tester and remove the directory together.
type testerEntry struct {
	tester  EngineApiTester
	dataDir string
}

// Close releases all cached testers and removes any temp directories created
// for them. Errors are joined so a single late failure does not skip earlier
// cleanups. The map is snapshotted under the lock and then drained without
// it, so the slow tester.Close + dir.RemoveAll work runs in parallel rather
// than serialised behind extr.mu.
func (extr *EngineXTestRunner) Close() error {
	extr.mu.Lock()
	var entries []testerEntry
	for _, perAlloc := range extr.testers {
		for i := range perAlloc {
			entries = append(entries, perAlloc[i])
		}
	}
	extr.testers = nil
	extr.mu.Unlock()
	var errs []error
	for i := range entries {
		err := extr.evict(entries[i])
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Evict closes the tester for (fork, preAllocHash) and removes its temp dir.
// Safe to call when no such tester exists. Use this to free a tester after a
// group of tests has finished, so a worker slot can host another (fork,
// preAllocHash) combination. The lock is held only long enough to remove the
// entry from the map; the slow tester.Close and dir.RemoveAll run unlocked
// so other workers can concurrently enter getOrCreateTester / Evict.
func (extr *EngineXTestRunner) Evict(fork Fork, preAllocHash PreAllocHash) error {
	extr.mu.Lock()
	perAlloc, ok := extr.testers[fork]
	if !ok {
		extr.mu.Unlock()
		return nil
	}
	entry, ok := perAlloc[preAllocHash]
	if !ok {
		extr.mu.Unlock()
		return nil
	}
	delete(perAlloc, preAllocHash)
	if len(perAlloc) == 0 {
		delete(extr.testers, fork)
	}
	extr.mu.Unlock()
	return extr.evict(entry)
}

// evict performs the slow close-and-remove for a single tester. It does NOT
// touch extr.mu or extr.testers — callers are responsible for removing the
// entry from the map first. Shared by Evict (single-key) and Close (drain).
func (extr *EngineXTestRunner) evict(entry testerEntry) error {
	var errs []error
	err := entry.tester.Close()
	if err != nil {
		errs = append(errs, err)
	}
	err = dir.RemoveAll(entry.dataDir)
	if err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// testNameKey is the unexported context key callers use to attach a test name
// to the ctx passed into Run. The profile hook embeds the name in per-request
// profile ids so callers can route profiles into named files.
type testNameKey struct{}

// ContextWithTestName returns a copy of ctx that carries the given test name,
// retrievable inside Run by the runner's per-request profile hook plumbing.
// An empty name is treated as "no name" and produces unprefixed profile ids.
func ContextWithTestName(ctx context.Context, name string) context.Context {
	if name == "" {
		return ctx
	}
	return context.WithValue(ctx, testNameKey{}, name)
}

func testNameFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(testNameKey{}).(string); ok {
		return v
	}
	return ""
}

func (extr *EngineXTestRunner) Run(ctx context.Context, test EngineXTestDefinition) error {
	tester, err := extr.getOrCreateTester(test.Fork, test.PreAllocHash)
	if err != nil {
		return err
	}
	return extr.execute(ctx, tester, test)
}

// EnsureTester pre-creates the tester for the given test's fork+preAllocHash.
// Call before benchmark timing to exclude setup costs.
func (extr *EngineXTestRunner) EnsureTester(test EngineXTestDefinition) error {
	_, err := extr.getOrCreateTester(test.Fork, test.PreAllocHash)
	return err
}

func (extr *EngineXTestRunner) execute(ctx context.Context, tester EngineApiTester, test EngineXTestDefinition) error {
	name := testNameFromContext(ctx)
	// reset back to genesis before test new payloads
	var fcuVersion string
	if len(test.NewPayloads) > 0 {
		fcuVersion = test.NewPayloads[0].FcuVersion
	} else {
		fcuVersion = fcuVersionFromTimestamp(tester.ChainConfig, tester.GenesisBlock.Time())
	}
	err := processFcu(ctx, tester, tester.GenesisBlock.Hash(), fcuVersion, name, extr.profileHook)
	if err != nil {
		return fmt.Errorf("reset head to genesis: %w", err)
	}
	for _, newPayload := range test.NewPayloads {
		err := processNewPayload(ctx, tester, newPayload, name, extr.profileHook)
		if err != nil {
			return err
		}
	}
	return verifyEngineXResult(ctx, tester.RpcApiClient, test)
}

func fcuVersionFromTimestamp(config *chain.Config, timestamp uint64) string {
	switch {
	case config.IsAmsterdam(timestamp):
		return "4"
	case config.IsCancun(timestamp):
		return "3"
	case config.IsShanghai(timestamp):
		return "2"
	default:
		return "1"
	}
}

type engineXResultReader interface {
	GetBlockByNumber(context.Context, rpc.BlockNumber, bool) (*requests.Block, error)
	GetProof(context.Context, common.Address, []common.Hash, rpc.BlockReference) (*accounts.AccProofResult, error)
}

func verifyEngineXResult(ctx context.Context, reader engineXResultReader, test EngineXTestDefinition) error {
	blockRef := rpc.LatestBlock
	if test.LastBlockHash != nil {
		block, err := reader.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		if err != nil {
			return fmt.Errorf("get final head: %w", err)
		}
		if block == nil {
			return errors.New("get final head: block not found")
		}
		if block.Hash != *test.LastBlockHash {
			return fmt.Errorf("final head mismatch: want %s, got %s", test.LastBlockHash, block.Hash)
		}
		blockRef = rpc.AsBlockReference(*test.LastBlockHash)
	}
	if test.PostStateDiff == nil {
		return nil
	}
	addresses := make([]common.Address, 0, len(*test.PostStateDiff))
	for address := range *test.PostStateDiff {
		addresses = append(addresses, address)
	}
	slices.SortFunc(addresses, func(a, b common.Address) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, address := range addresses {
		proof, err := reader.GetProof(ctx, address, nil, blockRef)
		if err != nil {
			return fmt.Errorf("get proof for %s: %w", address, err)
		}
		if err := verifyEngineXAccount(address, (*test.PostStateDiff)[address], proof); err != nil {
			return err
		}
	}
	return nil
}

func verifyEngineXAccount(address common.Address, expected *types.GenesisAccount, proof *accounts.AccProofResult) error {
	if proof == nil || proof.Balance == nil {
		return fmt.Errorf("postStateDiff account %s: incomplete proof", address)
	}
	if expected == nil {
		if proof.Balance.ToInt().Sign() != 0 || proof.Nonce != 0 || proof.CodeHash != (common.Hash{}) || proof.StorageHash != (common.Hash{}) {
			return fmt.Errorf("postStateDiff account %s: expected account to be absent", address)
		}
		return nil
	}
	expectedBalance := expected.Balance
	if expectedBalance == nil {
		expectedBalance = new(big.Int)
	}
	if proof.Balance.ToInt().Cmp(expectedBalance) != 0 {
		return fmt.Errorf("postStateDiff account %s balance mismatch: want %s, got %s", address, expectedBalance, proof.Balance.ToInt())
	}
	if uint64(proof.Nonce) != expected.Nonce {
		return fmt.Errorf("postStateDiff account %s nonce mismatch: want %d, got %d", address, expected.Nonce, proof.Nonce)
	}
	expectedCodeHash := crypto.Keccak256Hash(expected.Code)
	if proof.CodeHash != expectedCodeHash {
		return fmt.Errorf("postStateDiff account %s code hash mismatch: want %s, got %s", address, expectedCodeHash, proof.CodeHash)
	}
	expectedStorageRoot := engineXStorageRoot(expected.Storage)
	if proof.StorageHash != expectedStorageRoot {
		return fmt.Errorf("postStateDiff account %s storage root mismatch: want %s, got %s", address, expectedStorageRoot, proof.StorageHash)
	}
	return nil
}

func engineXStorageRoot(storage map[common.Hash]common.Hash) common.Hash {
	storageTrie := trie.NewInMemoryTrie(nil)
	keys := make([]common.Hash, 0, len(storage))
	for key := range storage {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b common.Hash) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, key := range keys {
		value := storage[key]
		trimmed := bytes.TrimLeft(value[:], "\x00")
		if len(trimmed) != 0 {
			storageTrie.Update(crypto.Keccak256(key[:]), trimmed)
		}
	}
	return storageTrie.Hash()
}

// getOrCreateTester returns a cached tester for (fork, preAllocHash) if one
// exists, otherwise it creates a new one. The slow InitialiseEngineApiTester
// path runs WITHOUT extr.mu held so other workers can hit the cache or start
// their own creates concurrently. If two callers race to create for the same
// key, the second one's tester is closed and the first one's cached tester is
// returned (rare in practice — workers in the CLI handle distinct keys).
func (extr *EngineXTestRunner) getOrCreateTester(fork Fork, preAllocHash PreAllocHash) (EngineApiTester, error) {
	extr.mu.Lock()
	if perAlloc, ok := extr.testers[fork]; ok {
		if entry, ok := perAlloc[preAllocHash]; ok {
			extr.mu.Unlock()
			return entry.tester, nil
		}
	}
	extr.mu.Unlock()
	// Slow path: build the genesis + data dir + node *without* holding the
	// lock so concurrent getOrCreateTester / Evict calls for other keys can
	// proceed.
	entry, err := extr.createTester(fork, preAllocHash)
	if err != nil {
		return EngineApiTester{}, err
	}
	// Re-acquire the lock to publish the entry. If a concurrent caller for
	// the same (fork, preAllocHash) pair already published one, discard ours.
	extr.mu.Lock()
	perAlloc, ok := extr.testers[fork]
	if !ok {
		perAlloc = make(map[PreAllocHash]testerEntry)
		extr.testers[fork] = perAlloc
	} else if existing, ok := perAlloc[preAllocHash]; ok {
		extr.mu.Unlock()
		// Lost the race; close our duplicate. evict acquires no locks and is
		// safe to call here.
		_ = extr.evict(entry)
		return existing.tester, nil
	}
	perAlloc[preAllocHash] = entry
	extr.mu.Unlock()
	return entry.tester, nil
}

// createTester builds a fresh EngineApiTester for (fork, preAllocHash). The
// caller is responsible for caching/publishing the result. No locks are taken.
func (extr *EngineXTestRunner) createTester(fork Fork, preAllocHash PreAllocHash) (testerEntry, error) {
	forkConfig, ok := testforks.Forks[fork.String()]
	if !ok {
		return testerEntry{}, testforks.UnsupportedForkError{Name: fork.String()}
	}
	alloc, ok := extr.preAllocs[preAllocHash]
	if !ok {
		return testerEntry{}, fmt.Errorf("pre_alloc %s not found", preAllocHash)
	}
	var forkConfigCopy chain.Config
	err := copier.CopyWithOption(&forkConfigCopy, forkConfig, copier.Option{DeepCopy: true})
	if err != nil {
		return testerEntry{}, err
	}
	forkConfig = &forkConfigCopy
	var genesis types.Genesis
	if alloc.Environment.GasLimit != 0 {
		// Earlier builder format stores genesis inputs as environment fields.
		env := alloc.Environment
		genesis = types.Genesis{
			Config:     forkConfig,
			Alloc:      alloc.Alloc,
			ExtraData:  []byte{0},
			Coinbase:   env.Coinbase,
			GasLimit:   uint64(env.GasLimit),
			Difficulty: uint256.NewInt(uint64(env.Difficulty)),
			Timestamp:  uint64(env.Timestamp),
		}
		if env.BaseFee != nil {
			genesis.BaseFee = uint256.NewInt(uint64(*env.BaseFee))
		}
		if env.ExcessBlobGas != nil {
			v := uint64(*env.ExcessBlobGas)
			genesis.ExcessBlobGas = &v
		}
		if env.BlobGasUsed != nil {
			v := uint64(*env.BlobGasUsed)
			genesis.BlobGasUsed = &v
		}
		if env.SlotNumber != nil {
			v := uint64(*env.SlotNumber)
			genesis.SlotNumber = &v
		}
	} else {
		// Final pre-allocation groups contain the genesis header directly.
		genesis = alloc.Genesis
		genesis.Alloc = alloc.Alloc
		genesis.Config = forkConfig
	}
	dataDir, err := os.MkdirTemp("", "enginex-tester-*")
	if err != nil {
		return testerEntry{}, fmt.Errorf("create temp data dir: %w", err)
	}
	engineApiClientTimeout := 10 * time.Minute
	tester, err := InitialiseEngineApiTester(extr.ctx, EngineApiTesterInitArgs{
		Logger:                 extr.logger,
		DataDir:                dataDir,
		Genesis:                &genesis,
		NoEmptyBlock1:          true,
		EngineApiClientTimeout: &engineApiClientTimeout,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = 512
			config.WarmupKzgCtxOnInit = extr.warmupKzgCtxOnInit
		},
		DisableTxPool: true,
		DisableSentry: true,
		// 8 GiB headroom for benchmark fixtures with large pre-alloc bytecode.
		MdbxDBSizeLimit: 8 * datasize.GB,
	})
	if err != nil {
		// Best-effort: drop the temp dir we just created. The tester wasn't
		// returned, so its own cleanups have already run inside
		// InitialiseEngineApiTester's rollback path.
		_ = dir.RemoveAll(dataDir)
		return testerEntry{}, fmt.Errorf("initialise tester for fork=%s preAlloc=%s: %w", fork, preAllocHash, err)
	}
	return testerEntry{tester: tester, dataDir: dataDir}, nil
}

func processNewPayload(ctx context.Context, tester EngineApiTester, payload EngineXTestNewPayload, testName string, hook RequestProfileHook) error {
	var enginePayload enginetypes.ExecutionPayload
	var blobHashes []common.Hash
	var parentBeaconRoot common.Hash
	var executionRequests []hexutil.Bytes
	err := json.Unmarshal(payload.Params[0], &enginePayload)
	if err != nil {
		return err
	}
	if len(payload.Params) > 1 {
		err := json.Unmarshal(payload.Params[1], &blobHashes)
		if err != nil {
			return err
		}
	}
	if len(payload.Params) > 2 {
		err := json.Unmarshal(payload.Params[2], &parentBeaconRoot)
		if err != nil {
			return err
		}
	}
	if len(payload.Params) > 3 {
		err := json.Unmarshal(payload.Params[3], &executionRequests)
		if err != nil {
			return err
		}
	}
	expectFailure := payload.ValidationError != "" || payload.ErrorCode != ""
	npStop := beginProfile(hook, "newpayload", testName, fmt.Sprintf("h%d_%s", uint64(enginePayload.BlockNumber), enginePayload.BlockHash.Hex()))
	enginePayloadStatus, err := RetryEngine(
		ctx,
		[]enginetypes.EngineStatus{enginetypes.SyncingStatus},
		nil,
		func() (*enginetypes.PayloadStatus, enginetypes.EngineStatus, error) {
			var r *enginetypes.PayloadStatus
			var err error
			switch payload.NewPayloadVersion {
			case "1":
				r, err = tester.EngineApiClient.NewPayloadV1(ctx, &enginePayload)
			case "2":
				r, err = tester.EngineApiClient.NewPayloadV2(ctx, &enginePayload)
			case "3":
				r, err = tester.EngineApiClient.NewPayloadV3(ctx, &enginePayload, blobHashes, &parentBeaconRoot)
			case "4":
				r, err = tester.EngineApiClient.NewPayloadV4(ctx, &enginePayload, blobHashes, &parentBeaconRoot, executionRequests)
			case "5":
				r, err = tester.EngineApiClient.NewPayloadV5(ctx, &enginePayload, blobHashes, &parentBeaconRoot, executionRequests)
			default:
				return nil, "", fmt.Errorf("unsupported new payload version: %s", payload.NewPayloadVersion)
			}
			if err != nil {
				return nil, "", err
			}
			return r, r.Status, nil
		},
	)
	if err != nil {
		npStop()
		if expectFailure {
			return nil
		}
		return err
	}
	npStop()
	if enginePayloadStatus.Status != enginetypes.ValidStatus {
		if expectFailure {
			return nil
		}
		return fmt.Errorf("payload status is not valid: %s", enginePayloadStatus.Status)
	}
	if expectFailure {
		return fmt.Errorf("expected payload to fail (validationError=%q errorCode=%q) but status was Valid", payload.ValidationError, payload.ErrorCode)
	}
	return processFcu(ctx, tester, enginePayload.BlockHash, payload.FcuVersion, testName, hook)
}

// beginProfile invokes the hook (if any) and returns a stop function. When the
// hook is nil, beginProfile returns a no-op stop so callers can always defer
// it unconditionally.
func beginProfile(hook RequestProfileHook, kind, testName, suffix string) func() {
	if hook == nil {
		return func() {}
	}
	id := suffix
	if testName != "" {
		id = testName + "__" + suffix
	}
	return hook(kind, id)
}

func processFcu(ctx context.Context, tester EngineApiTester, head common.Hash, version string, testName string, hook RequestProfileHook) error {
	fcu := enginetypes.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      common.Hash{},
		FinalizedBlockHash: common.Hash{},
	}
	stop := beginProfile(hook, "fcu", testName, head.Hex())
	defer stop()
	r, err := RetryEngine(
		ctx,
		[]enginetypes.EngineStatus{enginetypes.SyncingStatus},
		nil,
		func() (*enginetypes.ForkChoiceUpdatedResponse, enginetypes.EngineStatus, error) {
			var r *enginetypes.ForkChoiceUpdatedResponse
			var err error
			switch version {
			case "1":
				r, err = tester.EngineApiClient.ForkchoiceUpdatedV1(ctx, &fcu, nil)
			case "2":
				r, err = tester.EngineApiClient.ForkchoiceUpdatedV2(ctx, &fcu, nil)
			case "3":
				r, err = tester.EngineApiClient.ForkchoiceUpdatedV3(ctx, &fcu, nil)
			case "4":
				r, err = tester.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcu, nil, nil)
			default:
				return nil, "", fmt.Errorf("unsupported fcu version: %s", version)
			}
			if err != nil {
				return nil, "", err
			}
			return r, r.PayloadStatus.Status, nil
		},
	)
	if err != nil {
		return err
	}
	if r.PayloadStatus.Status != enginetypes.ValidStatus {
		return fmt.Errorf("payload status of fcu is not valid: %s", r.PayloadStatus.Status)
	}
	return nil
}

type EngineXTestDefinition struct {
	Fork          Fork                    `json:"network"`
	PreAllocHash  PreAllocHash            `json:"preHash"`
	LastBlockHash *common.Hash            `json:"lastblockhash"`
	PostStateDiff *EngineXPostStateDiff   `json:"postStateDiff"`
	NewPayloads   []EngineXTestNewPayload `json:"engineNewPayloads"`
}

type EngineXPostStateDiff map[common.Address]*types.GenesisAccount

func (diff *EngineXPostStateDiff) UnmarshalJSON(input []byte) error {
	var decoded map[common.UnprefixedAddress]*types.GenesisAccount
	if err := json.Unmarshal(input, &decoded); err != nil {
		return err
	}
	*diff = make(EngineXPostStateDiff, len(decoded))
	for address, account := range decoded {
		(*diff)[common.Address(address)] = account
	}
	return nil
}

type EngineXTestNewPayload struct {
	Params            []json.RawMessage `json:"params"`
	NewPayloadVersion string            `json:"newPayloadVersion"`
	FcuVersion        string            `json:"forkchoiceUpdatedVersion"`
	// ValidationError is the expected validation error name (e.g.
	// "BlockException.INCORRECT_BLOCK_FORMAT") for negative tests. When set,
	// the payload is expected to be rejected: either a non-Valid payload
	// status or any error returned by the engine API call counts as
	// success. A Valid status is a failure. Strict code/message matching is
	// intentionally skipped — EEST fixtures may be rejected at the JSON-RPC
	// parameter-validation step or by the payload validator depending on
	// implementation, and both forms are spec-permitted.
	ValidationError string `json:"validationError,omitempty"`
	// ErrorCode is the expected JSON-RPC error code (encoded as a string in
	// the EEST fixtures, e.g. "-32602") for malformed-payload tests. Treated
	// the same as ValidationError: any non-Valid status or RPC-level error
	// counts as success.
	ErrorCode string `json:"errorCode,omitempty"`
}

type PreAllocHash string

type PreAlloc struct {
	Environment EngineXEnvironment `json:"environment"`
	Genesis     types.Genesis      `json:"genesis"`
	Alloc       types.GenesisAlloc `json:"pre"`
}

// EngineXEnvironment maps the legacy builder-form pre-allocation fields.
type EngineXEnvironment struct {
	Coinbase      common.Address       `json:"currentCoinbase"`
	GasLimit      math.HexOrDecimal64  `json:"currentGasLimit"`
	Timestamp     math.HexOrDecimal64  `json:"currentTimestamp"`
	Difficulty    math.HexOrDecimal64  `json:"currentDifficulty"`
	BaseFee       *math.HexOrDecimal64 `json:"currentBaseFee"`
	ExcessBlobGas *math.HexOrDecimal64 `json:"currentExcessBlobGas"`
	BlobGasUsed   *math.HexOrDecimal64 `json:"currentBlobGasUsed"`
	SlotNumber    *math.HexOrDecimal64 `json:"slotNumber"`
}

type Fork string

func (f Fork) String() string {
	return string(f)
}

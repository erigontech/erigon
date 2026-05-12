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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

// NewEngineXTestRunner builds a runner that lazily creates engine-api testers
// per (fork, preAllocHash) tuple. The supplied ctx is forwarded to each tester
// at construction time. The caller must call Close on the returned runner to
// release the underlying testers and temp directories.
func NewEngineXTestRunner(ctx context.Context, logger log.Logger, preAllocsDir string) (*EngineXTestRunner, error) {
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
		err = json.Unmarshal(b, &preAlloc)
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
	return runner, nil
}

type EngineXTestRunner struct {
	ctx       context.Context
	logger    log.Logger
	preAllocs map[PreAllocHash]*PreAlloc
	mu        sync.Mutex
	testers   map[Fork]map[PreAllocHash]testerEntry
	wg        sync.WaitGroup
}

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
		for _, entry := range perAlloc {
			entries = append(entries, entry)
		}
	}
	extr.testers = nil
	extr.mu.Unlock()
	var errs []error
	for _, entry := range entries {
		err := extr.evict(entry)
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

// Execute runs the payload execution for a test (NewPayload + FCU)
// without any tester setup. The tester must already exist.
func (extr *EngineXTestRunner) Execute(ctx context.Context, test EngineXTestDefinition) error {
	tester, err := extr.getOrCreateTester(test.Fork, test.PreAllocHash)
	if err != nil {
		return err
	}
	return extr.execute(ctx, tester, test)
}

func (extr *EngineXTestRunner) execute(ctx context.Context, tester EngineApiTester, test EngineXTestDefinition) error {
	for _, newPayload := range test.NewPayloads {
		err := processNewPayload(ctx, tester, newPayload)
		if err != nil {
			return err
		}
	}
	return nil
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
	err := copier.Copy(&forkConfigCopy, forkConfig)
	if err != nil {
		return testerEntry{}, err
	}
	forkConfig = &forkConfigCopy
	var genesis types.Genesis
	if alloc.Environment.GasLimit != 0 {
		// New format: build genesis from environment fields
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
	} else {
		// Old format: genesis parsed directly from JSON
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
		},
		DisableTxPool: true,
		DisableSentry: true,
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

func processNewPayload(ctx context.Context, tester EngineApiTester, payload EngineXTestNewPayload) error {
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
		if expectFailure {
			return nil
		}
		return err
	}
	if enginePayloadStatus.Status != enginetypes.ValidStatus {
		if expectFailure {
			return nil
		}
		return fmt.Errorf("payload status is not valid: %s", enginePayloadStatus.Status)
	}
	if expectFailure {
		return fmt.Errorf("expected payload to fail (validationError=%q errorCode=%q) but status was Valid", payload.ValidationError, payload.ErrorCode)
	}
	return processFcu(ctx, tester, enginePayload.BlockHash, payload.FcuVersion)
}

func processFcu(ctx context.Context, tester EngineApiTester, head common.Hash, version string) error {
	fcu := enginetypes.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      common.Hash{},
		FinalizedBlockHash: common.Hash{},
	}
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
				r, err = tester.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcu, nil)
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
	Fork         Fork                    `json:"network"`
	PreAllocHash PreAllocHash            `json:"prehash"`
	NewPayloads  []EngineXTestNewPayload `json:"engineNewPayloads"`
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

// EngineXEnvironment maps the "environment" field from engine-x pre-alloc JSON files.
type EngineXEnvironment struct {
	Coinbase      common.Address       `json:"currentCoinbase"`
	GasLimit      math.HexOrDecimal64  `json:"currentGasLimit"`
	Timestamp     math.HexOrDecimal64  `json:"currentTimestamp"`
	Difficulty    math.HexOrDecimal64  `json:"currentDifficulty"`
	BaseFee       *math.HexOrDecimal64 `json:"currentBaseFee"`
	ExcessBlobGas *math.HexOrDecimal64 `json:"currentExcessBlobGas"`
	BlobGasUsed   *math.HexOrDecimal64 `json:"currentBlobGasUsed"`
}

type Fork string

func (f Fork) String() string {
	return string(f)
}

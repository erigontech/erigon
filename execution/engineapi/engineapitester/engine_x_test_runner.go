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
		testers:   make(map[Fork]map[PreAllocHash]EngineApiTester),
	}
	return runner, nil
}

type EngineXTestRunner struct {
	ctx       context.Context
	logger    log.Logger
	preAllocs map[PreAllocHash]*PreAlloc
	mu        sync.Mutex
	testers   map[Fork]map[PreAllocHash]EngineApiTester
	wg        sync.WaitGroup
	cleanups  []func() error
}

// Close releases all cached testers and removes any temp directories created
// for them. Cleanup callbacks run LIFO; errors are joined so a single late
// failure does not skip earlier cleanups.
func (extr *EngineXTestRunner) Close() error {
	extr.mu.Lock()
	defer extr.mu.Unlock()
	var errs []error
	for i := len(extr.cleanups) - 1; i >= 0; i-- {
		err := extr.cleanups[i]()
		if err != nil {
			errs = append(errs, err)
		}
	}
	extr.cleanups = nil
	extr.testers = nil
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
		if err := processNewPayload(ctx, tester, newPayload); err != nil {
			return err
		}
	}
	return nil
}

func (extr *EngineXTestRunner) getOrCreateTester(fork Fork, preAllocHash PreAllocHash) (EngineApiTester, error) {
	extr.mu.Lock()
	defer extr.mu.Unlock()
	testersPerAlloc, ok := extr.testers[fork]
	if ok {
		tester, ok := testersPerAlloc[preAllocHash]
		if ok {
			return tester, nil
		}
	} else {
		testersPerAlloc = make(map[PreAllocHash]EngineApiTester)
		extr.testers[fork] = testersPerAlloc
	}
	// create an engine api tester for [fork, preAllocHash] tuple
	forkConfig, ok := testforks.Forks[fork.String()]
	if !ok {
		return EngineApiTester{}, testforks.UnsupportedForkError{Name: fork.String()}
	}
	alloc, ok := extr.preAllocs[preAllocHash]
	if !ok {
		return EngineApiTester{}, fmt.Errorf("pre_alloc %s not found", preAllocHash)
	}
	var forkConfigCopy chain.Config
	err := copier.Copy(&forkConfigCopy, forkConfig)
	if err != nil {
		return EngineApiTester{}, err
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
		return EngineApiTester{}, fmt.Errorf("create temp data dir: %w", err)
	}
	// Track the temp dir cleanup before tester construction so the directory
	// is removed even if InitialiseEngineApiTester fails.
	extr.cleanups = append(extr.cleanups, func() error { return dir.RemoveAll(dataDir) })
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
	})
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("initialise tester for fork=%s preAlloc=%s: %w", fork, preAllocHash, err)
	}
	extr.cleanups = append(extr.cleanups, tester.Close)
	testersPerAlloc[preAllocHash] = tester
	return tester, nil
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
		return err
	}
	if enginePayloadStatus.Status != enginetypes.ValidStatus {
		return fmt.Errorf("payload status is not valid: %s", enginePayloadStatus.Status)
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

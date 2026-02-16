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

package executiontests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jinzhu/copier"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

func NewEngineXTestRunner(t *testing.T, logger log.Logger, preAllocsDir string) (*EngineXTestRunner, error) {
	preAllocs := make(map[PreAllocHash]*PreAlloc)
	err := filepath.WalkDir(preAllocsDir, func(path string, info os.DirEntry, err error) error {
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
		t:         t,
		logger:    logger,
		preAllocs: preAllocs,
		testers:   make(map[Fork]map[PreAllocHash]EngineApiTester),
	}
	return runner, nil
}

type EngineXTestRunner struct {
	t         *testing.T
	logger    log.Logger
	preAllocs map[PreAllocHash]*PreAlloc
	mu        sync.Mutex
	testers   map[Fork]map[PreAllocHash]EngineApiTester
	wg        sync.WaitGroup
}

func (extr *EngineXTestRunner) Run(ctx context.Context, test EngineXTestDefinition) error {
	tester, err := extr.getOrCreateTester(test.Fork, test.PreAllocHash)
	if err != nil {
		return err
	}
	if len(test.NewPayloads) > 0 {
		// make sure each test begins at genesis
		// TODO actually this should be a call to debug_setHead once we implement it
		//      because a FCU to an older canonical hash does NOT have to do an unwind as per spec
		//      (in fact it is in our interest NOT to unwind, and currently this is a no-op)
		//      hence debug_setHead will be the right way to do this once
		//      https://github.com/erigontech/erigon/issues/18922 is ready
		//err = processFcu(ctx, tester, tester.GenesisBlock.Hash(), test.NewPayloads[0].FcuVersion)
		//if err != nil {
		//	return err
		//}
	}
	for _, newPayload := range test.NewPayloads {
		err = processNewPayload(ctx, tester, newPayload)
		if err != nil {
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
	genesis := alloc.Genesis
	genesis.Alloc = alloc.Alloc
	genesis.Config = forkConfig
	engineApiClientTimeout := 10 * time.Minute
	tester := InitialiseEngineApiTester(extr.t, EngineApiTesterInitArgs{
		Logger:                 extr.logger,
		DataDir:                extr.t.TempDir(),
		Genesis:                &genesis,
		NoEmptyBlock1:          true,
		EngineApiClientTimeout: &engineApiClientTimeout,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = 512
		},
	})
	testersPerAlloc[preAllocHash] = tester
	return testersPerAlloc[preAllocHash], nil
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
	enginePayloadStatus, err := retryEngine(
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
	r, err := retryEngine(
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
	Genesis types.Genesis      `json:"genesis"`
	Alloc   types.GenesisAlloc `json:"pre"`
}

type Fork string

func (f Fork) String() string {
	return string(f)
}

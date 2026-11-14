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

package consensus_tests

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	serviceinterface "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
)

type gloasGossipMeta struct {
	Blocks              []gloasGossipBlock        `yaml:"blocks"`
	FinalizedCheckpoint *gloasFinalizedCheckpoint `yaml:"finalized_checkpoint"`
	Messages            []gloasGossipMessage      `yaml:"messages"`
}

type gloasGossipBlock struct {
	Block         string `yaml:"block"`
	Failed        bool   `yaml:"failed"`
	Pending       bool   `yaml:"pending"`
	Payload       string `yaml:"payload"`
	PayloadStatus string `yaml:"payload_status"`

	decoded *cltypes.SignedBeaconBlock
}

type gloasFinalizedCheckpoint struct {
	Epoch uint64       `yaml:"epoch"`
	Root  *common.Hash `yaml:"root"`
	Block string       `yaml:"block"`
}

type gloasGossipMessage struct {
	CurrentTimeMS *uint64 `yaml:"current_time_ms"`
	Message       string  `yaml:"message"`
	Expected      string  `yaml:"expected"`
}

type gossipFixtureNow struct {
	mu  sync.RWMutex
	now time.Time
}

func (n *gossipFixtureNow) Get() time.Time {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.now
}

func (n *gossipFixtureNow) Set(now time.Time) {
	n.mu.Lock()
	n.now = now
	n.mu.Unlock()
}

type gossipFixtureStore struct {
	*forkchoice.ForkChoiceStore
	finalized *solid.Checkpoint
	config    *clparams.BeaconChainConfig
	timeMs    uint64
	// minTimeMs keeps fork-choice time at or after the slot of the newest imported block, which
	// Caplin requires even though fixture message times may be earlier.
	minTimeMs uint64
}

func (s *gossipFixtureStore) FinalizedCheckpoint() solid.Checkpoint {
	if s.finalized != nil {
		return *s.finalized
	}
	return s.ForkChoiceStore.FinalizedCheckpoint()
}

type gloasGossipServices struct {
	envelope    services.ExecutionPayloadService
	attestation services.PayloadAttestationService
	preferences services.ProposerPreferencesService
}

func gloasGossipHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	var meta gloasGossipMeta
	if err := spectest.ReadMeta(root, "meta.yaml", &meta); err != nil {
		return err
	}
	anchorState, err := spectest.ReadBeaconState(root, c.Version(), "state.ssz_snappy")
	if err != nil {
		return err
	}
	cfg := anchorState.BeaconConfig()
	// Pyspec builds the setup chain with Gloas active from genesis, but validates gossip under the
	// configured fork epoch.
	gossipCfg := *cfg
	cfg.GloasForkEpoch = 0
	for i := range meta.Blocks {
		block := cltypes.NewSignedBeaconBlock(cfg, c.Version())
		if err := spectest.ReadSsz(root, c.Version(), meta.Blocks[i].Block+".ssz_snappy", block); err != nil {
			return fmt.Errorf("read setup block %d %s: %w", i, meta.Blocks[i].Block, err)
		}
		meta.Blocks[i].decoded = block
	}

	now := &gossipFixtureNow{now: time.Unix(int64(anchorState.GenesisTime()), 0)}
	clock := eth_clock.NewEthereumClockWithNow(anchorState.GenesisTime(), anchorState.GenesisValidatorsRoot(), &gossipCfg, now.Get)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	emitters := beaconevents.NewEventEmitter()
	networkCfg, _ := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobStorage := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	columnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), cfg, emitters)
	peerDasState := peerdasstate.NewPeerDasState(cfg, networkCfg)
	peerDas := das.NewPeerDas(nil, cfg, &clparams.CaplinConfig{}, columnStorage, blobStorage, nil, enode.ID{}, clock, peerDasState, nil, nil, db)
	syncedData := synced_data.NewSyncedDataManager(cfg, true)
	forkGraph, err := fork_graph.NewForkGraphDisk(anchorState, syncedData, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	if err != nil {
		return err
	}
	forkStore, err := forkchoice.NewForkChoiceStore(
		clock, anchorState, &forkChoiceSpectestEngine{}, pool.NewOperationsPool(cfg), forkGraph,
		emitters, syncedData, blobStorage, public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(), false, db,
	)
	if err != nil {
		return err
	}
	forkStore.SetSynced(true)
	forkStore.InitPeerDas(peerDas)
	store := &gossipFixtureStore{ForkChoiceStore: forkStore, config: cfg}
	store.timeMs = (anchorState.GenesisTime() + anchorState.Slot()*cfg.SecondsPerSlot) * 1000
	store.minTimeMs = store.timeMs
	if err := store.setFinalizedCheckpoint(root, c, meta.FinalizedCheckpoint); err != nil {
		return err
	}
	if err := store.importBlocks(ctx, meta.Blocks); err != nil {
		return err
	}

	headRoot, _, err := store.GetHead(nil)
	if err != nil {
		return fmt.Errorf("get fixture head: %w", err)
	}
	headState, err := store.GetStateAtBlockRoot(headRoot)
	if err != nil {
		return fmt.Errorf("get fixture head state: %w", err)
	}
	if headState == nil {
		return fmt.Errorf("get fixture head state: state for %x is unavailable", headRoot)
	}
	if err := syncedData.OnHeadStateWithBlockRoot(headState, headRoot); err != nil {
		return fmt.Errorf("set synced head state: %w", err)
	}

	epbsPool := pool.NewEpbsPool()
	fixtureServices := gloasGossipServices{
		envelope:    services.NewExecutionPayloadService(ctx, store, &gossipCfg, emitters, nil),
		attestation: services.NewPayloadAttestationService(ctx, store, clock, networkCfg, epbsPool, emitters, nil),
		preferences: services.NewProposerPreferencesService(syncedData, store, clock, &gossipCfg, epbsPool, emitters),
	}

	for i, message := range meta.Messages {
		if message.CurrentTimeMS != nil {
			now.Set(time.UnixMilli(int64(*message.CurrentTimeMS)))
			store.tick(*message.CurrentTimeMS)
		}
		serviceErr, err := fixtureServices.process(ctx, root, c.Version(), message.Message)
		if err != nil {
			return fmt.Errorf("message %d %s: %w", i, message.Message, err)
		}
		actual := gossipVerdict(serviceErr)
		if actual != message.Expected {
			if serviceErr == nil {
				return fmt.Errorf("message %d %s: expected %s, got valid", i, message.Message, message.Expected)
			}
			return fmt.Errorf("message %d %s: expected %s, got %s: %w", i, message.Message, message.Expected, actual, serviceErr)
		}
	}
	return nil
}

func (s *gossipFixtureStore) setFinalizedCheckpoint(root fs.FS, c spectest.TestCase, checkpoint *gloasFinalizedCheckpoint) error {
	if checkpoint == nil {
		return nil
	}
	if (checkpoint.Root == nil) == (checkpoint.Block == "") {
		return errors.New("finalized checkpoint must set exactly one of root or block")
	}
	rootHash := common.Hash{}
	if checkpoint.Root != nil {
		rootHash = *checkpoint.Root
	}
	if checkpoint.Block != "" {
		block := cltypes.NewSignedBeaconBlock(s.config, c.Version())
		if err := spectest.ReadSsz(root, c.Version(), checkpoint.Block+".ssz_snappy", block); err != nil {
			return fmt.Errorf("read finalized checkpoint block %s: %w", checkpoint.Block, err)
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return err
		}
		rootHash = common.Hash(blockRoot)
	}
	s.finalized = &solid.Checkpoint{Epoch: checkpoint.Epoch, Root: rootHash}
	return nil
}

func (s *gossipFixtureStore) importBlocks(ctx context.Context, blocks []gloasGossipBlock) error {
	if len(blocks) == 0 {
		return nil
	}
	anchorRoot, err := blocks[0].decoded.Block.HashSSZ()
	if err != nil {
		return err
	}
	if common.Hash(anchorRoot) != s.AnchorRoot() {
		return fmt.Errorf("anchor block %s root does not match anchor state", blocks[0].Block)
	}
	floor := s.minTimeMs
	for i, entry := range blocks {
		if entry.Payload != "" || entry.PayloadStatus != "" {
			return fmt.Errorf("setup block %d %s: payload and payload_status are not supported", i, entry.Block)
		}
		// Caplin keeps no block without a post-state, so a pending block stays unknown.
		if i == 0 || entry.Pending {
			continue
		}
		s.tick((entry.decoded.Block.Slot*s.config.SecondsPerSlot + s.GenesisTime()) * 1000)
		importErr := s.ForkChoiceStore.OnBlock(ctx, entry.decoded, true, true, true)
		if entry.Failed && importErr == nil {
			return fmt.Errorf("setup block %d %s: expected import failure", i, entry.Block)
		}
		if !entry.Failed && importErr != nil {
			return fmt.Errorf("setup block %d %s: %w", i, entry.Block, importErr)
		}
		if importErr == nil {
			floor = s.timeMs
		}
	}
	s.minTimeMs = floor
	return nil
}

func (s *gossipFixtureStore) tick(timeMs uint64) {
	timeMs = max(timeMs, s.minTimeMs)
	if timeMs < s.timeMs {
		s.SetTimeForSpecTest(timeMs)
	} else {
		s.OnTick(timeMs)
	}
	s.timeMs = timeMs
}

func (s gloasGossipServices) process(ctx context.Context, root fs.FS, version clparams.StateVersion, name string) (error, error) {
	compressed, err := fs.ReadFile(root, name+".ssz_snappy")
	if err != nil {
		return nil, err
	}
	data, err := utils.DecompressSnappy(compressed, false)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.HasPrefix(name, "execution_payload_envelope_"):
		return processGossip(ctx, s.envelope, data, version), nil
	case strings.HasPrefix(name, "payload_attestation_message_"):
		return processGossip(ctx, s.attestation, data, version), nil
	case strings.HasPrefix(name, "proposer_preferences_"):
		return processGossip(ctx, s.preferences, data, version), nil
	default:
		return nil, fmt.Errorf("unsupported gossip message file %s", name)
	}
}

func processGossip[T any](ctx context.Context, service serviceinterface.Service[T], data []byte, version clparams.StateVersion) error {
	msg, err := service.DecodeGossipMessage("", data, version)
	if err != nil {
		return err
	}
	return service.ProcessMessage(ctx, nil, msg)
}

func gossipVerdict(err error) string {
	if err == nil {
		return "valid"
	}
	if errors.Is(err, synced_data.ErrNotSynced) || strings.Contains(err.Error(), "ignore") {
		return "ignore"
	}
	return "reject"
}

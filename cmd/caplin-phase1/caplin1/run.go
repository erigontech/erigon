package caplin1

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"
	"github.com/spf13/afero"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

func RunCaplinPhase1(ctx context.Context, sentinel sentinel.SentinelClient, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig,
	engine execution_client.ExecutionEngine, state *state.CachingBeaconState,
	caplinFreezer freezer.Freezer, datadir string) error {
	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, genesisConfig)

	logger := log.New("app", "caplin")

	if caplinFreezer != nil {
		if err := freezer.PutObjectSSZIntoFreezer("beaconState", "caplin_core", 0, state, caplinFreezer); err != nil {
			return err
		}
	}
	forkChoice, err := forkchoice.NewForkChoiceStore(state, engine, caplinFreezer, true)
	if err != nil {
		logger.Error("Could not create forkchoice", "err", err)
		return err
	}
	bls.SetEnabledCaching(true)
	state.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		pk := v.PublicKey()
		if err := bls.LoadPublicKeyIntoCache(pk[:], false); err != nil {
			panic(err)
		}
		return true
	})
	gossipManager := network.NewGossipReceiver(ctx, sentinel, forkChoice, beaconConfig, genesisConfig, caplinFreezer)
	dataDirFs := afero.NewBasePathFs(afero.NewOsFs(), datadir)

	{ // start the gossip manager
		go gossipManager.Start()
		logger.Info("Started Ethereum 2.0 Gossip Service")
	}

	{ // start logging peers
		go func() {
			logIntervalPeers := time.NewTicker(1 * time.Minute)
			for {
				select {
				case <-logIntervalPeers.C:
					if peerCount, err := beaconRpc.Peers(); err == nil {
						logger.Info("P2P", "peers", peerCount)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	{ // start ticking forkChoice
		go func() {
			tickInterval := time.NewTicker(50 * time.Millisecond)
			for {
				select {
				case <-tickInterval.C:
					forkChoice.OnTick(uint64(time.Now().Unix()))
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// start the downloader service
	//go initDownloader(beaconRpc, genesisConfig, beaconConfig, state, nil, gossipManager, forkChoice, caplinFreezer, dataDirFs)

	forkChoiceConfig := stages.CaplinStagedSync(nil, beaconRpc, genesisConfig, beaconConfig, state, nil, gossipManager, forkChoice, caplinFreezer, dataDirFs)
	sync := stagedsync.New(
		stages.ConsensusStages(
			ctx,
			forkChoiceConfig,
		),
		nil,
		nil,
		logger,
	)
	db := memdb.New(os.TempDir())
	txn, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	var notFirst atomic.Bool
	for {
		err = sync.Run(db, txn, notFirst.CompareAndSwap(false, true))
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			logger.Error("Caplin Sync Stage Fail", "err", err.Error())
		}
	}
	return err
}

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

package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"github.com/erigontech/erigon/cmd/erigon/node"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// exitDiverged is the exit status of a follower that halted because its
// chain contradicts canonical L1.
const exitDiverged = 3

// exitError carries the exit status of a halted follower; the CLI exits
// with it.
type exitError struct {
	error
	code int
}

func (e exitError) ExitCode() int { return e.code }

// haltError is the error a node exits with after its follower halted.
func haltError(err error) error {
	if errors.Is(err, deriver.ErrDiverged) {
		return exitError{err, exitDiverged}
	}
	return err
}

// eezDataDir is where an EEZ chain keeps its data under datadir.
func eezDataDir(datadir string) string { return filepath.Join(datadir, "eez") }

// configureFollowerNode moves the node to <datadir>/eez and off the P2P
// network: the follower is the chain's only source of blocks.
func configureFollowerNode(cfg *nodecfg.Config) error {
	if slices.Contains(cfg.Http.API, "testing") {
		return fmt.Errorf("%w: the testing API builds blocks; the follower must be the chain's only driver", eez.ErrConfig)
	}
	cfg.Dirs = datadir.New(eezDataDir(cfg.Dirs.DataDir))
	dir.MustExist(cfg.Dirs.Log)
	cfg.P2P.MaxPeers = 0
	cfg.P2P.NoDiscovery = true
	return nil
}

// configureFollowerEth turns off every other driver and source of the
// chain: consensus layer, engine API, txpool and snapshot downloader.
func configureFollowerEth(cfg *ethconfig.Config) {
	cfg.InternalCL = false
	cfg.NoEngineAPI = true
	cfg.TxPool.Disable = true
	cfg.Snapshot.NoDownloader = true
}

// startFollower registers the follower of the node's EEZ chain. halted
// receives the error it stops with on its own, after which the node closes.
func startFollower(ctx context.Context, n *node.ErigonNode, cfg *eez.Config, dirs datadir.Dirs, logger log.Logger) (*follower.Service, error) {
	backend := n.Backend()
	chainConfig := backend.ChainConfig()
	if err := cfg.L2.Check(chainConfig); err != nil {
		return nil, err
	}
	db, ok := backend.ChainDB().(kv.TemporalRwDB)
	if !ok {
		return nil, fmt.Errorf("chain database %T is not temporal", backend.ChainDB())
	}
	engine, err := eezrules.New(chainConfig)
	if err != nil {
		return nil, err
	}
	blockReader, _ := backend.BlockIO()
	chain := deriver.NewExecChain(backend.ExecutionModule(), db, blockReader,
		receipts.NewGenerator(dirs, blockReader, engine, nil, time.Minute), chainConfig)
	reader, err := l1.DialRPC(ctx, cfg.L1RPC, logger)
	if err != nil {
		return nil, err
	}
	fc := follower.DefaultConfig
	fc.Dir, fc.Poll, fc.ReorgDepth, fc.LogChunk = dirs.DataDir, cfg.L1Poll, cfg.L1ReorgDepth, cfg.L1LogChunk
	f, err := follower.New(reader, chain, chainConfig, fc, logger)
	if err != nil {
		return nil, err
	}
	svc := follower.NewService(f, func(err error) {
		logger.Error("[eez] follower halted, stopping the node", "err", err)
		go n.Close()
	})
	n.RegisterLifecycle(svc)
	logger.Info("[eez] follower mode", "datadir", dirs.DataDir, "l1", cfg.L1RPC)
	return svc, nil
}

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

package commands

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/state/execctx"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	erigoncli "github.com/erigontech/erigon/node/cli"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains
)

func init() {
	withDataDir(readDomains)
	withChain(readDomains)
	withHeimdall(readDomains)
	withWorkers(readDomains)
	withStartTx(readDomains)

	rootCmd.AddCommand(readDomains)
}

// if trie variant is not hex, we could not have another rootHash with to verify it
var (
	stepSize uint64
	lastStep uint64
)

// write command to just seek and query state by addr and domain from state db and files (if any)
var readDomains = &cobra.Command{
	Use:       "read_domains",
	Short:     `Run block execution and commitment with Domains.`,
	Example:   "go run ./cmd/integration read_domains --datadir=... --verbosity=3",
	ValidArgs: []string{"account", "storage", "code", "commitment"},
	Args:      cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx := cmd.Context()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults

		spec, err := chainspec.ChainSpecByName(chain)
		if err != nil {
			utils.Fatalf("unknown chain %s", chain)
		}
		ethConfig.Genesis = spec.Genesis
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)

		var readFromDomain string
		var addrs [][]byte
		for i := 0; i < len(args); i++ {
			if i == 0 {
				switch s := strings.ToLower(args[i]); s {
				case "account", "storage", "code", "commitment":
					readFromDomain = s
				default:
					logger.Error("invalid domain to read from", "arg", args[i])
					return
				}
				continue
			}
			addr, err := hex.DecodeString(strings.TrimPrefix(args[i], "0x"))
			if err != nil {
				logger.Warn("invalid address passed", "str", args[i], "at position", i, "err", err)
				continue
			}
			addrs = append(addrs, addr)
		}

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		stateDb, err := mdbx.New(dbcfg.ChainDB, log.New()).Path(filepath.Join(dirs.DataDir, "statedb")).WriteMap(true).Open(ctx)
		if err != nil {
			return
		}
		defer stateDb.Close()

		if err := requestDomains(chainDb, stateDb, ctx, readFromDomain, addrs, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func requestDomains(chainDb, stateDb kv.RwDB, ctx context.Context, readDomain string, addrs [][]byte, logger log.Logger) error {
	stateTx, err := stateDb.BeginRw(ctx)
	must(err)
	defer stateTx.Rollback()
	temporalTx, ok := stateTx.(kv.TemporalTx)
	if !ok {
		return errors.New("stateDb transaction is not a temporal transaction")
	}
	domains, err := execctx.NewSharedDomains(ctx, temporalTx, logger)
	if err != nil {
		return err
	}

	r := state.NewReaderV3(domains.AsGetter(temporalTx))
	latestTx, latestBlock, err := domains.SeekCommitment(ctx, temporalTx)
	if err != nil {
		return fmt.Errorf("failed to seek commitment to txn %d: %w", startTxNum, err)
	}
	if latestTx < startTxNum {
		return fmt.Errorf("latest available txn to start is  %d and its less than start txn %d", latestTx, startTxNum)
	}
	logger.Info("seek commitment", "block", latestBlock, "tx", latestTx)

	switch readDomain {
	case kv.AccountsDomain.String():
		for _, addr := range addrs {

			acc, err := r.ReadAccountData(accounts.InternAddress(common.BytesToAddress(addr)))
			if err != nil {
				logger.Error("failed to read account", "addr", addr, "err", err)
				continue
			}
			logger.Info(fmt.Sprintf("%x: nonce=%d balance=%d code=%x root=%x", addr, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash, acc.Root))
		}
	case kv.StorageDomain.String():
		for _, addr := range addrs {
			a, s := accounts.InternAddress(common.BytesToAddress(addr[:length.Addr])), accounts.InternKey(common.BytesToHash(addr[length.Addr:]))
			st, _, err := r.ReadAccountStorage(a, s)
			if err != nil {
				logger.Error("failed to read storage", "addr", a.String(), "key", s.String(), "err", err)
				continue
			}
			logger.Info(fmt.Sprintf("%s %s -> %x", a.String(), s.String(), st))
		}
	case kv.CodeDomain.String():
		for _, addr := range addrs {
			code, err := r.ReadAccountCode(accounts.InternAddress(common.BytesToAddress(addr)))
			if err != nil {
				logger.Error("failed to read code", "addr", addr, "err", err)
				continue
			}
			logger.Info(fmt.Sprintf("%s: %x", addr, code))
		}
	}
	return nil
}

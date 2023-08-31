package commands

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/state/temporal"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/debug"
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
		ctx, _ := libcommon.RootContext()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults
		ethConfig.Genesis = core.GenesisBlockByChainName(chain)
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
		chainDb, err := openDB(dbCfg(kv.ChainDB, dirs.Chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		stateDb, err := kv2.NewMDBX(log.New()).Path(filepath.Join(dirs.DataDir, "statedb")).WriteMap().Open()
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
	sn, bsn, agg := allSnapshots(ctx, chainDb, logger)
	defer sn.Close()
	defer bsn.Close()
	defer agg.Close()

	ac := agg.MakeContext()
	defer ac.Close()

	domains := agg.SharedDomains(ac)
	defer domains.Close()

	stateTx, err := stateDb.BeginRw(ctx)
	must(err)
	defer stateTx.Rollback()

	domains.SetTx(stateTx)

	//defer agg.StartWrites().FinishWrites()

	r := state.NewReaderV4(stateTx.(*temporal.Tx))
	//w := state.NewWriterV4(stateTx.(*temporal.Tx))

	latestBlock, latestTx, err := domains.SeekCommitment(0, math.MaxUint64)
	if err != nil && startTxNum != 0 {
		return fmt.Errorf("failed to seek commitment to tx %d: %w", startTxNum, err)
	}
	if latestTx < startTxNum {
		return fmt.Errorf("latest available tx to start is  %d and its less than start tx %d", latestTx, startTxNum)
	}
	logger.Info("seek commitment", "block", latestBlock, "tx", latestTx)

	switch readDomain {
	case "account":
		for _, addr := range addrs {

			acc, err := r.ReadAccountData(libcommon.BytesToAddress(addr))
			if err != nil {
				logger.Error("failed to read account", "addr", addr, "err", err)
				continue
			}
			fmt.Printf("%x: nonce=%d balance=%d code=%x root=%x\n", addr, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash, acc.Root)
		}
	case "storage":
		for _, addr := range addrs {
			a, s := libcommon.BytesToAddress(addr[:length.Addr]), libcommon.BytesToHash(addr[length.Addr:])
			st, err := r.ReadAccountStorage(a, 0, &s)
			if err != nil {
				logger.Error("failed to read storage", "addr", a.String(), "key", s.String(), "err", err)
				continue
			}
			fmt.Printf("%s %s -> %x\n", a.String(), s.String(), st)
		}
	case "code":
		for _, addr := range addrs {
			code, err := r.ReadAccountCode(libcommon.BytesToAddress(addr), 0, libcommon.Hash{})
			if err != nil {
				logger.Error("failed to read code", "addr", addr, "err", err)
				continue
			}
			fmt.Printf("%s: %x\n", addr, code)
		}
	}
	return nil
}

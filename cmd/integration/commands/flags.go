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
	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/turbo/cli"
)

var (
	chaindata                    string
	databaseVerbosity            int
	referenceChaindata           string
	block, pruneTo, unwind       uint64
	unwindEvery                  uint64
	batchSizeStr                 string
	domain                       string
	reset, noCommit, squeeze     bool
	bucket                       string
	datadirCli, toChaindata      string
	migration                    string
	integrityFast, integritySlow bool
	file                         string
	HeimdallURL                  string
	txtrace                      bool // Whether to trace the execution (should only be used together with `block`)
	unwindTypes                  []string
	chain                        string // Which chain to use (mainnet, sepolia, etc.)
	outputCsvFile                string

	startTxNum uint64

	dbWriteMap bool

	chainTipMode bool
	syncCfg      = ethconfig.Defaults.Sync
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withConfig(cmd *cobra.Command) {
	cmd.Flags().String("config", "", "yaml/toml config file location")
}

func withMining(cmd *cobra.Command) {
	cmd.Flags().Bool("mine", false, "Enable mining")
	cmd.Flags().StringArray("miner.notify", nil, "Comma separated HTTP URL list to notify of new work packages")
	cmd.Flags().Uint64("miner.gaslimit", ethconfig.DefaultBlockGasLimit, "Target gas limit for mined blocks")
	cmd.Flags().Int64("miner.gasprice", ethconfig.Defaults.Miner.GasPrice.Int64(), "Target gas price for mined blocks")
	cmd.Flags().String("miner.etherbase", "0", "Public address for block mining rewards (default = first account")
	cmd.Flags().String("miner.extradata", "", "Block extra data set by the miner (default = client version)")
	cmd.Flags().Duration("miner.recommit", ethconfig.Defaults.Miner.Recommit, "Time interval to recreate the block being mined")
	cmd.Flags().Bool("miner.noverify", false, "Disable remote sealing verification")
}

func withFile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&file, "file", "", "path to file")
	must(cmd.MarkFlagFilename("file"))
	must(cmd.MarkFlagRequired("file"))
}

func withReferenceChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&referenceChaindata, "chaindata.reference", "", "path to the 2nd (reference/etalon) db")
	must(cmd.MarkFlagDirname("chaindata.reference"))
}

func withToChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&toChaindata, "chaindata.to", "", "target chaindata")
	must(cmd.MarkFlagDirname("chaindata.to"))
}

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 0, "block test at this block")
}

func withUnwind(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwind, "unwind", 0, "how much blocks unwind on each iteration")
}
func withNoCommit(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&noCommit, "no-commit", false, "run everything in 1 transaction, but doesn't commit it")
}

func withPruneTo(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&pruneTo, "prune.to", 0, "how much blocks unwind on each iteration")
}

func withUnwindEvery(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwindEvery, "unwind.every", 0, "each iteration test will move forward `--unwind.every` blocks, then unwind `--unwind` blocks")
}

func withReset(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&reset, "reset", false, "reset given stage")
}

func withSqueeze(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&squeeze, "squeeze", true, "use offset-pointers from commitment.kv to account.kv")
}

func withBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&bucket, "bucket", "", "reset given stage")
}

func withDataDir2(cmd *cobra.Command) {
	// --datadir is required, but no --chain flag: read chainConfig from db instead
	cmd.Flags().StringVar(&datadirCli, utils.DataDirFlag.Name, "", utils.DataDirFlag.Usage)
	must(cmd.MarkFlagDirname(utils.DataDirFlag.Name))
	must(cmd.MarkFlagRequired(utils.DataDirFlag.Name))
	cmd.Flags().IntVar(&databaseVerbosity, "database.verbosity", 2, "Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning.")

	cmd.Flags().BoolVar(&dbWriteMap, utils.DbWriteMapFlag.Name, utils.DbWriteMapFlag.Value, utils.DbWriteMapFlag.Usage)
}

func withDataDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadirCli, "datadir", "", "data directory for temporary ELT files")
	must(cmd.MarkFlagRequired("datadir"))
	must(cmd.MarkFlagDirname("datadir"))

	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))

	cmd.Flags().IntVar(&databaseVerbosity, "database.verbosity", 2, "Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning")

	cmd.Flags().BoolVar(&dbWriteMap, utils.DbWriteMapFlag.Name, utils.DbWriteMapFlag.Value, utils.DbWriteMapFlag.Usage)
}

func withConcurrentCommitment(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&statecfg.ExperimentalConcurrentCommitment, utils.ExperimentalConcurrentCommitmentFlag.Name, utils.ExperimentalConcurrentCommitmentFlag.Value, utils.ExperimentalConcurrentCommitmentFlag.Usage)
}

func withBatchSize(cmd *cobra.Command) {
	cmd.Flags().StringVar(&batchSizeStr, "batchSize", cli.BatchSizeFlag.Value, cli.BatchSizeFlag.Usage)
}

func withDomain(cmd *cobra.Command) {
	cmd.Flags().StringVar(&domain, "domain", "", "Comma separated names of domain/inverted_indices")
}

func withIntegrityChecks(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&integritySlow, "integrity.slow", false, "enable slow data-integrity checks")
	cmd.Flags().BoolVar(&integrityFast, "integrity.fast", false, "enable fast data-integrity checks")
}

func withMigration(cmd *cobra.Command) {
	cmd.Flags().StringVar(&migration, "migration", "", "action to apply to given migration")
}

func withTxTrace(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&txtrace, "txtrace", false, "enable tracing of transactions")
}

func withChain(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chain, "chain", "", "pick a chain to assume (mainnet, sepolia, etc.)")
	must(cmd.MarkFlagRequired("chain"))
}

func withHeimdall(cmd *cobra.Command) {
	cmd.Flags().StringVar(&HeimdallURL, "bor.heimdall", "http://localhost:1317", "URL of Heimdall service")
}

func withWorkers(cmd *cobra.Command) {
	cmd.Flags().IntVar(&syncCfg.ExecWorkerCount, "exec.workers", ethconfig.Defaults.Sync.ExecWorkerCount, "")
}

func withStartTx(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&startTxNum, "tx", 0, "start processing from tx")
}

func withOutputCsvFile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&outputCsvFile, "output.csv.file", "", "location to output csv data")
}

func withUnwindTypes(cmd *cobra.Command) {
	cmd.Flags().StringSliceVar(&unwindTypes, "unwind.types", nil, "types to unwind for polygon sync")
}

func withChaosMonkey(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&syncCfg.ChaosMonkey, utils.ChaosMonkeyFlag.Name, utils.ChaosMonkeyFlag.Value, utils.ChaosMonkeyFlag.Usage)
}
func withChainTipMode(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&chainTipMode, "sync.mode.chaintip", false, "Every block does: `CalcCommitment`, `rwtx.Commit()`, generate diffs/changesets. Also can use it to generate diffs before `integration loop_exec`")
}

package commands

import (
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
)

var (
	chaindata                      string
	databaseVerbosity              int
	snapshotMode, snapshotDir      string
	referenceChaindata             string
	block, pruneTo, unwind         uint64
	unwindEvery                    uint64
	batchSizeStr                   string
	reset                          bool
	bucket                         string
	datadir, toChaindata           string
	migration                      string
	integrityFast, integritySlow   bool
	file                           string
	txtrace                        bool // Whether to trace the execution (should only be used together eith `block`)
	pruneFlag                      string
	pruneH, pruneR, pruneT, pruneC uint64
	experiments                    []string
	chain                          string // Which chain to use (mainnet, ropsten, rinkeby, goerli, etc.)
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withMining(cmd *cobra.Command) {
	cmd.Flags().Bool("mine", false, "Enable mining")
	cmd.Flags().StringArray("miner.notify", nil, "Comma separated HTTP URL list to notify of new work packages")
	cmd.Flags().Uint64("miner.gastarget", ethconfig.Defaults.Miner.GasFloor, "Target gas floor for mined blocks")
	cmd.Flags().Uint64("miner.gaslimit", ethconfig.Defaults.Miner.GasCeil, "Target gas ceiling for mined blocks")
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

func withPruneTo(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&pruneTo, "prune.to", 0, "how much blocks unwind on each iteration")
}

func withUnwindEvery(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwindEvery, "unwind.every", 0, "each iteration test will move forward `--unwind.every` blocks, then unwind `--unwind` blocks")
}

func withReset(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&reset, "reset", false, "reset given stage")
}

func withBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&bucket, "bucket", "", "reset given stage")
}

func withDatadir2(cmd *cobra.Command) {
	cmd.Flags().String(utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	must(cmd.MarkFlagDirname(utils.DataDirFlag.Name))
	must(cmd.MarkFlagRequired(utils.DataDirFlag.Name))
	cmd.Flags().IntVar(&databaseVerbosity, "database.verbosity", 2, "Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning.")
}

func withDatadir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadir, "datadir", paths.DefaultDataDir(), "data directory for temporary ELT files")
	must(cmd.MarkFlagDirname("datadir"))

	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))

	cmd.Flags().StringVar(&snapshotMode, "snapshot.mode", "", "set of snapshots to use")
	cmd.Flags().StringVar(&snapshotDir, "snapshot.dir", "", "snapshot dir")
	must(cmd.MarkFlagDirname("snapshot.dir"))

	cmd.Flags().IntVar(&databaseVerbosity, "database.verbosity", 2, "Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning")
}

func withBatchSize(cmd *cobra.Command) {
	cmd.Flags().StringVar(&batchSizeStr, "batchSize", "512M", "batch size for execution stage")
}

func withIntegrityChecks(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&integritySlow, "integrity.slow", false, "enable slow data-integrity checks")
	cmd.Flags().BoolVar(&integrityFast, "integrity.fast", true, "enable fast data-integrity checks")
}

func withMigration(cmd *cobra.Command) {
	cmd.Flags().StringVar(&migration, "migration", "", "action to apply to given migration")
}

func withTxTrace(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&txtrace, "txtrace", false, "enable tracing of transactions")
}

func withChain(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chain, "chain", "", "pick a chain to assume (mainnet, ropsten, etc.)")
}

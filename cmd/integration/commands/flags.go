package commands

import (
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/paths"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
)

var (
	chaindata          string
	database           string
	snapshotMode       string
	snapshotDir        string
	toChaindata        string
	referenceChaindata string
	block              uint64
	unwind             uint64
	unwindEvery        uint64
	cacheSizeStr       string
	batchSizeStr       string
	reset              bool
	bucket             string
	datadir            string
	mapSizeStr         string
	migration          string
	integritySlow      bool
	integrityFast      bool
	silkwormPath       string
	file               string
	txtrace            bool // Whether to trace the execution (should only be used together eith `block`)
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
	cmd.Flags().StringVar(&database, "database", "", "lmdb|mdbx")
}

func withDatadir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadir, "datadir", paths.DefaultDataDir(), "data directory for temporary ELT files")
	must(cmd.MarkFlagDirname("datadir"))
	cmd.Flags().StringVar(&mapSizeStr, "lmdb.mapSize", "", "map size for LMDB")

	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))

	cmd.Flags().StringVar(&snapshotMode, "snapshot.mode", "", "set of snapshots to use")
	cmd.Flags().StringVar(&snapshotDir, "snapshot.dir", "", "snapshot dir")
	must(cmd.MarkFlagDirname("snapshot.dir"))

	cmd.Flags().StringVar(&database, "database", "", "lmdb|mdbx")
}

func withBatchSize(cmd *cobra.Command) {
	cmd.Flags().StringVar(&cacheSizeStr, "cacheSize", "0", "cache size for execution stage")
	cmd.Flags().StringVar(&batchSizeStr, "batchSize", "512M", "batch size for execution stage")
}

func withIntegrityChecks(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&integritySlow, "integrity.slow", true, "enable slow data-integrity checks")
	cmd.Flags().BoolVar(&integrityFast, "integrity.fast", true, "enable fast data-integrity checks")
}

func withMigration(cmd *cobra.Command) {
	cmd.Flags().StringVar(&migration, "migration", "", "action to apply to given migration")
}

func withSilkworm(cmd *cobra.Command) {
	cmd.Flags().StringVar(&silkwormPath, "silkworm", "", "file path of libsilkworm_tg_api.so")
	must(cmd.MarkFlagFilename("silkworm"))
}

func withTxTrace(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&txtrace, "txtrace", false, "enable tracing of transactions")
}

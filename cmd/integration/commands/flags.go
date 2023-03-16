package commands

import (
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/turbo/cli"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
)

var (
	chaindata                      string
	databaseVerbosity              int
	referenceChaindata             string
	block, pruneTo, unwind         uint64
	unwindEvery                    uint64
	batchSizeStr                   string
	reset, warmup                  bool
	bucket                         string
	datadirCli, toChaindata        string
	migration                      string
	integrityFast, integritySlow   bool
	file                           string
	HeimdallgRPCAddress            string
	HeimdallURL                    string
	txtrace                        bool // Whether to trace the execution (should only be used together with `block`)
	pruneFlag                      string
	pruneH, pruneR, pruneT, pruneC uint64
	pruneHBefore, pruneRBefore     uint64
	pruneTBefore, pruneCBefore     uint64
	experiments                    []string
	chain                          string // Which chain to use (mainnet, rinkeby, goerli, etc.)

	commitmentMode string
	commitmentTrie string
	commitmentFreq int
	startTxNum     uint64
	traceFromTx    uint64

	_forceSetHistoryV3    bool
	workers, reconWorkers uint64
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
	cmd.Flags().Uint64("miner.gaslimit", ethconfig.Defaults.Miner.GasLimit, "Target gas limit for mined blocks")
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
	cmd.Flags().BoolVar(&warmup, "warmup", false, "warmup relevant tables by parallel random reads")
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
}

func withDataDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadirCli, "datadir", "", "data directory for temporary ELT files")
	must(cmd.MarkFlagRequired("datadir"))
	must(cmd.MarkFlagDirname("datadir"))

	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))

	cmd.Flags().IntVar(&databaseVerbosity, "database.verbosity", 2, "Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning")
}

func withBatchSize(cmd *cobra.Command) {
	cmd.Flags().StringVar(&batchSizeStr, "batchSize", cli.BatchSizeFlag.Value, cli.BatchSizeFlag.Usage)
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
	cmd.Flags().StringVar(&chain, "chain", "mainnet", "pick a chain to assume (mainnet, sepolia, etc.)")
	must(cmd.MarkFlagRequired("chain"))
}

func withHeimdall(cmd *cobra.Command) {
	cmd.Flags().StringVar(&HeimdallURL, "bor.heimdall", "http://localhost:1317", "URL of Heimdall service")
}

func withWorkers(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&workers, "exec.workers", uint64(ethconfig.Defaults.Sync.ExecWorkerCount), "")
	cmd.Flags().Uint64Var(&reconWorkers, "recon.workers", uint64(ethconfig.Defaults.Sync.ReconWorkerCount), "")
}

func withStartTx(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&startTxNum, "startTx", 0, "start processing from tx")
}

func withTraceFromTx(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&traceFromTx, "txtrace.from", 0, "start tracing from tx number")
}

func withCommitment(cmd *cobra.Command) {
	cmd.Flags().StringVar(&commitmentMode, "commitment.mode", "direct", "defines the way to calculate commitments: 'direct' mode reads from state directly, 'update' accumulate updates before commitment, 'off' actually disables commitment calculation")
	cmd.Flags().StringVar(&commitmentTrie, "commitment.trie", "hex", "hex - use Hex Patricia Hashed Trie for commitments, bin - use of binary patricia trie")
	cmd.Flags().IntVar(&commitmentFreq, "commitment.freq", 25000, "how many blocks to skip between calculating commitment")
}

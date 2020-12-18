package commands

import (
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/spf13/cobra"
)

var (
	chaindata          string
	database           string
	snapshotMode       string
	snapshotDir        string
	compact            bool
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
	freelistReuse      int
	migration          string
	silkwormPath       string
	file               string
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))
	must(cmd.MarkFlagRequired("chaindata"))
	cmd.Flags().StringVar(&snapshotMode, "snapshotMode", "", "set of snapshots to use")
	cmd.Flags().StringVar(&snapshotDir, "snapshotDir", "", "snapshot dir")
	cmd.Flags().StringVar(&database, "database", "", "lmdb|mdbx")
}

func withFile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&file, "file", "", "path to file")
	must(cmd.MarkFlagFilename("file"))
	must(cmd.MarkFlagRequired("file"))
}

func withLmdbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&mapSizeStr, "lmdb.mapSize", "", "map size for LMDB")
	cmd.Flags().IntVar(&freelistReuse, "maxFreelistReuse", 0, "Find a big enough contiguous page range for large values in freelist is hard just allocate new pages and even don't try to search if value is bigger than this limit. Measured in pages.")
}

func withCompact(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&compact, "compact", false, "compact db file. if remove much data form LMDB it slows down tx.Commit because it performs `realloc()` of free_list every commit")
}

func withReferenceChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&referenceChaindata, "reference_chaindata", "", "path to the 2nd (reference/etalon) db")
	must(cmd.MarkFlagDirname("reference_chaindata"))
}

func withToChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&toChaindata, "to_chaindata", "", "target chaindata")
	must(cmd.MarkFlagDirname("to_chaindata"))
}

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 0, "block test at this block")
}

func withUnwind(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwind, "unwind", 0, "how much blocks unwind on each iteration")
}

func withUnwindEvery(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwindEvery, "unwind_every", 0, "each iteration test will move forward `--unwind_every` blocks, then unwind `--unwind` blocks")
}

func withReset(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&reset, "reset", false, "reset given stage")
}

func withBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&bucket, "bucket", "", "reset given stage")
}

func withDatadir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadir, "datadir", node.DefaultDataDir(), "data directory for temporary ELT files")
}

func withBatchSize(cmd *cobra.Command) {
	cmd.Flags().StringVar(&cacheSizeStr, "cacheSize", "0", "cache size for execution stage")
	cmd.Flags().StringVar(&batchSizeStr, "batchSize", "512M", "batch size for execution stage")
}

func withMigration(cmd *cobra.Command) {
	cmd.Flags().StringVar(&migration, "migration", "", "action to apply to given migration")
}

func withSilkworm(cmd *cobra.Command) {
	cmd.Flags().StringVar(&silkwormPath, "silkworm", "", "file path of libsilkworm_tg_api.so")
	must(cmd.MarkFlagFilename("silkworm"))
}

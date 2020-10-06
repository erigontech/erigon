package commands

import (
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/spf13/cobra"
)

var (
	chaindata          string
	snapshotMode       string
	snapshotDir        string
	compact            bool
	referenceChaindata string
	block              uint64
	unwind             uint64
	unwindEvery        uint64
	hdd                bool
	reset              bool
	bucket             string
	datadir            string
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
}

func withCompact(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&compact, "compact", false, "compact db file. if remove much data form LMDB it slows down tx.Commit because it performs `realloc()` of free_list every commit")
}

func withReferenceChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&referenceChaindata, "reference_chaindata", "", "path to the 2nd (reference/etalon) db")
	must(cmd.MarkFlagDirname("reference_chaindata"))
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

func withHDD(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&hdd, "hdd", false, "optimizations valuable for HDD")
}

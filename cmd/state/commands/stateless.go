package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/spf13/cobra"
)

var (
	statefile        string
	triesize         uint32
	preroot          bool
	snapshotInterval uint64
	snapshotFrom     uint64
	witnessInterval  uint64
	noverify         bool
	bintries         bool
)

func init() {
	withChaindata(statelessCmd)
	withStatsfile(statelessCmd)
	withBlock(statelessCmd)

	statelessCmd.Flags().StringVar(&statefile, "statefile", "state", "path to the file where the state will be periodically written during the analysis")
	statelessCmd.Flags().Uint32Var(&triesize, "triesize", 1024*1024, "maximum number of nodes in the state trie")
	statelessCmd.Flags().BoolVar(&preroot, "preroot", false, "Attempt to compute hash of the trie without modifying it")
	statelessCmd.Flags().Uint64Var(&snapshotInterval, "snapshotInterval", 0, "how often to take snapshots (0 - never, 1 - every block, 1000 - every 1000th block, etc)")
	statelessCmd.Flags().Uint64Var(&snapshotFrom, "snapshotFrom", 0, "from which block to start snapshots")
	statelessCmd.Flags().Uint64Var(&witnessInterval, "witnessInterval", 1, "after which block to extract witness (put a large number like 10000000 to disable)")
	statelessCmd.Flags().BoolVar(&noverify, "noVerify", false, "skip snapshot verification on loading")
	statelessCmd.Flags().BoolVar(&bintries, "bintries", false, "use binary tries instead of hexary to generate/load block witnesses")

	rootCmd.AddCommand(statelessCmd)

}

var statelessCmd = &cobra.Command{
	Use:   "stateless",
	Short: "Stateless Ethereum prototype",
	RunE: func(cmd *cobra.Command, args []string) error {

		createDb := func(path string) (ethdb.Database, error) {
			return ethdb.NewBoltDatabase(path)
		}

		stateless.Stateless(
			block,
			chaindata,
			statefile,
			triesize,
			preroot,
			snapshotInterval,
			snapshotFrom,
			witnessInterval,
			statsfile,
			!noverify,
			bintries,
			createDb,
		)

		return nil
	},
}

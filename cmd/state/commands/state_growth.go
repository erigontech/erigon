package commands

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"

	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/spf13/cobra"
)

func init() {
	stateGrowthCmd := &cobra.Command{
		Use:   "stateGrowth",
		Short: "stateGrowth",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			localDB := ethdb.NewLMDB().Path(file() + "_sg").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
				return dbutils.BucketsCfg{
					stateless.MainHashesBucket:      {},
					stateless.ReportsProgressBucket: {},
				}
			}).MustOpen()

			remoteDB, _, err := ethdb.NewRemote().Path(privateApiAddr).Open("", "", "")
			if err != nil {
				return err
			}

			fmt.Println("Processing started...")
			stateless.NewStateGrowth1Reporter(ctx, remoteDB, localDB).StateGrowth1(ctx)
			stateless.NewStateGrowth2Reporter(ctx, remoteDB, localDB).StateGrowth2(ctx)
			return nil
		},
	}

	withPrivateApi(stateGrowthCmd)
	rootCmd.AddCommand(stateGrowthCmd)
}

// Generate name off the file for snapshot
// Each day has it's own partition
// It means that you can only continue execution of report from last snapshot.Save() checkpoint - read buckets forward from last key
// But not re-read bucket
func file() string {
	dir := path.Join(os.TempDir(), "turbo_geth_reports")
	if err := os.MkdirAll(dir, 0770); err != nil {
		panic(err)
	}
	return path.Join(dir, time.Now().Format("2006-01-02"))
}

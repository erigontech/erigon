package commands

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(gasLimitsCmd)
	withRemoteDb(gasLimitsCmd)
	rootCmd.AddCommand(gasLimitsCmd)
}

var gasLimitsCmd = &cobra.Command{
	Use:   "gasLimits",
	Short: "gasLimits",
	RunE: func(cmd *cobra.Command, args []string) error {
		dial := func(ctx context.Context) (in io.Reader, out io.Writer, closer io.Closer, err error) {
			dialer := net.Dialer{}
			conn, err := dialer.DialContext(ctx, "tcp", remoteDbAddress)
			return conn, conn, conn, err
		}

		db, err := remote.NewDB(dial)
		if err != nil {
			return err
		}

		ctx, _ := getContext()
		fmt.Println("Processing started...")

		stateless.GasLimits(ctx, db)
		return nil
	},
}

package app

import (
	"fmt"
	"os"
	"runtime"

	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/urfave/cli/v2"
)

var (
	preverifiedFlag = cli.StringFlag{
		Name:     "preverified",
		Category: "Snapshots",
		Usage:    "preverified to use (remote, local, embedded)",
		Value:    "remote",
	}
	concurrencyFlag = cli.IntFlag{
		Name:  "concurrency",
		Usage: "level of concurrency for some operation",
		Value: runtime.GOMAXPROCS(0),
	}
	verifyChainFlag = cli.StringFlag{
		Name:  "verify.chain",
		Usage: "name of the chain to verify",
	}
)

func handlePreverifiedFlag(cliCtx *cli.Context, dirs *datadir.Dirs) (err error) {
	switch value := preverifiedFlag.Get(cliCtx); value {
	case "local":
		panicif.Err(os.Setenv(snapcfg.RemotePreverifiedEnvKey, dirs.PreverifiedPath()))
		fallthrough
	case "remote":
		err = snapcfg.LoadRemotePreverified(cliCtx.Context)
		if err != nil {
			// TODO: Check if we should continue? What if we ask for a git revision and
			// can't get it? What about a branch? Can we reset to the embedded snapshot hashes?
			return fmt.Errorf("loading remote preverified snapshots: %w", err)
		}
	case "embedded":
		// Should already be loaded.
	default:
		err = fmt.Errorf("invalid preverified flag value %q", value)
		return
	}
	return
}

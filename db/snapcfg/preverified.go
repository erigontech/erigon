package snapcfg

import (
	"context"
	"fmt"
	"os"

	"github.com/anacrolix/missinggo/v2/panicif"

	"github.com/erigontech/erigon/db/datadir"
)

// Loads preverified from locations other than just remote. Usually for utility commands that want
// to test different preverified sources.
//
// forceChainTomlURL is forwarded to LoadRemotePreverified — see its docstring.
func LoadPreverified(ctx context.Context, flagValue string, dirs *datadir.Dirs, chainName string, forceChainTomlURL string) (err error) {
	switch flagValue {
	case "local":
		panicif.Err(os.Setenv(RemotePreverifiedEnvKey, dirs.PreverifiedPath()))
		fallthrough
	case "remote":
		err = LoadRemotePreverified(ctx, chainName, forceChainTomlURL)
		if err != nil {
			// TODO: Check if we should continue? What if we ask for a git revision and
			// can't get it? What about a branch?
			return fmt.Errorf("loading remote preverified snapshots for chain %q: %w", chainName, err)
		}
	default:
		err = fmt.Errorf("invalid preverified flag value %q", flagValue)
		return
	}
	return
}

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
func LoadPreverified(ctx context.Context, flagValue string, dirs *datadir.Dirs) (err error) {
	switch flagValue {
	case "local":
		panicif.Err(os.Setenv(RemotePreverifiedEnvKey, dirs.PreverifiedPath()))
		fallthrough
	case "remote":
		err = LoadRemotePreverified(ctx)
		if err != nil {
			// TODO: Check if we should continue? What if we ask for a git revision and
			// can't get it? What about a branch? Can we reset to the embedded snapshot hashes?
			return fmt.Errorf("loading remote preverified snapshots: %w", err)
		}
	case "embedded":
		// Should already be loaded.
	default:
		err = fmt.Errorf("invalid preverified flag value %q", flagValue)
		return
	}
	return
}

package snapcfg

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/anacrolix/missinggo/v2/panicif"

	"github.com/erigontech/erigon/db/datadir"
)

func PreverifiedItemsForDatadir(dirs *datadir.Dirs, chainName string) (PreverifiedItems, error) {
	data, err := os.ReadFile(dirs.PreverifiedPath())
	if err == nil {
		items, parseErr := ParseToml(data)
		if parseErr != nil {
			return nil, fmt.Errorf("parse manifest: %w", parseErr)
		}
		return items, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	cfg, ok := KnownCfg(chainName)
	if !ok {
		return nil, fmt.Errorf("config for chain %q is not known", chainName)
	}
	return cfg.Preverified.Items, nil
}

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

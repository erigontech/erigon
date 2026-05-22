package app

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	storagesnapshot "github.com/erigontech/erigon/node/components/storage/snapshot"
)

// adoptCliAction promotes snapshot batches that a running node staged
// and validated under --snapshot.adoption-policy=stage|warn but did
// not cut over. Each <datadir>/temp/adoption-* directory holds
// canonical files already validated before the node stopped; this
// renames them over the live snapshots. Run it against a stopped node,
// then restart — the node rescans the snapshot directory on startup.
func adoptCliAction(cliCtx *cli.Context) error {
	logger := log.Root()
	dryRun := dryRunFlag.Get(cliCtx)
	dirs := datadir.Open(cliCtx.String(utils.DataDirFlag.Name))

	recovered, err := storagesnapshot.RecoverStagedAdoptions(dirs.Snap, dirs.Tmp, dryRun, logger)
	if err != nil {
		return err
	}
	if len(recovered) == 0 {
		logger.Info("adopt: no staged batches found")
		return nil
	}
	for _, b := range recovered {
		if dryRun {
			logger.Info("adopt (dry-run): would cut over", "batch", b.Name, "files", len(b.Files))
			for _, f := range b.Files {
				fmt.Printf("  %s\n", f)
			}
		} else {
			logger.Info("adopt: cut over", "batch", b.Name, "files", len(b.Files))
		}
	}
	if !dryRun {
		logger.Info("adopt complete — restart Erigon; the canonical files are now live")
	}
	return nil
}

package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

	entries, err := os.ReadDir(dirs.Tmp)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("adopt: no staged batches found")
			return nil
		}
		return err
	}
	var batches []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "adoption-") {
			batches = append(batches, filepath.Join(dirs.Tmp, e.Name()))
		}
	}
	if len(batches) == 0 {
		logger.Info("adopt: no staged batches found")
		return nil
	}

	for _, batch := range batches {
		files, err := storagesnapshot.CutoverStagedDir(dirs.Snap, batch, dryRun, logger)
		if err != nil {
			return fmt.Errorf("adopt %s: %w", filepath.Base(batch), err)
		}
		if dryRun {
			logger.Info("adopt (dry-run): would cut over", "batch", filepath.Base(batch), "files", len(files))
			for _, f := range files {
				fmt.Printf("  %s\n", f)
			}
		} else {
			logger.Info("adopt: cut over", "batch", filepath.Base(batch), "files", len(files))
		}
	}
	if !dryRun {
		logger.Info("adopt complete — restart Erigon; the canonical files are now live")
	}
	return nil
}

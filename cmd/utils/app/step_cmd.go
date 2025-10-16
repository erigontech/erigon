package app

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/node/debug"
)

func stepRebase(cliCtx *cli.Context) error {
	logger, _, _, _, err := debug.Setup(cliCtx, true /* root logger */)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	currentStepSize := uint64(config3.DefaultStepSize) // in the future we can allow overrides
	newStepSize := cliCtx.Uint64("new-step-size")
	logger.Info("Rebasing step size", "current", currentStepSize, "new", newStepSize)

	if newStepSize == currentStepSize {
		logger.Info("Step size is already at the desired value; exiting", "new-step-size", newStepSize)
		return nil
	}
	if newStepSize > currentStepSize && newStepSize%currentStepSize != 0 {
		logger.Crit("Invalid step size", "new-step-size", newStepSize)
		return fmt.Errorf("new step size must be a multiple of %d", currentStepSize)
	} else if currentStepSize > newStepSize && currentStepSize%newStepSize != 0 {
		logger.Crit("Invalid step size", "new-step-size", newStepSize)
		return fmt.Errorf("new step size must be divisible from %d", currentStepSize)
	}

	factor := newStepSize / currentStepSize
	decr := newStepSize < currentStepSize
	if decr {
		factor = currentStepSize / newStepSize
		logger.Info("Decreasing step size", "factor", factor)
	} else {
		logger.Info("Increasing step size", "factor", factor)
	}

	// Look for files to be renamed
	dirs := datadir.Open(cliCtx.String("datadir"))
	re := regexp.MustCompile(`^v(\d+(?:\.\d+)?)-(.+)\.(\d+)-(\d+)\.([^.]+)$`)

	rens := make([]string, 0, 100)
	domains, err := collectRenameList(dirs.SnapDomain, re, decr, factor)
	if err != nil {
		return err
	}
	hists, err := collectRenameList(dirs.SnapHistory, re, decr, factor)
	if err != nil {
		return err
	}
	accessors, err := collectRenameList(dirs.SnapAccessors, re, decr, factor)
	if err != nil {
		return err
	}
	idxs, err := collectRenameList(dirs.SnapIdx, re, decr, factor)
	if err != nil {
		return err
	}
	rens = append(rens, domains...)
	rens = append(rens, hists...)
	rens = append(rens, accessors...)
	rens = append(rens, idxs...)

	// List files to be renamed
	for i := 0; i < len(rens); i += 2 {
		fmt.Printf("R: %s => %s\n", rens[i], rens[i+1])
	}

	// Collect and list .torrent files to be deleted
	dels := make([]string, 0, 100)
	domainTorrents, err := collectTorrentFiles(dirs.SnapDomain)
	if err != nil {
		return err
	}
	histTorrents, err := collectTorrentFiles(dirs.SnapHistory)
	if err != nil {
		return err
	}
	accessorTorrents, err := collectTorrentFiles(dirs.SnapAccessors)
	if err != nil {
		return err
	}
	idxTorrents, err := collectTorrentFiles(dirs.SnapIdx)
	if err != nil {
		return err
	}
	dels = append(dels, domainTorrents...)
	dels = append(dels, histTorrents...)
	dels = append(dels, accessorTorrents...)
	dels = append(dels, idxTorrents...)

	// include whole chaindata directory for deletion
	dels = append(dels, dirs.Chaindata)
	for _, f := range dels {
		fmt.Printf("D: %s\n", f)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Proceed with these changes? [y/N]: ")
	answer, _ := reader.ReadString('\n')
	answer = strings.TrimSpace(strings.ToLower(answer))
	if answer != "y" && answer != "yes" {
		return fmt.Errorf("aborted by user")
	}

	// Perform renames
	for i := 0; i < len(rens); i += 2 {
		from := rens[i]
		to := rens[i+1]
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", from, to, err)
		}
		fmt.Printf("Renamed: %s => %s\n", from, to)
	}

	// Perform deletions
	for _, p := range dels {
		if err := dir.RemoveAll(p); err != nil {
			return fmt.Errorf("delete %s: %w", p, err)
		}
		fmt.Printf("Deleted: %s\n", p)
	}

	// warn about manual changes required
	fmt.Printf("\nMANUAL CHANGES REQUIRED:\n\n")
	newFrozenSteps := config3.DefaultStepsInFrozenFile / factor
	if decr {
		newFrozenSteps = config3.DefaultStepsInFrozenFile * factor
	}
	fmt.Printf("When starting erigon against this datadir, use the: --%s %d --%s %d flags.\n", utils.ErigonDBStepSizeFlag.Name, newStepSize, utils.ErigonDBStepsInFrozenFileFlag.Name, newFrozenSteps)

	return nil
}

// collectRenameList walks the snapshot domain directory and builds a list of
// from/to file paths to be renamed according to the new step size rules.
func collectRenameList(domainPath string, re *regexp.Regexp, decr bool, factor uint64) ([]string, error) {
	renList := make([]string, 0, 100)

	walkErr := fs.WalkDir(os.DirFS(domainPath), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !re.MatchString(path) {
			return nil
		}
		matches := re.FindStringSubmatch(path)
		from, err := strconv.ParseUint(matches[3], 10, 64)
		if err != nil {
			return err
		}
		to, err := strconv.ParseUint(matches[4], 10, 64)
		if err != nil {
			return err
		}

		// decreasing step size -> 1 step will contain more steps;
		// increasing step size -> 1 step will be a fraction of new steps -> datadir may not contain a full step!
		if decr {
			from *= factor
			to *= factor
		} else {
			from /= factor
			to /= factor
		}

		newPath := fmt.Sprintf("v%s-%s.%d-%d.%s", matches[1], matches[2], from, to, matches[5])
		renList = append(renList, filepath.Join(domainPath, path), filepath.Join(domainPath, newPath))

		return nil
	})

	if walkErr != nil {
		return nil, walkErr
	}
	return renList, nil
}

// collectTorrentFiles walks given root and returns absolute paths to files ending with .torrent
func collectTorrentFiles(root string) ([]string, error) {
	list := make([]string, 0, 100)
	walkErr := fs.WalkDir(os.DirFS(root), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".torrent") {
			list = append(list, filepath.Join(root, path))
		}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return list, nil
}

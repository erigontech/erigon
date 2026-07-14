package app

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
)

type stepRebasePlan struct {
	renames   []string // flattened from/to pairs
	deletions []string
	resetExec bool
	settings  state.ErigonDBSettings
}

func stepRebase(ctx context.Context, cliCtx *cli.Command) error {
	logger := log.Root()
	dirs := datadir.Open(cliCtx.String("datadir"))
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	if err != nil {
		return err
	}
	newStepSize := cliCtx.Uint64("new-step-size")
	keepBlocks := cliCtx.Bool("keep-blocks")
	logger.Info("Rebasing step size", "current", settings.StepSize, "new", newStepSize, "keep-blocks", keepBlocks)
	if newStepSize == 0 {
		logger.Crit("Invalid step size", "new-step-size", newStepSize)
		return fmt.Errorf("new step size must be greater than 0")
	}
	if settings.StepSize == 0 {
		logger.Crit("Invalid current step size", "current-step-size", settings.StepSize)
		return fmt.Errorf("current step size must be greater than 0")
	}
	if newStepSize == settings.StepSize {
		logger.Info("Step size is already at the desired value; exiting", "new-step-size", newStepSize)
		return nil
	}
	plan, err := buildStepRebasePlan(dirs, settings, newStepSize, keepBlocks, logger)
	if err != nil {
		return err
	}
	printStepRebasePlan(plan)
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Proceed with these changes? [y/N]: ")
	answer, _ := reader.ReadString('\n')
	answer = strings.TrimSpace(strings.ToLower(answer))
	if answer != "y" && answer != "yes" {
		return fmt.Errorf("aborted by user")
	}
	return applyStepRebasePlan(ctx, dirs, plan, logger)
}

func buildStepRebasePlan(dirs datadir.Dirs, settings *state.ErigonDBSettings, newStepSize uint64, keepBlocks bool, logger log.Logger) (stepRebasePlan, error) {
	currentStepSize := settings.StepSize
	if newStepSize > currentStepSize && newStepSize%currentStepSize != 0 {
		return stepRebasePlan{}, fmt.Errorf("new step size must be a multiple of %d", currentStepSize)
	}
	if currentStepSize > newStepSize && currentStepSize%newStepSize != 0 {
		return stepRebasePlan{}, fmt.Errorf("new step size must be divisible from %d", currentStepSize)
	}
	factor := newStepSize / currentStepSize
	decr := newStepSize < currentStepSize
	if decr {
		factor = currentStepSize / newStepSize
		logger.Info("Decreasing step size", "factor", factor)
	} else {
		logger.Info("Increasing step size", "factor", factor)
	}
	re := regexp.MustCompile(`^v(\d+(?:\.\d+)?)-(.+)\.(\d+)-(\d+)\.([^.]+)$`)
	stateSnapDirs := []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapAccessors, dirs.SnapIdx}
	rens := make([]string, 0, 100)
	for _, p := range stateSnapDirs {
		list, err := collectRenameList(p, re, decr, factor)
		if err != nil {
			return stepRebasePlan{}, err
		}
		rens = append(rens, list...)
	}
	dels := make([]string, 0, 100)
	for _, p := range stateSnapDirs {
		list, err := collectTorrentFiles(p)
		if err != nil {
			return stepRebasePlan{}, err
		}
		dels = append(dels, list...)
	}
	if !keepBlocks {
		dels = append(dels, dirs.Chaindata)
	}
	// erigondb.toml.torrent is invalidated by the rebase
	dels = append(dels, filepath.Join(dirs.Snap, "erigondb.toml.torrent"))
	newFrozenSteps := settings.StepsInFrozenFile / factor
	if decr {
		newFrozenSteps = settings.StepsInFrozenFile * factor
	}
	newSettings := state.ErigonDBSettings{StepSize: newStepSize, StepsInFrozenFile: newFrozenSteps}
	return stepRebasePlan{renames: rens, deletions: dels, resetExec: keepBlocks, settings: newSettings}, nil
}

func printStepRebasePlan(plan stepRebasePlan) {
	for i := 0; i < len(plan.renames); i += 2 {
		fmt.Printf("R: %s => %s\n", plan.renames[i], plan.renames[i+1])
	}
	for _, f := range plan.deletions {
		fmt.Printf("D: %s\n", f)
	}
	if plan.resetExec {
		fmt.Printf("Reset: execution state tables in chaindata (blocks kept)\n")
	}
}

func applyStepRebasePlan(ctx context.Context, dirs datadir.Dirs, plan stepRebasePlan, logger log.Logger) error {
	unlock, err := dirs.TryFlock()
	if err != nil {
		return err
	}
	defer unlock()
	// reset before renaming: a crash mid-apply then leaves old settings with old file
	// names and cleared exec state, which is a consistent datadir and a re-runnable command
	if plan.resetExec {
		if err := resetExecState(ctx, dirs, logger); err != nil {
			return err
		}
	}
	for i := 0; i < len(plan.renames); i += 2 {
		from, to := plan.renames[i], plan.renames[i+1]
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", from, to, err)
		}
		fmt.Printf("Renamed: %s => %s\n", from, to)
	}
	for _, p := range plan.deletions {
		if err := dir.RemoveAll(p); err != nil {
			return fmt.Errorf("delete %s: %w", p, err)
		}
		fmt.Printf("Deleted: %s\n", p)
	}
	settingsBytes, err := toml.Marshal(plan.settings)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), settingsBytes, 0644)
}

func resetExecState(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	exists, err := dir.FileExist(filepath.Join(dirs.Chaindata, "mdbx.dat"))
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("no chaindata database to keep at %s", dirs.Chaindata)
	}
	// exclusive non-Accede open: fail fast if the DB is in use rather than resetting a live node
	chainDB, err := dbCfg(dbcfg.ChainDB, dirs.Chaindata).Accede(false).Exclusive(true).Open(ctx)
	if err != nil {
		return err
	}
	defer chainDB.Close()
	agg := openAgg(ctx, dirs, chainDB, logger)
	defer agg.Close()
	tdb, err := temporal.New(chainDB, agg, nil)
	if err != nil {
		return err
	}
	return rawdbreset.ResetExec(ctx, tdb)
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
		return nil, fmt.Errorf("scanning %s: %w", domainPath, walkErr)
	}
	return renList, nil
}

// collectTorrentFiles walks given root and returns absolute paths to files ending with .torrent
func collectTorrentFiles(root string) ([]string, error) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil, nil
	}
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
		return nil, fmt.Errorf("scanning %s: %w", root, walkErr)
	}
	return list, nil
}

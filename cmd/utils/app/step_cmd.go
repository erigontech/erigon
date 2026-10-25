package app

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/ethconfig/features"
)

type stepRebasePlan struct {
	renames      []string // flattened from/to pairs
	deletions    []string
	resetExec    bool
	fromStepSize uint64
	alignFactor  uint64
	alignEnd     uint64
	settings     state.ErigonDBSettings
}

var stepFileRe = regexp.MustCompile(`^v(\d+(?:\.\d+)?)-(.+)\.(\d+)-(\d+)\.([^.]+)$`)

func stateSnapDirs(dirs datadir.Dirs) []string {
	return []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapAccessors, dirs.SnapIdx}
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
	var alignEnd uint64
	if newStepSize > settings.StepSize {
		if err := withStepRebaseAgg(ctx, dirs, keepBlocks, logger, func(agg *state.Aggregator) (err error) {
			alignEnd, err = agg.StepAlignedEnd(newStepSize)
			return err
		}); err != nil {
			return err
		}
	}
	plan, err := buildStepRebasePlan(dirs, settings, newStepSize, keepBlocks, alignEnd, logger)
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

func buildStepRebasePlan(dirs datadir.Dirs, settings *state.ErigonDBSettings, newStepSize uint64, keepBlocks bool, alignEnd uint64, logger log.Logger) (stepRebasePlan, error) {
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
	rens := make([]string, 0, 100)
	dels := make([]string, 0, 100)
	var alignFactor uint64
	if decr {
		for _, p := range stateSnapDirs(dirs) {
			list, err := collectRenameList(p, decr, factor, math.MaxUint64)
			if err != nil {
				return stepRebasePlan{}, err
			}
			rens = append(rens, list...)
		}
	} else {
		if settings.RefsInCommitmentBranches() {
			return stepRebasePlan{}, fmt.Errorf("references_in_commitment_branches is on: merged commitment files would be referenced, and read as plain at step size %d", newStepSize)
		}
		alignFactor = factor
		tail, err := collectFilesPast(stateSnapDirs(dirs), alignEnd/currentStepSize)
		if err != nil {
			return stepRebasePlan{}, err
		}
		if alignEnd == 0 && len(tail) > 0 {
			return stepRebasePlan{}, fmt.Errorf("no file boundary common to all state files is a multiple of %d txs, nothing would be kept", newStepSize)
		}
		dels = append(dels, tail...)
	}
	for _, p := range stateSnapDirs(dirs) {
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
	newSettings := *settings
	newSettings.StepSize = newStepSize
	newSettings.StepsInFrozenFile = newFrozenSteps
	return stepRebasePlan{renames: rens, deletions: dels, resetExec: keepBlocks, fromStepSize: currentStepSize, alignFactor: alignFactor, alignEnd: alignEnd, settings: newSettings}, nil
}

func printStepRebasePlan(plan stepRebasePlan) {
	if plan.alignFactor != 0 {
		fmt.Printf("M: merge state files below txNum %d to multiples of %d txs, then rename every file to 1/%d of its steps\n", plan.alignEnd, plan.settings.StepSize, plan.alignFactor)
	}
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
	current, err := state.ResolveErigonDBSettings(dirs, logger, false)
	if err != nil {
		return err
	}
	if current.StepSize != plan.fromStepSize {
		return fmt.Errorf("step size is %d, the plan was made at %d", current.StepSize, plan.fromStepSize)
	}
	renames, deletions := plan.renames, slices.Clone(plan.deletions)
	if plan.alignFactor != 0 {
		if err := withStepRebaseAgg(ctx, dirs, plan.resetExec, logger, func(agg *state.Aggregator) error {
			end, err := agg.AlignFilesToStepSize(ctx, plan.settings.StepSize)
			if err != nil {
				return err
			}
			if end != plan.alignEnd {
				return fmt.Errorf("state files changed since the plan: aligned up to txNum %d, planned %d", end, plan.alignEnd)
			}
			return nil
		}); err != nil {
			return err
		}
		endStep := plan.alignEnd / plan.fromStepSize
		for _, p := range stateSnapDirs(dirs) {
			list, err := collectRenameList(p, false, plan.alignFactor, endStep)
			if err != nil {
				return err
			}
			renames = append(renames, list...)
		}
		tail, err := collectFilesPast(stateSnapDirs(dirs), endStep)
		if err != nil {
			return err
		}
		for _, p := range stateSnapDirs(dirs) {
			torrents, err := collectTorrentFiles(p)
			if err != nil {
				return err
			}
			tail = append(tail, torrents...)
		}
		for _, f := range tail {
			if !slices.Contains(deletions, f) {
				deletions = append(deletions, f)
			}
		}
	}
	// reset before renaming: a crash mid-apply then leaves old settings with old file
	// names and cleared exec state, which is a consistent datadir and a re-runnable command
	if plan.resetExec {
		if err := resetExecState(ctx, dirs, logger); err != nil {
			return err
		}
	}
	for _, p := range deletions {
		if err := dir.RemoveAll(p); err != nil {
			return fmt.Errorf("delete %s: %w", p, err)
		}
		fmt.Printf("Deleted: %s\n", p)
	}
	for i := 0; i < len(renames); i += 2 {
		from, to := renames[i], renames[i+1]
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", from, to, err)
		}
		fmt.Printf("Renamed: %s => %s\n", from, to)
	}
	settingsBytes, err := toml.Marshal(plan.settings)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), settingsBytes, 0o644)
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

func withStepRebaseAgg(ctx context.Context, dirs datadir.Dirs, keepBlocks bool, logger log.Logger, fn func(*state.Aggregator) error) error {
	exists, err := dir.FileExist(filepath.Join(dirs.Chaindata, "mdbx.dat"))
	if err != nil {
		return err
	}
	if !exists && keepBlocks {
		return fmt.Errorf("no chaindata database to keep at %s", dirs.Chaindata)
	}
	chainDB, err := dbCfg(dbcfg.ChainDB, dirs.Chaindata).Accede(false).Exclusive(true).Open(ctx)
	if err != nil {
		return err
	}
	defer chainDB.Close()
	if _, err := features.EnableSyncCfg(chainDB, ethconfig.Sync{}); err != nil {
		return err
	}
	agg, err := tryOpenAgg(ctx, dirs, chainDB, logger)
	if err != nil {
		return err
	}
	defer agg.Close()
	return fn(agg)
}

func collectFilesPast(roots []string, endStep uint64) ([]string, error) {
	var past []string
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, entry := range entries {
			matches := stepFileRe.FindStringSubmatch(entry.Name())
			if matches == nil {
				continue
			}
			to, err := strconv.ParseUint(matches[4], 10, 64)
			if err != nil {
				return nil, err
			}
			if to > endStep {
				past = append(past, filepath.Join(root, entry.Name()))
			}
		}
	}
	return past, nil
}

// collectRenameList walks the snapshot domain directory and builds a list of
// from/to file paths to be renamed according to the new step size rules.
func collectRenameList(domainPath string, decr bool, factor, keepToStep uint64) ([]string, error) {
	renList := make([]string, 0, 100)

	walkErr := fs.WalkDir(os.DirFS(domainPath), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !stepFileRe.MatchString(path) {
			return nil
		}
		matches := stepFileRe.FindStringSubmatch(path)
		from, err := strconv.ParseUint(matches[3], 10, 64)
		if err != nil {
			return err
		}
		to, err := strconv.ParseUint(matches[4], 10, 64)
		if err != nil {
			return err
		}
		if to > keepToStep {
			return nil
		}
		if !decr && (from%factor != 0 || to%factor != 0) {
			return fmt.Errorf("%s does not start and end at a multiple of %d steps", filepath.Join(domainPath, path), factor)
		}

		// decreasing step size -> 1 step will contain more steps;
		newFrom, newTo := from/factor, to/factor
		if decr {
			newFrom, newTo = from*factor, to*factor
		}
		if matches[2] == kv.CommitmentDomain.String() && matches[5] == "kv" {
			ver, err := version.ParseVersion(path)
			if err != nil {
				return err
			}
			if state.CommitmentBranchReferenced(ver, 1, from, to) && !state.CommitmentBranchReferenced(ver, 1, newFrom, newTo) {
				return fmt.Errorf("%s: referenced commitment branches would be read as plain at the new step size", filepath.Join(domainPath, path))
			}
		}

		newPath := fmt.Sprintf("v%s-%s.%d-%d.%s", matches[1], matches[2], newFrom, newTo, matches[5])
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

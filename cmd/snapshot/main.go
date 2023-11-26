package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

var (
	DataDirFlag = flags.DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for logs, temp files. local snapshots etc",
		Value: flags.DirectoryString(""),
	}
	CmpLocations = cli.StringSliceFlag{
		Name:     "locs",
		Usage:    `Locations to source snapshots for comparison with optional ":version" e.g. ../snapshots:v1,r2:location:v2 torrent,r2:location`,
		Required: true,
	}
	SegTypes = cli.StringSliceFlag{
		Name:     "types",
		Usage:    `Segment types to comparre with optional e.g. headers,bodies,transactions`,
		Required: true,
	}
)

var cmpCommand = cli.Command{
	Action:    compareSegments,
	Name:      "cmp",
	Usage:     "Compare snapshot segments",
	ArgsUsage: "<start block (000's)> <end block (000's)>",
	Flags: []cli.Flag{
		&DataDirFlag,
		&CmpLocations,
		&SegTypes,
	},
	Description: ``,
}

var copyCommand = cli.Command{
	Action:    copySegments,
	Name:      "copy",
	Usage:     "copy snapshot segments",
	ArgsUsage: "<start block> <end block>",
	Flags: []cli.Flag{
		&DataDirFlag,
	},
	Description: ``,
}

var moveCommand = cli.Command{
	Action:    moveSegments,
	Name:      "move",
	Usage:     "Move snapshot segments",
	ArgsUsage: "<start block> <end block>",
	Flags: []cli.Flag{
		&DataDirFlag,
	},
	Description: ``,
}

func main() {
	app := cli.NewApp()
	app.Name = "snapshot"
	app.Version = params.VersionWithCommit(params.GitCommit)

	app.Commands = []*cli.Command{
		&cmpCommand,
		&copyCommand,
		&moveCommand,
	}

	app.Flags = []cli.Flag{
		&DataDirFlag,
		&logging.LogVerbosityFlag,
		&logging.LogConsoleVerbosityFlag,
		&logging.LogDirVerbosityFlag,
	}

	app.UsageText = app.Name + ` [command] [flags]`

	app.Action = func(context *cli.Context) error {
		if context.Args().Present() {
			var goodNames []string
			for _, c := range app.VisibleCommands() {
				goodNames = append(goodNames, c.Name)
			}
			_, _ = fmt.Fprintf(os.Stderr, "Command '%s' not found. Available commands: %s\n", context.Args().First(), goodNames)
			cli.ShowAppHelpAndExit(context, 1)
		}

		debug.RaiseFdLimit()

		return nil
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setupLogger(ctx *cli.Context) (log.Logger, error) {
	dataDir := ctx.String(DataDirFlag.Name)
	logsDir := filepath.Join(dataDir, "logs")

	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, err
	}

	logger := logging.SetupLoggerCtx("snapshot", ctx, false /* rootLogger */)

	return logger, nil
}

func handleTerminationSignals(stopFunc func(), logger log.Logger) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	switch s := <-signalCh; s {
	case syscall.SIGTERM:
		logger.Info("Stopping")
		stopFunc()
	case syscall.SIGINT:
		logger.Info("Terminating")
		os.Exit(-int(syscall.SIGINT))
	}
}

type ltype int

const (
	torrent ltype = iota
	localfs
	remotefs
)

type locator struct {
	ltype   ltype
	src     string
	root    string
	version uint8
}

func (l locator) String() string {
	var val string

	switch l.ltype {
	case torrent:
		val = "torrent"
	case localfs:
		val = l.root
	case remotefs:
		val = l.src + ":" + l.root
	}

	if l.version > 0 {
		val += fmt.Sprint(":v", l.version)
	}

	return val
}

var locatorExp, _ = regexp.Compile(`^(?:(\w+)\:)?(.*)(?:\:(v\d+))+`)

func ParseLocator(value string) (*locator, error) {
	if matches := locatorExp.FindStringSubmatch(value); len(matches) > 0 {
		var loc locator

		switch {
		case len(matches[1]) > 0:
			loc.ltype = remotefs
			loc.src = matches[1]
			loc.root = matches[2]
		case matches[2] == "torrent":
			loc.ltype = torrent
		default:
			loc.ltype = localfs
			loc.root = downloader.Clean(matches[2])
		}

		if len(matches[3]) > 0 {
			version, err := strconv.ParseUint(matches[3][1:], 10, 8)
			if err != nil {
				return nil, fmt.Errorf("can't parse version: %s: %w", matches[3], err)
			}

			loc.version = uint8(version)
		}

		return &loc, nil
	}

	return nil, fmt.Errorf("Invalid locator syntax")
}

func compareSegments(cliCtx *cli.Context) error {
	logger, err := setupLogger(cliCtx)

	if err != nil {
		return err
	}

	var firstLoc, secondLoc *locator

	var remotes []string
	var rcCli *downloader.RCloneClient

	checkRemote := func(src string) error {
		if rcCli == nil {
			rcCli, err = downloader.NewRCloneClient(logger)

			if err != nil {
				return err
			}

			remotes, err = rcCli.ListRemotes(context.Background())

			if err != nil {
				return err
			}
		}

		hasRemote := false
		for _, remote := range remotes {
			if src == remote {
				hasRemote = true
				break
			}
		}

		if !hasRemote {
			return fmt.Errorf("unknown remote: %s", src)
		}

		return nil
	}

	err = func() error {
		for _, val := range cliCtx.StringSlice(CmpLocations.Name) {
			switch {
			case firstLoc == nil:
				if firstLoc, err = ParseLocator(val); err != nil {
					return err
				}

				if firstLoc.ltype == remotefs {
					if err = checkRemote(firstLoc.src); err != nil {
						return err
					}
				}

			case secondLoc == nil:
				if secondLoc, err = ParseLocator(val); err != nil {
					return err
				}

				if secondLoc.ltype == remotefs {
					if err = checkRemote(secondLoc.src); err != nil {
						return err
					}
				}

				break
			}
		}

		return nil
	}()

	if err != nil {
		return err
	}

	var snapTypes []snaptype.Type

	for _, val := range cliCtx.StringSlice(SegTypes.Name) {
		segType, ok := snaptype.ParseFileType(val)

		if !ok {
			return fmt.Errorf("unknown file type: %s", val)
		}

		snapTypes = append(snapTypes, segType)
	}

	var firstBlock, lastBlock uint64

	if cliCtx.Args().Len() > 0 {
		firstBlock, err = strconv.ParseUint(cliCtx.Args().Get(0), 10, 64)
	}

	if cliCtx.Args().Len() > 1 {
		lastBlock, err = strconv.ParseUint(cliCtx.Args().Get(1), 10, 64)
	}

	dataDir := cliCtx.String(DataDirFlag.Name)
	var tempDir string

	if len(dataDir) == 0 {
		dataDir, err := os.MkdirTemp("", "snapshot-cpy-")
		if err != nil {
			return err
		}
		tempDir = dataDir
		defer os.RemoveAll(dataDir)
	} else {
		tempDir = filepath.Join(dataDir, "temp")

		if err := os.MkdirAll(tempDir, 0755); err != nil {
			return err
		}
	}

	var firstSession *downloader.RCloneSession
	var secondSession *downloader.RCloneSession

	if rcCli != nil {
		if firstLoc.ltype == remotefs {
			firstSession, err = rcCli.NewSession(cliCtx.Context, filepath.Join(tempDir, "s1"), firstLoc.src+":"+firstLoc.root)

			if err != nil {
				return err
			}
		}

		if secondLoc.ltype == remotefs {
			secondSession, err = rcCli.NewSession(cliCtx.Context, filepath.Join(tempDir, "s2"), secondLoc.src+":"+secondLoc.root)

			if err != nil {
				return err
			}
		}
	}

	logger.Info(fmt.Sprintf("Starting compare: %s==%s", firstLoc.String(), secondLoc.String()), "first", firstBlock, "last", lastBlock, "types", snapTypes)

	if firstSession != nil {

	}

	var f2ents []fs.DirEntry

	if secondSession != nil {
		logger.Info("Reading s2 dir", "remoteFs", secondSession.RemoteFsRoot(), "group", secondSession.Label())
		files, err := secondSession.ReadRemoteDir(cliCtx.Context, true)

		if err != nil {
			return err
		}

		for _, ent := range files {
			if info, err := ent.Info(); err == nil {
				if snapInfo, ok := info.Sys().(downloader.SnapInfo); ok && snapInfo.Version() > 0 {
					if secondLoc.version == snapInfo.Version() &&
						(firstBlock == 0 || snapInfo.From()*1000 >= firstBlock) &&
						(lastBlock == 0 || snapInfo.From()*1000 < lastBlock) {
						includeType := len(snapTypes) == 0

						for _, snapType := range snapTypes {
							if snapInfo.Type() == snapType {
								includeType = true
								break
							}
						}

						if includeType {
							f2ents = append(f2ents, ent)
						}
					}
				}
			}
		}
	}

	for i, ent := range f2ents {
		entInfo, _ := ent.Info()
		logger.Info(fmt.Sprintf("Processing %s", ent.Name()), "entry", fmt.Sprint(i, "/", len(f2ents)), "size", datasize.ByteSize(entInfo.Size()).HumanReadable())
		info, err := secondSession.Download(cliCtx.Context, ent.Name())

		if err != nil {
			return err
		}

		f2snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
			Enabled:      true,
			Produce:      false,
			NoDownloader: true,
		}, secondSession.LocalFsRoot(), secondLoc.version, logger)

		f2snaps.ReopenList([]string{ent.Name()}, false)

		blockReader := freezeblocks.NewBlockReader(f2snaps, nil)

		blockReader.HeadersRange(cliCtx.Context, func(header *types.Header) error {
			return nil
		})

		os.Remove(info.Name())
	}

	return nil
}

func copySegments(cliCtx *cli.Context) error {
	logger, err := setupLogger(cliCtx)

	if err != nil {
		return err
	}

	logger.Info("Starting copy")

	return nil
}

func moveSegments(cliCtx *cli.Context) error {
	logger, err := setupLogger(cliCtx)

	if err != nil {
		return err
	}

	logger.Info("Starting move")

	return nil
}

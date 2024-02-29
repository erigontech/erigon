package torrents

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	gosync "sync"
	"time"

	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/log/v3"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cmd/snapshots/manifest"
	"github.com/ledgerwatch/erigon/cmd/snapshots/sync"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var Command = cli.Command{
	Action: func(cliCtx *cli.Context) error {
		return torrents(cliCtx, "list")
	},
	Name:  "torrent",
	Usage: "torrent utilities",
	Subcommands: []*cli.Command{
		{
			Action: func(cliCtx *cli.Context) error {
				return torrents(cliCtx, "list")
			},
			Name:      "list",
			Usage:     "list torrents available at the specified storage location",
			ArgsUsage: "<location>",
		},
		{
			Action: func(cliCtx *cli.Context) error {
				return torrents(cliCtx, "hashes")
			},
			Name:      "hashes",
			Usage:     "list the hashes (in toml format) at the specified storage location",
			ArgsUsage: "<location> <start block> <end block>",
		},
		{
			Action: func(cliCtx *cli.Context) error {
				return torrents(cliCtx, "update")
			},
			Name:      "update",
			Usage:     "update re-create the torrents for the contents available at its storage location",
			ArgsUsage: "<location> <start block> <end block>",
		},
		{
			Action: func(cliCtx *cli.Context) error {
				return torrents(cliCtx, "verify")
			},
			Name:      "verify",
			Usage:     "verify that manifest contents are available at its storage location",
			ArgsUsage: "<location> <start block> <end block>",
		},
	},
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&logging.LogVerbosityFlag,
		&logging.LogConsoleVerbosityFlag,
		&logging.LogDirVerbosityFlag,
	},
	Description: ``,
}

func torrents(cliCtx *cli.Context, command string) error {
	logger := sync.Logger(cliCtx.Context)

	var src *sync.Locator
	var err error

	var firstBlock, lastBlock uint64

	pos := 0

	if src, err = sync.ParseLocator(cliCtx.Args().Get(pos)); err != nil {
		return err
	}

	pos++

	if cliCtx.Args().Len() > pos {
		if src, err = sync.ParseLocator(cliCtx.Args().Get(pos)); err != nil {
			return err
		}

		if err != nil {
			return err
		}
	}

	pos++

	if cliCtx.Args().Len() > pos {
		firstBlock, err = strconv.ParseUint(cliCtx.Args().Get(pos), 10, 64)
		if err != nil {
			return err
		}
	}

	pos++

	if cliCtx.Args().Len() > pos {
		lastBlock, err = strconv.ParseUint(cliCtx.Args().Get(pos), 10, 64)

		if err != nil {
			return err
		}
	}

	if src == nil {
		return fmt.Errorf("missing data source")
	}

	var rcCli *downloader.RCloneClient

	switch src.LType {
	case sync.RemoteFs:
		if rcCli == nil {
			rcCli, err = downloader.NewRCloneClient(logger)

			if err != nil {
				return err
			}
		}

		if err = sync.CheckRemote(rcCli, src.Src); err != nil {
			return err
		}
	}

	var srcSession *downloader.RCloneSession

	dataDir := cliCtx.String(utils.DataDirFlag.Name)
	var tempDir string

	if len(dataDir) == 0 {
		dataDir, err := os.MkdirTemp("", "snapshot-torrents-")
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

	if rcCli != nil {
		if src != nil && src.LType == sync.RemoteFs {
			srcSession, err = rcCli.NewSession(cliCtx.Context, filepath.Join(tempDir, "src"), src.Src+":"+src.Root)

			if err != nil {
				return err
			}
		}
	}

	if src != nil && srcSession == nil {
		return fmt.Errorf("no src session established")
	}

	logger.Debug("Starting torrents " + command)

	switch command {
	case "hashes":
		return torrentHashes(cliCtx.Context, srcSession, firstBlock, lastBlock)
	case "update":
		startTime := time.Now()

		logger.Info(fmt.Sprintf("Starting update: %s", src.String()), "first", firstBlock, "last", lastBlock, "dir", tempDir)

		err := updateTorrents(cliCtx.Context, srcSession, firstBlock, lastBlock, logger)

		if err == nil {
			logger.Info(fmt.Sprintf("Finished update: %s", src.String()), "elapsed", time.Since(startTime))
		} else {
			logger.Info(fmt.Sprintf("Aborted update: %s", src.String()), "err", err)
		}

		return err

	case "verify":
		startTime := time.Now()

		logger.Info(fmt.Sprintf("Starting verify: %s", src.String()), "first", firstBlock, "last", lastBlock, "dir", tempDir)

		err := verifyTorrents(cliCtx.Context, srcSession, firstBlock, lastBlock, logger)

		if err == nil {
			logger.Info(fmt.Sprintf("Verified: %s", src.String()), "elapsed", time.Since(startTime))
		} else {
			logger.Info(fmt.Sprintf("Verification failed: %s", src.String()), "err", err)
		}

		return err
	}

	return listTorrents(cliCtx.Context, srcSession, os.Stdout, firstBlock, lastBlock)
}

func listTorrents(ctx context.Context, srcSession *downloader.RCloneSession, out *os.File, from uint64, to uint64) error {
	entries, err := manifest.DownloadManifest(ctx, srcSession)

	if err != nil {
		entries, err = srcSession.ReadRemoteDir(ctx, true)
	}

	if err != nil {
		return err
	}

	for _, fi := range entries {
		if filepath.Ext(fi.Name()) != ".torrent" {
			continue
		}
		if from > 0 || to > 0 {
			info, _, ok := snaptype.ParseFileName("", strings.TrimSuffix(fi.Name(), ".torrent"))
			if ok {
				if from > 0 && info.From < from {
					continue
				}

				if to > 0 && info.From > to {
					continue
				}
			}
		}

		fmt.Fprintln(out, fi.Name())
	}

	return nil
}

func torrentHashes(ctx context.Context, srcSession *downloader.RCloneSession, from uint64, to uint64) error {
	entries, err := manifest.DownloadManifest(ctx, srcSession)

	if err != nil {
		return err
	}

	type hashInfo struct {
		name, hash string
	}

	var hashes []hashInfo
	var hashesMutex gosync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)

	for _, fi := range entries {
		if filepath.Ext(fi.Name()) != ".torrent" {
			continue
		}
		if from > 0 || to > 0 {
			info, _, ok := snaptype.ParseFileName("", strings.TrimSuffix(fi.Name(), ".torrent"))
			if ok {
				if from > 0 && info.From < from {
					continue
				}

				if to > 0 && info.From > to {
					continue
				}
			}
		}

		file := fi.Name()

		g.Go(func() error {
			var mi *metainfo.MetaInfo

			errs := 0

			for {
				reader, err := srcSession.Cat(gctx, file)

				if err != nil {
					return fmt.Errorf("can't read remote torrent: %s: %w", file, err)
				}

				mi, err = metainfo.Load(reader)

				if err != nil {
					errs++

					if errs == 4 {
						return fmt.Errorf("can't parse remote torrent: %s: %w", file, err)
					}

					continue
				}

				break
			}

			info, err := mi.UnmarshalInfo()

			if err != nil {
				return fmt.Errorf("can't unmarshal torrent info: %s: %w", file, err)
			}

			hashesMutex.Lock()
			defer hashesMutex.Unlock()
			hashes = append(hashes, hashInfo{info.Name, mi.HashInfoBytes().String()})

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	slices.SortFunc(hashes, func(a, b hashInfo) int {
		return strings.Compare(a.name, b.name)
	})

	for _, hi := range hashes {
		fmt.Printf("'%s' = '%s'\n", hi.name, hi.hash)
	}

	return nil
}

func updateTorrents(ctx context.Context, srcSession *downloader.RCloneSession, from uint64, to uint64, logger log.Logger) error {
	entries, err := manifest.DownloadManifest(ctx, srcSession)

	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)

	torrentFiles := downloader.NewAtomicTorrentFiles(srcSession.LocalFsRoot())

	for _, fi := range entries {
		if filepath.Ext(fi.Name()) != ".torrent" {
			continue
		}
		file := strings.TrimSuffix(fi.Name(), ".torrent")

		g.Go(func() error {
			if from > 0 || to > 0 {
				info, _, ok := snaptype.ParseFileName("", file)
				if ok {
					if from > 0 && info.From < from {
						return nil
					}

					if to > 0 && info.From > to {
						return nil
					}
				}
			}

			logger.Info(fmt.Sprintf("Updating %s", file+".torrent"))

			err := srcSession.Download(gctx, file)

			if err != nil {
				return err
			}

			defer os.Remove(filepath.Join(srcSession.LocalFsRoot(), file))

			err = downloader.BuildTorrentIfNeed(gctx, file, srcSession.LocalFsRoot(), torrentFiles)

			if err != nil {
				return err
			}

			defer os.Remove(filepath.Join(srcSession.LocalFsRoot(), file+".torrent"))

			return srcSession.Upload(gctx, file+".torrent")
		})
	}

	return g.Wait()
}

func verifyTorrents(ctx context.Context, srcSession *downloader.RCloneSession, from uint64, to uint64, logger log.Logger) error {
	entries, err := manifest.DownloadManifest(ctx, srcSession)

	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)

	torrentFiles := downloader.NewAtomicTorrentFiles(srcSession.LocalFsRoot())

	for _, fi := range entries {
		if filepath.Ext(fi.Name()) != ".torrent" {
			continue
		}
		file := strings.TrimSuffix(fi.Name(), ".torrent")

		g.Go(func() error {
			if from > 0 || to > 0 {
				info, _, ok := snaptype.ParseFileName("", file)
				if ok {
					if from > 0 && info.From < from {
						return nil
					}

					if to > 0 && info.From > to {
						return nil
					}
				}
			}

			logger.Info(fmt.Sprintf("Validating %s", file+".torrent"))

			var mi *metainfo.MetaInfo

			errs := 0

			for {
				reader, err := srcSession.Cat(gctx, file+".torrent")

				if err != nil {
					return fmt.Errorf("can't read remote torrent: %s: %w", file+".torrent", err)
				}

				mi, err = metainfo.Load(reader)

				if err != nil {
					errs++

					if errs == 4 {
						return fmt.Errorf("can't parse remote torrent: %s: %w", file+".torrent", err)
					}

					continue
				}

				break
			}

			info, err := mi.UnmarshalInfo()

			if err != nil {
				return fmt.Errorf("can't unmarshal torrent info: %s: %w", file+".torrent", err)
			}

			if info.Name != file {
				return fmt.Errorf("torrent name does not match file: %s", file)
			}

			err = srcSession.Download(gctx, file)

			if err != nil {
				return err
			}

			defer os.Remove(filepath.Join(srcSession.LocalFsRoot(), file))

			err = downloader.BuildTorrentIfNeed(gctx, file, srcSession.LocalFsRoot(), torrentFiles)

			if err != nil {
				return err
			}

			torrentPath := filepath.Join(srcSession.LocalFsRoot(), file+".torrent")

			defer os.Remove(torrentPath)

			lmi, err := metainfo.LoadFromFile(torrentPath)

			if err != nil {
				return fmt.Errorf("can't load local torrent from: %s: %w", torrentPath, err)
			}

			if lmi.HashInfoBytes() != mi.HashInfoBytes() {
				return fmt.Errorf("computed local hash does not match torrent: %s: expected: %s, got: %s", file+".torrent", lmi.HashInfoBytes(), mi.HashInfoBytes())
			}

			localInfo, err := lmi.UnmarshalInfo()

			if err != nil {
				return fmt.Errorf("can't unmarshal local torrent info: %s: %w", torrentPath, err)
			}

			if localInfo.Name != info.Name {
				return fmt.Errorf("computed local name does not match torrent: %s: expected: %s, got: %s", file+".torrent", localInfo.Name, info.Name)
			}

			return nil
		})
	}

	return g.Wait()
}

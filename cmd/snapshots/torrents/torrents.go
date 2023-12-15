package torrents

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	gosync "sync"

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

var (
	SrcFlag = cli.StringFlag{
		Name:     "src",
		Usage:    `Source location for files`,
		Required: false,
	}
)

var Command = cli.Command{
	Action:    torrents,
	Name:      "torrent",
	Usage:     "torrent utilities - list, hashes, update, verify",
	ArgsUsage: "<cmd> <start block> <end block>",
	Flags: []cli.Flag{
		&SrcFlag,
		&utils.DataDirFlag,
		&logging.LogVerbosityFlag,
		&logging.LogConsoleVerbosityFlag,
		&logging.LogDirVerbosityFlag,
	},
	Description: ``,
}

func torrents(cliCtx *cli.Context) error {
	logger := sync.Logger(cliCtx.Context)

	var src *sync.Locator
	var err error

	var rcCli *downloader.RCloneClient

	if src, err = sync.ParseLocator(cliCtx.String(SrcFlag.Name)); err != nil {
		return err
	}

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

	var firstBlock, lastBlock uint64

	command := "list"
	pos := 0

	if cliCtx.Args().Len() > 0 {
		arg := cliCtx.Args().Get(pos)

		firstBlock, err = strconv.ParseUint(arg, 10, 64)

		if err != nil {
			switch arg {
			case "update", "hashes", "verify":
				command = arg
			default:
				return fmt.Errorf("unknown torrents command: %q", arg)
			}

			pos++

			if cliCtx.Args().Len() > pos {
				firstBlock, err = strconv.ParseUint(cliCtx.Args().Get(pos), 10, 64)

				if err != nil {
					return err
				}
			}
		} else {
			command = "list"
		}
	}

	pos++

	if cliCtx.Args().Len() > pos {
		lastBlock, err = strconv.ParseUint(cliCtx.Args().Get(pos), 10, 64)

		if err != nil {
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
		return updateTorrents(cliCtx.Context, srcSession, firstBlock, lastBlock)
	case "verify":
		return verifyTorrents(cliCtx.Context, srcSession, firstBlock, lastBlock)
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
		if filepath.Ext(fi.Name()) == ".torrent" {
			if from > 0 || to > 0 {
				info, _ := snaptype.ParseFileName("", strings.TrimSuffix(fi.Name(), ".torrent"))

				if from > 0 && info.From < from {
					continue
				}

				if to > 0 && info.From > to {
					continue
				}
			}

			fmt.Fprintln(out, fi.Name())
		}
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
		if filepath.Ext(fi.Name()) == ".torrent" {
			if from > 0 || to > 0 {
				info, _ := snaptype.ParseFileName("", strings.TrimSuffix(fi.Name(), ".torrent"))

				if from > 0 && info.From < from {
					continue
				}

				if to > 0 && info.From > to {
					continue
				}
			}

			g.Go(func() error {
				var mi *metainfo.MetaInfo

				errs := 0

				for {
					reader, err := srcSession.Cat(gctx, fi.Name())

					if err != nil {
						return fmt.Errorf("can't read remote torrent: %s: %w", fi.Name(), err)
					}

					mi, err = metainfo.Load(reader)

					if err != nil {
						errs++

						if errs == 4 {
							return fmt.Errorf("can't parse remote torrent: %s: %w", fi.Name(), err)
						}

						continue
					}

					break
				}

				info, err := mi.UnmarshalInfo()

				if err != nil {
					return fmt.Errorf("can't unmarshal torrent info: %s: %w", fi.Name(), err)
				}

				hashesMutex.Lock()
				defer hashesMutex.Unlock()
				hashes = append(hashes, hashInfo{info.Name, mi.HashInfoBytes().String()})

				return nil
			})
		}
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

func updateTorrents(ctx context.Context, srcSession *downloader.RCloneSession, from uint64, to uint64) error {
	entries, err := manifest.DownloadManifest(ctx, srcSession)

	if err != nil {
		return err
	}

	for _, fi := range entries {
		if filepath.Ext(fi.Name()) == ".torrent" {
			file := strings.TrimSuffix(fi.Name(), ".torrent")

			err := func() error {
				if from > 0 || to > 0 {
					info, _ := snaptype.ParseFileName("", file)

					if from > 0 && info.From < from {
						return nil
					}

					if to > 0 && info.From > to {
						return nil
					}
				}

				err := srcSession.Download(ctx, file)

				if err != nil {
					return err
				}

				defer os.Remove(filepath.Join(srcSession.LocalFsRoot(), file))

				torrentPath, err := downloader.BuildTorrentIfNeed(ctx, file, srcSession.LocalFsRoot())

				if err != nil {
					return err
				}

				defer os.Remove(torrentPath)

				return srcSession.Upload(ctx, file+".torrent")
			}()

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func verifyTorrents(ctx context.Context, srcSession *downloader.RCloneSession, from uint64, to uint64) error {
	return fmt.Errorf("TODO")
}

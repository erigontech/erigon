package copy

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cmd/snapshots/flags"
	"github.com/ledgerwatch/erigon/cmd/snapshots/sync"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/urfave/cli/v2"
)

var (
	TorrentsFlag = cli.BoolFlag{
		Name:     "torrents",
		Usage:    `Include torrent files in copy`,
		Required: false,
	}

	HashesFlag = cli.BoolFlag{
		Name:     "hashes",
		Usage:    `Include hash .toml in copy`,
		Required: false,
	}

	ManifestFlag = cli.BoolFlag{
		Name:     "manifest",
		Usage:    `Include mannfest .txt in copy`,
		Required: false,
	}

	VersionFlag = cli.IntFlag{
		Name:     "version",
		Usage:    `File versions to copy`,
		Required: false,
		Value:    0,
	}
)

var Command = cli.Command{
	Action:    copy,
	Name:      "copy",
	Usage:     "copy snapshot segments",
	ArgsUsage: "<start block> <end block>",
	Flags: []cli.Flag{
		&VersionFlag,
		&flags.SegTypes,
		&TorrentsFlag,
		&HashesFlag,
		&ManifestFlag,
		&utils.DataDirFlag,
		&logging.LogVerbosityFlag,
		&logging.LogConsoleVerbosityFlag,
		&logging.LogDirVerbosityFlag,
		&utils.WebSeedsFlag,
		&utils.NATFlag,
		&utils.DisableIPV6,
		&utils.DisableIPV4,
		&utils.TorrentDownloadRateFlag,
		&utils.TorrentUploadRateFlag,
		&utils.TorrentVerbosityFlag,
		&utils.TorrentPortFlag,
		&utils.TorrentMaxPeersFlag,
		&utils.TorrentConnsPerFileFlag,
	},
	Description: ``,
}

func copy(cliCtx *cli.Context) error {
	logger := sync.Logger(cliCtx.Context)

	logger.Info("Starting copy")

	var src, dst *sync.Locator
	var err error

	var rcCli *downloader.RCloneClient
	var torrentCli *sync.TorrentClient

	pos := 0

	if cliCtx.Args().Len() > pos {
		val := cliCtx.Args().Get(pos)

		if src, err = sync.ParseLocator(val); err != nil {
			return err
		}
	}

	pos++

	if cliCtx.Args().Len() > pos {
		val := cliCtx.Args().Get(pos)

		if src, err = sync.ParseLocator(val); err != nil {
			return err
		}

		pos++
	}

	switch dst.LType {
	case sync.TorrentFs:
		return fmt.Errorf("can't copy to torrent - need intermediate local fs")

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

	switch src.LType {
	case sync.TorrentFs:
		torrentCli, err = sync.NewTorrentClient(cliCtx, dst.Chain)
		if err != nil {
			return fmt.Errorf("can't create torrent: %w", err)
		}

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

	typeValues := cliCtx.StringSlice(flags.SegTypes.Name)
	snapTypes := make([]snaptype.Type, 0, len(typeValues))

	for _, val := range typeValues {
		segType, ok := snaptype.ParseFileType(val)

		if !ok {
			return fmt.Errorf("unknown file type: %s", val)
		}

		snapTypes = append(snapTypes, segType)
	}

	torrents := cliCtx.Bool(TorrentsFlag.Name)
	hashes := cliCtx.Bool(HashesFlag.Name)
	manifest := cliCtx.Bool(ManifestFlag.Name)

	var firstBlock, lastBlock uint64

	version := cliCtx.Int(VersionFlag.Name)

	if version != 0 {
		dst.Version = snaptype.Version(version)
	}

	if cliCtx.Args().Len() > pos {
		if firstBlock, err = strconv.ParseUint(cliCtx.Args().Get(pos), 10, 64); err != nil {
			return err
		}

		pos++
	}

	if cliCtx.Args().Len() > pos {
		if lastBlock, err = strconv.ParseUint(cliCtx.Args().Get(pos), 10, 64); err != nil {
			return err
		}
	}

	switch src.LType {
	case sync.LocalFs:
		switch dst.LType {
		case sync.LocalFs:
			return localToLocal(src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		case sync.RemoteFs:
			return localToRemote(rcCli, src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		default:
			return fmt.Errorf("unhandled torrent destination: %s", dst)
		}

	case sync.RemoteFs:
		switch dst.LType {
		case sync.LocalFs:
			return remoteToLocal(cliCtx.Context, rcCli, src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		case sync.RemoteFs:
			return remoteToRemote(rcCli, src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		default:
			return fmt.Errorf("unhandled torrent destination: %s", dst)
		}

	case sync.TorrentFs:
		switch dst.LType {
		case sync.LocalFs:
			return torrentToLocal(torrentCli, src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		case sync.RemoteFs:
			return torrentToRemote(torrentCli, rcCli, src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		default:
			return fmt.Errorf("unhandled torrent destination: %s", dst)
		}

	}
	return nil
}

func torrentToLocal(torrentCli *sync.TorrentClient, src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return fmt.Errorf("TODO")
}

func torrentToRemote(torrentCli *sync.TorrentClient, rcCli *downloader.RCloneClient, src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return fmt.Errorf("TODO")
}

func localToRemote(rcCli *downloader.RCloneClient, src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return fmt.Errorf("TODO")
}

func localToLocal(src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return fmt.Errorf("TODO")
}

func remoteToLocal(ctx context.Context, rcCli *downloader.RCloneClient, src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	logger := sync.Logger(ctx)

	if rcCli == nil {
		return fmt.Errorf("no remote downloader")
	}

	session, err := rcCli.NewSession(ctx, dst.Root, src.Src+":"+src.Root)

	if err != nil {
		return err
	}

	logger.Info("Reading src dir", "remoteFs", session.RemoteFsRoot(), "label", session.Label())
	fileEntries, err := session.ReadRemoteDir(ctx, true)

	if err != nil {
		return err
	}

	files := selectFiles(fileEntries, dst.Version, from, to, snapTypes, torrents, hashes, manifest)

	logger.Info(fmt.Sprintf("Downloading %s", files))

	return session.Download(ctx, files...)
}

func remoteToRemote(rcCli *downloader.RCloneClient, src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return fmt.Errorf("TODO")
}

type sinf struct {
	snaptype.FileInfo
}

func (i sinf) Version() snaptype.Version {
	return i.FileInfo.Version
}

func (i sinf) From() uint64 {
	return i.FileInfo.From
}

func (i sinf) To() uint64 {
	return i.FileInfo.To
}

func (i sinf) Type() snaptype.Type {
	return i.FileInfo.Type
}

func selectFiles(entries []fs.DirEntry, version snaptype.Version, firstBlock, lastBlock uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) []string {
	var files []string

	for _, ent := range entries {
		if info, err := ent.Info(); err == nil {
			snapInfo, _ := info.Sys().(downloader.SnapInfo)

			if torrents {
				if ext := filepath.Ext(info.Name()); ext == ".torrent" {
					fileName := strings.TrimSuffix(info.Name(), ".torrent")

					if fileInfo, isStateFile, ok := snaptype.ParseFileName("", fileName); ok {
						if isStateFile {
							//TODO
						} else {
							snapInfo = sinf{fileInfo}
						}
					}
				}
			}

			switch {
			case snapInfo != nil && snapInfo.Type() != nil:
				if (version == 0 || version == snapInfo.Version()) &&
					(firstBlock == 0 || snapInfo.From() >= firstBlock) &&
					(lastBlock == 0 || snapInfo.From() < lastBlock) {

					if len(snapTypes) == 0 {
						files = append(files, info.Name())
					} else {
						for _, snapType := range snapTypes {
							if snapType == snapInfo.Type() {
								files = append(files, info.Name())
								break
							}
						}
					}
				}

			case manifest:

			case hashes:

			}
		}
	}

	return files
}

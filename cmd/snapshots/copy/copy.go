// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package copy

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-db/downloader"
	"github.com/erigontech/erigon-lib/snaptype"
	"github.com/erigontech/erigon-lib/version"
	"github.com/erigontech/erigon/cmd/snapshots/flags"
	"github.com/erigontech/erigon/cmd/snapshots/sync"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/turbo/logging"
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

	VersionFlag = cli.StringFlag{
		Name:     "version",
		Usage:    `File versions to copy`,
		Required: false,
		Value:    "0.0",
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

	switch dst.LType { //nolint:govet
	case sync.TorrentFs:
		return errors.New("can't copy to torrent - need intermediate local fs")

	case sync.RemoteFs:
		if rcCli == nil { //nolint:govet
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
		config := sync.NewTorrentClientConfigFromCobra(cliCtx, dst.Chain) //nolint:govet
		torrentCli, err = sync.NewTorrentClient(cliCtx.Context, config)
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

	versionStr := cliCtx.String(VersionFlag.Name)

	if versionStr != "" && versionStr != "0.0" {
		dst.Version, _ = version.ParseVersion("v" + versionStr) //nolint:govet
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
		switch dst.LType { //nolint:govet
		case sync.LocalFs:
			return localToLocal(src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		case sync.RemoteFs:
			return localToRemote(rcCli, src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		default:
			return fmt.Errorf("unhandled torrent destination: %s", dst)
		}

	case sync.RemoteFs:
		switch dst.LType { //nolint:govet
		case sync.LocalFs:
			return remoteToLocal(cliCtx.Context, rcCli, src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		case sync.RemoteFs:
			return remoteToRemote(rcCli, src, dst, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
		default:
			return fmt.Errorf("unhandled torrent destination: %s", dst)
		}

	case sync.TorrentFs:
		switch dst.LType { //nolint:govet
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
	return errors.New("TODO")
}

func torrentToRemote(torrentCli *sync.TorrentClient, rcCli *downloader.RCloneClient, src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return errors.New("TODO")
}

func localToRemote(rcCli *downloader.RCloneClient, src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return errors.New("TODO")
}

func localToLocal(src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return errors.New("TODO")
}

func remoteToLocal(ctx context.Context, rcCli *downloader.RCloneClient, src *sync.Locator, dst *sync.Locator, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	logger := sync.Logger(ctx)

	if rcCli == nil {
		return errors.New("no remote downloader")
	}

	session, err := rcCli.NewSession(ctx, dst.Root, src.Src+":"+src.Root, nil)

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
	return errors.New("TODO")
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
				if (version.IsZero() || version == snapInfo.Version()) &&
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

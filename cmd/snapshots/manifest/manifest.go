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

package manifest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/downloader"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon/v3/cmd/snapshots/sync"
	"github.com/erigontech/erigon/v3/cmd/utils"
	"github.com/erigontech/erigon/v3/turbo/logging"
)

var (
	VersionFlag = cli.IntFlag{
		Name:     "version",
		Usage:    `Manifest file versions`,
		Required: false,
		Value:    0,
	}
)

var Command = cli.Command{
	Action: func(cliCtx *cli.Context) error {
		return manifest(cliCtx, "list")
	},
	Name:  "manifest",
	Usage: "manifest utilities",
	Subcommands: []*cli.Command{
		{
			Action: func(cliCtx *cli.Context) error {
				return manifest(cliCtx, "list")
			},
			Name:      "list",
			Usage:     "list manifest from storage location",
			ArgsUsage: "<location>",
		},
		{
			Action: func(cliCtx *cli.Context) error {
				return manifest(cliCtx, "update")
			},
			Name:      "update",
			Usage:     "update the manifest to match the files available at its storage location",
			ArgsUsage: "<location>",
		},
		{
			Action: func(cliCtx *cli.Context) error {
				return manifest(cliCtx, "verify")
			},
			Name:      "verify",
			Usage:     "verify that manifest matches the files available at its storage location",
			ArgsUsage: "<location>",
		},
	},
	Flags: []cli.Flag{
		&VersionFlag,
		&utils.DataDirFlag,
		&logging.LogVerbosityFlag,
		&logging.LogConsoleVerbosityFlag,
		&logging.LogDirVerbosityFlag,
	},
	Description: ``,
}

func manifest(cliCtx *cli.Context, command string) error {
	logger := sync.Logger(cliCtx.Context)

	var src *sync.Locator
	var err error

	var rcCli *downloader.RCloneClient

	pos := 0

	if cliCtx.Args().Len() == 0 {
		return errors.New("missing manifest location")
	}

	arg := cliCtx.Args().Get(pos)

	if src, err = sync.ParseLocator(arg); err != nil {
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

	var srcSession *downloader.RCloneSession

	tempDir, err := os.MkdirTemp("", "snapshot-manifest-")

	if err != nil {
		return err
	}

	defer os.RemoveAll(tempDir)

	if rcCli != nil {
		if src != nil && src.LType == sync.RemoteFs {
			srcSession, err = rcCli.NewSession(cliCtx.Context, tempDir, src.Src+":"+src.Root, nil)

			if err != nil {
				return err
			}
		}
	}

	if src != nil && srcSession == nil {
		return errors.New("no src session established")
	}

	logger.Debug("Starting manifest " + command)

	var version *snaptype.Version

	if val := cliCtx.Int(VersionFlag.Name); val != 0 {
		v := snaptype.Version(val)
		version = &v
	}

	switch command {
	case "update":
		return updateManifest(cliCtx.Context, tempDir, srcSession, version)
	case "verify":
		return verifyManifest(cliCtx.Context, srcSession, version, os.Stdout)
	default:
		return listManifest(cliCtx.Context, srcSession, os.Stdout)
	}
}

func listManifest(ctx context.Context, srcSession *downloader.RCloneSession, out *os.File) error {
	entries, err := DownloadManifest(ctx, srcSession)

	if err != nil {
		return err
	}

	for _, fi := range entries {
		fmt.Fprintln(out, fi.Name())
	}

	return nil
}

func updateManifest(ctx context.Context, tmpDir string, srcSession *downloader.RCloneSession, version *snaptype.Version) error {
	entities, err := srcSession.ReadRemoteDir(ctx, true)

	if err != nil {
		return err
	}

	manifestFile := "manifest.txt"

	fileMap := map[string]string{}
	torrentMap := map[string]string{}

	for _, fi := range entities {
		var file string
		var files map[string]string

		if filepath.Ext(fi.Name()) == ".torrent" {
			file = strings.TrimSuffix(fi.Name(), ".torrent")
			files = torrentMap
		} else {
			file = fi.Name()
			files = fileMap
		}

		info, isStateFile, ok := snaptype.ParseFileName("", file)
		if !ok {
			continue
		}
		if !isStateFile && version != nil && *version != info.Version {
			continue
		}

		files[file] = fi.Name()
	}

	var files []string

	for file := range fileMap {
		if torrent, ok := torrentMap[file]; ok {
			files = append(files, file, torrent)
		}
	}

	sort.Strings(files)

	manifestEntries := bytes.Buffer{}

	for _, file := range files {
		fmt.Fprintln(&manifestEntries, file)
	}

	_ = os.WriteFile(filepath.Join(tmpDir, manifestFile), manifestEntries.Bytes(), 0644)
	defer os.Remove(filepath.Join(tmpDir, manifestFile))

	return srcSession.Upload(ctx, manifestFile)
}

func verifyManifest(ctx context.Context, srcSession *downloader.RCloneSession, version *snaptype.Version, out *os.File) error {
	manifestEntries, err := DownloadManifest(ctx, srcSession)

	if err != nil {
		return fmt.Errorf("verification failed: can't read manifest: %w", err)
	}

	manifestFiles := map[string]struct{}{}

	for _, fi := range manifestEntries {
		var file string

		if filepath.Ext(fi.Name()) == ".torrent" {
			file = strings.TrimSuffix(fi.Name(), ".torrent")
		} else {
			file = fi.Name()
		}

		info, isStateFile, ok := snaptype.ParseFileName("", file)
		if !ok {
			continue
		}
		if !isStateFile && version != nil && *version != info.Version {
			continue
		}

		manifestFiles[fi.Name()] = struct{}{}
	}

	dirEntries, err := srcSession.ReadRemoteDir(ctx, true)

	if err != nil {
		return fmt.Errorf("verification failed: can't read dir: %w", err)
	}

	dirFiles := map[string]struct{}{}

	for _, fi := range dirEntries {

		var file string

		if filepath.Ext(fi.Name()) == ".torrent" {
			file = strings.TrimSuffix(fi.Name(), ".torrent")
		} else {
			file = fi.Name()
		}

		info, isStateFile, ok := snaptype.ParseFileName("", file)
		if !ok {
			continue
		}
		if !isStateFile && version != nil && *version != info.Version {
			continue
		}

		if _, ok := manifestFiles[fi.Name()]; ok {
			delete(manifestFiles, fi.Name())
		} else {
			dirFiles[fi.Name()] = struct{}{}
		}
	}

	var missing string
	var extra string

	if len(manifestFiles) != 0 {
		files := make([]string, 0, len(manifestFiles))

		for file := range manifestFiles {
			files = append(files, file)
		}

		missing = fmt.Sprintf(": manifest files not in src: %s", files)
	}

	if len(dirFiles) != 0 {
		files := make([]string, 0, len(dirFiles))

		for file := range dirFiles {
			files = append(files, file)
		}

		extra = fmt.Sprintf(": src files not in manifest: %s", files)
	}

	if len(missing) > 0 || len(extra) != 0 {
		return fmt.Errorf("manifest does not match src contents%s%s", missing, extra)
	}
	return nil
}

type dirEntry struct {
	name string
}

func (e dirEntry) Name() string {
	return e.name
}

func (e dirEntry) IsDir() bool {
	return false
}

func (e dirEntry) Type() fs.FileMode {
	return e.Mode()
}

func (e dirEntry) Size() int64 {
	return -1
}

func (e dirEntry) Mode() fs.FileMode {
	return fs.ModeIrregular
}

func (e dirEntry) ModTime() time.Time {
	return time.Time{}
}

func (e dirEntry) Sys() any {
	return nil
}

func (e dirEntry) Info() (fs.FileInfo, error) {
	return e, nil
}

func DownloadManifest(ctx context.Context, session *downloader.RCloneSession) ([]fs.DirEntry, error) {

	reader, err := session.Cat(ctx, "manifest.txt")

	if err != nil {
		return nil, err
	}

	var entries []fs.DirEntry

	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		entries = append(entries, dirEntry{scanner.Text()})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

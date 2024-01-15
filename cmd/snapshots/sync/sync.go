package sync

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloadernat"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/erigon/params"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type LType int

const (
	TorrentFs LType = iota
	LocalFs
	RemoteFs
)

type Locator struct {
	LType   LType
	Src     string
	Root    string
	Version uint8
	Chain   string
}

func (l Locator) String() string {
	var val string

	switch l.LType {
	case TorrentFs:
		val = "torrent"
	case LocalFs:
		val = l.Root
	case RemoteFs:
		val = l.Src + ":" + l.Root
	}

	if l.Version > 0 {
		val += fmt.Sprint(":v", l.Version)
	}

	return val
}

var locatorExp, _ = regexp.Compile(`^(?:(\w+)\:)?([^\:]*)(?:\:(v\d+))?`)
var srcExp, _ = regexp.Compile(`^erigon-v\d+-snapshots-(.*)$`)

func ParseLocator(value string) (*Locator, error) {
	if matches := locatorExp.FindStringSubmatch(value); len(matches) > 0 {
		var loc Locator

		switch {
		case matches[1] == "torrent":
			loc.LType = TorrentFs

			if len(matches[2]) > 0 {
				version, err := strconv.ParseUint(matches[2][1:], 10, 8)
				if err != nil {
					return nil, fmt.Errorf("can't parse version: %s: %w", matches[3], err)
				}

				loc.Version = uint8(version)
			}

		case len(matches[1]) > 0:
			loc.LType = RemoteFs
			loc.Src = matches[1]
			loc.Root = matches[2]

			if matches := srcExp.FindStringSubmatch(loc.Root); len(matches) > 1 {
				loc.Chain = matches[1]
			}

			if len(matches[3]) > 0 {
				version, err := strconv.ParseUint(matches[3][1:], 10, 8)
				if err != nil {
					return nil, fmt.Errorf("can't parse version: %s: %w", matches[3], err)
				}

				loc.Version = uint8(version)
			}

		default:
			loc.LType = LocalFs
			loc.Root = downloader.Clean(matches[2])
		}

		return &loc, nil
	}

	if path, err := filepath.Abs(value); err == nil {
		return &Locator{
			LType: LocalFs,
			Root:  path,
		}, nil
	}

	return nil, fmt.Errorf("Invalid locator syntax")
}

type TorrentClient struct {
	*torrent.Client
	cfg *torrent.ClientConfig
}

func NewTorrentClient(cliCtx *cli.Context, chain string) (*TorrentClient, error) {
	logger := Logger(cliCtx.Context)
	tempDir := TempDir(cliCtx.Context)

	torrentDir := filepath.Join(tempDir, "torrents", chain)

	dirs := datadir.New(torrentDir)

	webseedsList := common.CliString2Array(cliCtx.String(utils.WebSeedsFlag.Name))

	if known, ok := snapcfg.KnownWebseeds[chain]; ok {
		webseedsList = append(webseedsList, known...)
	}

	var downloadRate, uploadRate datasize.ByteSize

	if err := downloadRate.UnmarshalText([]byte(cliCtx.String(utils.TorrentDownloadRateFlag.Name))); err != nil {
		return nil, err
	}

	if err := uploadRate.UnmarshalText([]byte(cliCtx.String(utils.TorrentUploadRateFlag.Name))); err != nil {
		return nil, err
	}

	logLevel, _, err := downloadercfg.Int2LogLevel(cliCtx.Int(utils.TorrentVerbosityFlag.Name))

	if err != nil {
		return nil, err
	}

	version := "erigon: " + params.VersionWithCommit(params.GitCommit)

	cfg, err := downloadercfg.New(dirs, version, logLevel, downloadRate, uploadRate,
		cliCtx.Int(utils.TorrentPortFlag.Name),
		cliCtx.Int(utils.TorrentConnsPerFileFlag.Name), 0, nil, webseedsList, chain)

	if err != nil {
		return nil, err
	}

	err = os.RemoveAll(torrentDir)

	if err != nil {
		return nil, fmt.Errorf("can't clean torrent dir: %w", err)
	}

	if err := os.MkdirAll(torrentDir, 0755); err != nil {
		return nil, err
	}

	cfg.ClientConfig.DataDir = torrentDir

	cfg.ClientConfig.PieceHashersPerTorrent = 32 * runtime.NumCPU()
	cfg.ClientConfig.DisableIPv6 = cliCtx.Bool(utils.DisableIPV6.Name)
	cfg.ClientConfig.DisableIPv4 = cliCtx.Bool(utils.DisableIPV4.Name)

	natif, err := nat.Parse(utils.NATFlag.Value)

	if err != nil {
		return nil, fmt.Errorf("invalid nat option %s: %w", utils.NATFlag.Value, err)
	}

	downloadernat.DoNat(natif, cfg.ClientConfig, logger)

	cfg.ClientConfig.DefaultStorage = storage.NewMMap(torrentDir)

	cli, err := torrent.NewClient(cfg.ClientConfig)

	if err != nil {
		return nil, fmt.Errorf("can't create torrent client: %w", err)
	}

	return &TorrentClient{cli, cfg.ClientConfig}, nil
}

type torrentSession struct {
	cli   *TorrentClient
	items map[string]snapcfg.PreverifiedItem
}

type fileInfo struct {
	info snapcfg.PreverifiedItem
}

func (fi *fileInfo) Name() string {
	return fi.info.Name
}

func (fi *fileInfo) Size() int64 {
	return 0
}

func (fi *fileInfo) Mode() fs.FileMode {
	return fs.ModeIrregular
}

func (fi *fileInfo) ModTime() time.Time {
	return time.Time{}
}

func (fi *fileInfo) IsDir() bool {
	return false
}

type torrentInfo struct {
	snapInfo *snaptype.FileInfo
	hash     string
}

func (i *torrentInfo) Version() uint8 {
	if i.snapInfo != nil {
		return i.snapInfo.Version
	}

	return 0
}

func (i *torrentInfo) From() uint64 {
	if i.snapInfo != nil {
		return i.snapInfo.From
	}

	return 0
}

func (i *torrentInfo) To() uint64 {
	if i.snapInfo != nil {
		return i.snapInfo.To
	}

	return 0
}

func (i *torrentInfo) Type() snaptype.Type {
	if i.snapInfo != nil {
		return i.snapInfo.T
	}

	return 0
}

func (i *torrentInfo) Hash() string {
	return i.hash
}

func (fi *fileInfo) Sys() any {
	info := torrentInfo{hash: fi.info.Hash}
	if snapInfo, ok := snaptype.ParseFileName("", fi.Name()); ok {
		info.snapInfo = &snapInfo
	}

	return &info
}

type dirEntry struct {
	info *fileInfo
}

func (e dirEntry) Name() string {
	return e.info.Name()
}

func (e dirEntry) IsDir() bool {
	return e.info.IsDir()
}

func (e dirEntry) Type() fs.FileMode {
	return fs.ModeIrregular
}

func (e dirEntry) Info() (fs.FileInfo, error) {
	return e.info, nil
}

func (s *torrentSession) ReadRemoteDir(ctx context.Context, refresh bool) ([]fs.DirEntry, error) {
	var entries = make([]fs.DirEntry, 0, len(s.items))

	for _, info := range s.items {
		entries = append(entries, &dirEntry{&fileInfo{info}})
	}

	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	return entries, nil
}

func (s *torrentSession) LocalFsRoot() string {
	return s.cli.cfg.DataDir
}

func (s *torrentSession) RemoteFsRoot() string {
	return ""
}

func (s *torrentSession) Download(ctx context.Context, files ...string) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(len(files))

	for _, f := range files {
		file := f

		g.Go(func() error {
			it, ok := s.items[file]

			if !ok {
				return fs.ErrNotExist
			}

			t, err := func() (*torrent.Torrent, error) {
				infoHash := snaptype.Hex2InfoHash(it.Hash)

				for _, t := range s.cli.Torrents() {
					if t.Name() == file {
						return t, nil
					}
				}

				mi := &metainfo.MetaInfo{AnnounceList: downloader.Trackers}
				magnet := mi.Magnet(&infoHash, &metainfo.Info{Name: file})
				spec, err := torrent.TorrentSpecFromMagnetUri(magnet.String())

				if err != nil {
					return nil, err
				}

				spec.DisallowDataDownload = true

				t, _, err := s.cli.AddTorrentSpec(spec)
				if err != nil {
					return nil, err
				}

				return t, nil
			}()

			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.GotInfo():
			}

			if !t.Complete.Bool() {
				t.AllowDataDownload()
				t.DownloadAll()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-t.Complete.On():
				}
			}

			closed := t.Closed()
			t.Drop()
			<-closed

			return nil
		})
	}

	return g.Wait()
}

func (s *torrentSession) Label() string {
	return "torrents"
}

func NewTorrentSession(cli *TorrentClient, chain string) *torrentSession {
	session := &torrentSession{cli, map[string]snapcfg.PreverifiedItem{}}
	for _, it := range snapcfg.KnownCfg(chain, 0).Preverified {
		session.items[it.Name] = it
	}

	return session
}

func DownloadManifest(ctx context.Context, session DownloadSession) ([]fs.DirEntry, error) {
	if session, ok := session.(*downloader.RCloneSession); ok {
		reader, err := session.Cat(ctx, "manifest.txt")

		if err != nil {
			return nil, err
		}

		var entries []fs.DirEntry

		scanner := bufio.NewScanner(reader)

		for scanner.Scan() {
			entries = append(entries, dirEntry{&fileInfo{snapcfg.PreverifiedItem{Name: scanner.Text()}}})
		}

		if err := scanner.Err(); err != nil {
			return nil, err
		}

		return entries, nil
	}

	return nil, fmt.Errorf("not implemented for %T", session)
}

type DownloadSession interface {
	Download(ctx context.Context, files ...string) error
	ReadRemoteDir(ctx context.Context, refresh bool) ([]fs.DirEntry, error)
	LocalFsRoot() string
	RemoteFsRoot() string
	Label() string
}

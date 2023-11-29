package main

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloadernat"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

var (
	DataDirFlag = flags.DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for logs, temp files. local snapshots etc",
		Value: flags.DirectoryString(""),
	}
	CmpLocations = cli.StringSliceFlag{
		Name:     "locations",
		Usage:    `Locations to source snapshots for comparison with optional ":version" e.g. ../snapshots:v1,r2:location:v2 torrent,r2:location`,
		Required: true,
	}
	SegTypes = cli.StringSliceFlag{
		Name:     "types",
		Usage:    `Segment types to comparre with optional e.g. headers,bodies,transactions`,
		Required: false,
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
	torrentfs ltype = iota
	localfs
	remotefs
)

type locator struct {
	ltype   ltype
	src     string
	root    string
	version uint8
	chain   string
}

func (l locator) String() string {
	var val string

	switch l.ltype {
	case torrentfs:
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
var srcExp, _ = regexp.Compile(`^erigon-v\d+-snapshots-(.*)$`)

func ParseLocator(value string) (*locator, error) {
	if matches := locatorExp.FindStringSubmatch(value); len(matches) > 0 {
		var loc locator

		switch {
		case len(matches[1]) > 0:
			loc.ltype = remotefs
			loc.src = matches[1]
			loc.root = matches[2]

			if matches := srcExp.FindStringSubmatch(loc.root); len(matches) > 1 {
				loc.chain = matches[1]
			}

		case matches[2] == "torrent":
			loc.ltype = torrentfs
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

type torrentClient struct {
	*torrent.Client
	cfg *torrent.ClientConfig
}

type torrentSession struct {
	cli   *torrentClient
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

func (s *torrentSession) Download(ctx context.Context, file string) (fs.FileInfo, error) {
	it, ok := s.items[file]

	if !ok {
		return nil, fs.ErrNotExist
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
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.GotInfo():
	}

	if !t.Complete.Bool() {
		t.AllowDataDownload()
		t.DownloadAll()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.Complete.On():
		}
	}

	t.Drop()

	return os.Stat(filepath.Join(s.cli.cfg.DataDir, file))
}

func (s *torrentSession) Label() string {
	return "torrents"
}

func newTorrentSession(cli *torrentClient, chain string) *torrentSession {
	session := &torrentSession{cli, map[string]snapcfg.PreverifiedItem{}}
	for _, it := range snapcfg.KnownCfg(chain, nil, nil).Preverified {
		session.items[it.Name] = it
	}

	return session
}

type downloadSession interface {
	Download(ctx context.Context, file string) (fs.FileInfo, error)
	ReadRemoteDir(ctx context.Context, refresh bool) ([]fs.DirEntry, error)
	LocalFsRoot() string
	RemoteFsRoot() string
	Label() string
}

func compareSegments(cliCtx *cli.Context) error {
	logger, err := setupLogger(cliCtx)

	if err != nil {
		return err
	}

	var loc1, loc2 *locator

	var remotes []string
	var rcCli *downloader.RCloneClient
	var torrentCli *torrentClient

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

	torrentInit := func(chain string) (*torrentClient, error) {
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

		return &torrentClient{cli, cfg.ClientConfig}, nil
	}

	var chain string

	err = func() error {
		for _, val := range cliCtx.StringSlice(CmpLocations.Name) {
			switch {
			case loc1 == nil:
				if loc1, err = ParseLocator(val); err != nil {
					return err
				}

				switch loc1.ltype {
				case remotefs:
					if err = checkRemote(loc1.src); err != nil {
						return err
					}

					chain = loc1.chain
				}

			case loc2 == nil:
				if loc2, err = ParseLocator(val); err != nil {
					return err
				}

				switch loc2.ltype {
				case remotefs:
					if err = checkRemote(loc2.src); err != nil {
						return err
					}

					chain = loc2.chain
				}

				return nil
			}
		}

		return nil
	}()

	if err != nil {
		return err
	}

	if loc1.ltype == torrentfs || loc2.ltype == torrentfs {
		torrentCli, err = torrentInit(chain)
		if err != nil {
			return fmt.Errorf("can't create torrent: %w", err)
		}
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

	var session1 downloadSession
	var session2 downloadSession

	if rcCli != nil {
		if loc1.ltype == remotefs {
			session1, err = rcCli.NewSession(cliCtx.Context, filepath.Join(tempDir, "s1"), loc1.src+":"+loc1.root)

			if err != nil {
				return err
			}
		}

		if loc2.ltype == remotefs {
			session2, err = rcCli.NewSession(cliCtx.Context, filepath.Join(tempDir, "s2"), loc2.src+":"+loc2.root)

			if err != nil {
				return err
			}
		}
	}

	if torrentCli != nil {
		if loc1.ltype == torrentfs {
			session1 = newTorrentSession(torrentCli, chain)
		}

		if loc2.ltype == torrentfs {
			session2 = newTorrentSession(torrentCli, chain)
		}
	}

	if session1 == nil {
		return fmt.Errorf("no first session established")
	}

	if session1 == nil {
		return fmt.Errorf("no second session established")
	}

	logger.Info(fmt.Sprintf("Starting compare: %s==%s", loc1.String(), loc2.String()), "first", firstBlock, "last", lastBlock, "types", snapTypes)

	logger.Info("Reading s1 dir", "remoteFs", session1.RemoteFsRoot(), "label", session1.Label())
	files, err := session1.ReadRemoteDir(cliCtx.Context, true)

	if err != nil {
		return err
	}

	h1ents, b1ents := splitEntries(files, loc1.version, firstBlock, lastBlock)

	logger.Info("Reading s2 dir", "remoteFs", session2.RemoteFsRoot(), "label", session2.Label())
	files, err = session2.ReadRemoteDir(cliCtx.Context, true)

	if err != nil {
		return err
	}

	h2ents, b2ents := splitEntries(files, loc2.version, firstBlock, lastBlock)

	c := comparitor{
		chain:    chain,
		loc1:     loc1,
		loc2:     loc2,
		session1: session1,
		session2: session2,
	}

	var funcs []func(ctx context.Context) error

	if len(snapTypes) == 0 {
		funcs = append(funcs, func(ctx context.Context) error {
			return c.compareHeaders(ctx, h1ents, h2ents, logger)
		}, func(ctx context.Context) error {
			return c.compareBodies(ctx, b1ents, b2ents, logger)
		})
	} else {
		for _, snapType := range snapTypes {
			if snapType == snaptype.Headers {
				funcs = append(funcs, func(ctx context.Context) error {
					return c.compareHeaders(ctx, h1ents, h2ents, logger)
				})
			}

			if snapType == snaptype.Bodies {
				funcs = append(funcs, func(ctx context.Context) error {
					return c.compareBodies(ctx, b1ents, b2ents, logger)
				})
			}
		}
	}

	if len(funcs) > 0 {
		g, ctx := errgroup.WithContext(cliCtx.Context)
		g.SetLimit(len(funcs))

		for _, f := range funcs {
			func(ctx context.Context, f func(ctx context.Context) error) {
				g.Go(func() error { return f(ctx) })
			}(ctx, f)
		}

		return g.Wait()
	}
	return nil
}

type bodyEntry struct {
	from, to           uint64
	body, transactions fs.DirEntry
}

func splitEntries(files []fs.DirEntry, version uint8, firstBlock, lastBlock uint64) (hents []fs.DirEntry, bents []*bodyEntry) {
	for _, ent := range files {
		if info, err := ent.Info(); err == nil {
			if snapInfo, ok := info.Sys().(downloader.SnapInfo); ok && snapInfo.Version() > 0 {
				if version == snapInfo.Version() &&
					(firstBlock == 0 || snapInfo.From() >= firstBlock) &&
					(lastBlock == 0 || snapInfo.From() < lastBlock) {

					if snapInfo.Type() == snaptype.Headers {
						hents = append(hents, ent)
					}

					if snapInfo.Type() == snaptype.Bodies {
						found := false

						for _, bent := range bents {
							if snapInfo.From() == bent.from &&
								snapInfo.To() == bent.to {
								bent.body = ent
								found = true
							}
						}

						if !found {
							bents = append(bents, &bodyEntry{snapInfo.From(), snapInfo.To(), ent, nil})
						}
					}

					if snapInfo.Type() == snaptype.Transactions {
						found := false

						for _, bent := range bents {
							if snapInfo.From() == bent.from &&
								snapInfo.To() == bent.to {
								bent.transactions = ent
								found = true

							}
						}

						if !found {
							bents = append(bents, &bodyEntry{snapInfo.From(), snapInfo.To(), nil, ent})
						}
					}
				}
			}
		}
	}

	return hents, bents
}

type comparitor struct {
	chain      string
	loc1, loc2 *locator
	session1   downloadSession
	session2   downloadSession
}

func (c comparitor) chainConfig() *chain.Config {
	return params.ChainConfigByChainName(c.chain)
}

func (c comparitor) compareHeaders(ctx context.Context, f1ents []fs.DirEntry, f2ents []fs.DirEntry, logger log.Logger) error {
	for i1, ent1 := range f1ents {
		var snapInfo1 downloader.SnapInfo

		if info, err := ent1.Info(); err == nil {
			snapInfo1, _ = info.Sys().(downloader.SnapInfo)
		}

		if snapInfo1 == nil {
			continue
		}

		for i2, ent2 := range f2ents {
			var snapInfo2 downloader.SnapInfo

			ent2Info, err := ent2.Info()

			if err == nil {
				snapInfo2, _ = ent2Info.Sys().(downloader.SnapInfo)
			}

			if snapInfo2 == nil ||
				snapInfo1.Type() != snapInfo2.Type() ||
				snapInfo1.From() != snapInfo2.From() ||
				snapInfo1.To() != snapInfo2.To() {
				continue
			}

			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(2)

			g.Go(func() error {
				logger.Info(fmt.Sprintf("Downloading %s", ent1.Name()), "entry", fmt.Sprint(i1+1, "/", len(f1ents)))
				_, err := c.session1.Download(gctx, ent1.Name())

				if err != nil {
					return err
				}

				return nil
			})

			g.Go(func() error {
				logger.Info(fmt.Sprintf("Downloading %s", ent2.Name()), "entry", fmt.Sprint(i2+1, "/", len(f2ents)), "size", datasize.ByteSize(ent2Info.Size()))
				_, err := c.session2.Download(gctx, ent2.Name())

				if err != nil {
					return err
				}

				return nil
			})

			if err := g.Wait(); err != nil {
				return err
			}

			logger.Info(fmt.Sprintf("Comparing %s %s", ent1.Name(), ent2.Name()))

			f1snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
				Enabled:      true,
				Produce:      false,
				NoDownloader: true,
			}, c.session1.LocalFsRoot(), c.loc1.version, logger)

			f1snaps.ReopenList([]string{ent1.Name()}, false)

			f2snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
				Enabled:      true,
				Produce:      false,
				NoDownloader: true,
			}, c.session2.LocalFsRoot(), c.loc2.version, logger)

			f2snaps.ReopenList([]string{ent2.Name()}, false)

			blockReader1 := freezeblocks.NewBlockReader(f1snaps, nil)
			blockReader2 := freezeblocks.NewBlockReader(f2snaps, nil)

			g, gctx = errgroup.WithContext(ctx)
			g.SetLimit(2)

			h2chan := make(chan *types.Header)

			g.Go(func() error {
				blockReader2.HeadersRange(gctx, func(h2 *types.Header) error {
					select {
					case h2chan <- h2:
						return nil
					case <-gctx.Done():
						return gctx.Err()
					}
				})

				close(h2chan)
				return nil
			})

			g.Go(func() error {
				err := blockReader1.HeadersRange(gctx, func(h1 *types.Header) error {
					select {
					case h2 := <-h2chan:
						if h2 == nil {
							return fmt.Errorf("header %d unknown", h1.Number.Uint64())
						}

						if h1.Number.Uint64() != h2.Number.Uint64() {
							return fmt.Errorf("mismatched headers: expected %d, Got: %d", h1.Number.Uint64(), h2.Number.Uint64())
						}

						var h1buf, h2buf bytes.Buffer

						h1.EncodeRLP(&h1buf)
						h2.EncodeRLP(&h2buf)

						if !bytes.Equal(h1buf.Bytes(), h2buf.Bytes()) {
							return fmt.Errorf("%d: headers do not match", h1.Number.Uint64())
						}

						return nil
					case <-gctx.Done():
						return gctx.Err()
					}
				})

				return err
			})

			err = g.Wait()

			files := f1snaps.OpenFiles()
			f1snaps.Close()

			files = append(files, f2snaps.OpenFiles()...)
			f2snaps.Close()

			for _, file := range files {
				os.Remove(file)
			}

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c comparitor) compareBodies(ctx context.Context, f1ents []*bodyEntry, f2ents []*bodyEntry, logger log.Logger) error {
	for i1, ent1 := range f1ents {
		for i2, ent2 := range f2ents {
			if ent1.from != ent2.from ||
				ent1.to != ent2.to {
				continue
			}

			g, ctx := errgroup.WithContext(ctx)
			g.SetLimit(4)

			b1err := make(chan error, 1)

			g.Go(func() error {
				logger.Info(fmt.Sprintf("Downloading %s", ent1.body.Name()), "entry", fmt.Sprint(i1+1, "/", len(f1ents)))
				_, err := c.session1.Download(ctx, ent1.body.Name())

				b1err <- err

				if err != nil {
					return fmt.Errorf("can't download %s: %w", ent1.body.Name(), err)
				}

				logger.Info(fmt.Sprintf("Indexing %s", ent1.body.Name()))
				return freezeblocks.BodiesIdx(ctx,
					filepath.Join(c.session1.LocalFsRoot(), ent1.body.Name()), ent1.from, c.session1.LocalFsRoot(), nil, log.LvlDebug, logger)
			})

			g.Go(func() error {
				logger.Info(fmt.Sprintf("Downloading %s", ent1.transactions.Name()), "entry", fmt.Sprint(i1+1, "/", len(f1ents)))
				_, err := c.session1.Download(ctx, ent1.transactions.Name())

				if err != nil {
					return fmt.Errorf("can't download %s: %w", ent1.transactions.Name(), err)
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case err = <-b1err:
					if err != nil {
						return fmt.Errorf("can't create transaction index: no bodies: %w", err)
					}
				}

				logger.Info(fmt.Sprintf("Indexing %s", ent1.transactions.Name()))
				return freezeblocks.TransactionsIdx(ctx, c.chainConfig(), c.loc1.version, ent1.from, ent1.to,
					c.session1.LocalFsRoot(), c.session1.LocalFsRoot(), nil, log.LvlDebug, logger)
			})

			b2err := make(chan error, 1)

			g.Go(func() error {
				logger.Info(fmt.Sprintf("Downloading %s", ent2.body.Name()), "entry", fmt.Sprint(i2+1, "/", len(f2ents)))
				_, err := c.session2.Download(ctx, ent2.body.Name())

				b2err <- err

				if err != nil {
					return fmt.Errorf("can't download %s: %w", ent2.body.Name(), err)
				}

				logger.Info(fmt.Sprintf("Indexing %s", ent2.body.Name()))
				return freezeblocks.BodiesIdx(ctx,
					filepath.Join(c.session2.LocalFsRoot(), ent2.body.Name()), ent2.from, c.session1.LocalFsRoot(), nil, log.LvlDebug, logger)
			})

			g.Go(func() error {
				logger.Info(fmt.Sprintf("Downloading %s", ent2.transactions.Name()), "entry", fmt.Sprint(i2+1, "/", len(f2ents)))
				_, err := c.session2.Download(ctx, ent2.transactions.Name())

				if err != nil {
					return fmt.Errorf("can't download %s: %w", ent2.transactions.Name(), err)
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case err = <-b2err:
					if err != nil {
						return fmt.Errorf("can't create transaction index: no bodies: %w", err)
					}
				}

				logger.Info(fmt.Sprintf("Indexing %s", ent2.transactions.Name()))
				return freezeblocks.TransactionsIdx(ctx, c.chainConfig(), c.loc2.version, ent2.from, ent2.to,
					c.session2.LocalFsRoot(), c.session2.LocalFsRoot(), nil, log.LvlDebug, logger)
			})

			if err := g.Wait(); err != nil {
				return err
			}

			logger.Info(fmt.Sprintf("Comparing %s %s", ent1.body.Name(), ent2.body.Name()))

			f1snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
				Enabled:      true,
				Produce:      false,
				NoDownloader: true,
			}, c.session1.LocalFsRoot(), c.loc1.version, logger)

			f1snaps.ReopenList([]string{ent1.body.Name(), ent1.transactions.Name()}, false)

			f2snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
				Enabled:      true,
				Produce:      false,
				NoDownloader: true,
			}, c.session2.LocalFsRoot(), c.loc2.version, logger)

			f2snaps.ReopenList([]string{ent2.body.Name(), ent2.transactions.Name()}, false)

			blockReader1 := freezeblocks.NewBlockReader(f1snaps, nil)
			blockReader2 := freezeblocks.NewBlockReader(f2snaps, nil)

			err := func() error {

				for i := uint64(ent1.from); i < ent1.to; i++ {
					body1, err := blockReader1.BodyWithTransactions(ctx, nil, common.Hash{}, i)

					if err != nil {
						return fmt.Errorf("%d: can't get body 1: %w", err)
					}

					body2, err := blockReader2.BodyWithTransactions(ctx, nil, common.Hash{}, i)

					if err != nil {
						return fmt.Errorf("%d: can't get body 2: %w", err)
					}

					var b1buf, b2buf bytes.Buffer

					body1.EncodeRLP(&b1buf)
					body2.EncodeRLP(&b2buf)

					if !bytes.Equal(b1buf.Bytes(), b2buf.Bytes()) {
						return fmt.Errorf("%d: bodies do not match", i)
					}
				}

				return nil
			}()

			files := f1snaps.OpenFiles()
			f1snaps.Close()

			files = append(files, f2snaps.OpenFiles()...)
			f2snaps.Close()

			for _, file := range files {
				os.Remove(file)
			}

			if err != nil {
				return err
			}
		}
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

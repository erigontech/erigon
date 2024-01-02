package simulator

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

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
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

// The code in this file is taken from cmd/snapshots - which is yet to be merged
// to devel - once tthat is done this file can be removed

type TorrentClient struct {
	*torrent.Client
	cfg   *torrent.ClientConfig
	items map[string]snapcfg.PreverifiedItem
}

func NewTorrentClient(ctx context.Context, chain string, torrentDir string, logger log.Logger) (*TorrentClient, error) {

	relativeDataDir := torrentDir
	if torrentDir != "" {
		var err error
		absdatadir, err := filepath.Abs(torrentDir)
		if err != nil {
			panic(err)
		}
		torrentDir = absdatadir
	}

	dirs := datadir.Dirs{
		RelativeDataDir: relativeDataDir,
		DataDir:         torrentDir,
		Snap:            torrentDir,
	}

	webseedsList := common.CliString2Array(utils.WebSeedsFlag.Value)

	if known, ok := snapcfg.KnownWebseeds[chain]; ok {
		webseedsList = append(webseedsList, known...)
	}

	var downloadRate, uploadRate datasize.ByteSize

	if err := downloadRate.UnmarshalText([]byte(utils.TorrentDownloadRateFlag.Value)); err != nil {
		return nil, err
	}

	if err := uploadRate.UnmarshalText([]byte(utils.TorrentUploadRateFlag.Value)); err != nil {
		return nil, err
	}

	logLevel, _, err := downloadercfg.Int2LogLevel(utils.TorrentVerbosityFlag.Value)

	if err != nil {
		return nil, err
	}

	version := "erigon: " + params.VersionWithCommit(params.GitCommit)

	cfg, err := downloadercfg.New(dirs, version, logLevel, downloadRate, uploadRate,
		utils.TorrentPortFlag.Value, utils.TorrentConnsPerFileFlag.Value, 0, nil, webseedsList, chain)

	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(torrentDir, 0755); err != nil {
		return nil, err
	}

	cfg.ClientConfig.DataDir = torrentDir

	cfg.ClientConfig.PieceHashersPerTorrent = 32 * runtime.NumCPU()
	cfg.ClientConfig.DisableIPv6 = utils.DisableIPV6.Value
	cfg.ClientConfig.DisableIPv4 = utils.DisableIPV4.Value

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

	items := map[string]snapcfg.PreverifiedItem{}
	for _, it := range snapcfg.KnownCfg(chain, 0).Preverified {
		items[it.Name] = it
	}

	return &TorrentClient{cli, cfg.ClientConfig, items}, nil
}

func (s *TorrentClient) LocalFsRoot() string {
	return s.cfg.DataDir
}

func (s *TorrentClient) Download(ctx context.Context, files ...string) error {
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

				for _, t := range s.Torrents() {
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

				t, _, err := s.AddTorrentSpec(spec)
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

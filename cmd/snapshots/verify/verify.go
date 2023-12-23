package verify

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cmd/snapshots/flags"
	"github.com/ledgerwatch/erigon/cmd/snapshots/sync"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/urfave/cli/v2"
)

var (
	SrcFlag = cli.StringFlag{
		Name:     "src",
		Usage:    `Source location for verification files (torrent,hash,manifest)`,
		Required: false,
	}
	DstFlag = cli.StringFlag{
		Name:     "dst",
		Usage:    `Destination location containiong copies to be verified`,
		Required: true,
	}
	ChainFlag = cli.StringFlag{
		Name:     "chain",
		Usage:    `The chain being validated, required if not included src or dst naming`,
		Required: false,
	}
	TorrentsFlag = cli.BoolFlag{
		Name:     "torrents",
		Usage:    `Verify against torrent files`,
		Required: false,
	}

	HashesFlag = cli.BoolFlag{
		Name:     "hashes",
		Usage:    `Verify against hash .toml contents`,
		Required: false,
	}

	ManifestFlag = cli.BoolFlag{
		Name:     "manifest",
		Usage:    `Verify against manifest .txt contents`,
		Required: false,
	}
)

var Command = cli.Command{
	Action:    verify,
	Name:      "verify",
	Usage:     "verify snapshot segments against hashes and torrents",
	ArgsUsage: "<start block> <end block>",
	Flags: []cli.Flag{
		&SrcFlag,
		&DstFlag,
		&ChainFlag,
		&flags.SegTypes,
		&TorrentsFlag,
		&HashesFlag,
		&ManifestFlag,
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

func verify(cliCtx *cli.Context) error {
	logger := sync.Logger(cliCtx.Context)

	logger.Info("Starting verify")

	var src, dst *sync.Locator
	var err error

	var rcCli *downloader.RCloneClient
	var torrentCli *sync.TorrentClient

	if src, err = sync.ParseLocator(cliCtx.String(SrcFlag.Name)); err != nil {
		return err
	}

	if dst, err = sync.ParseLocator(cliCtx.String(DstFlag.Name)); err != nil {
		return err
	}

	chain := cliCtx.String(ChainFlag.Name)

	switch dst.LType {
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

		if len(chain) == 0 {
			chain = dst.Chain
		}
	}

	switch src.LType {
	case sync.TorrentFs:
		if torrentCli == nil {
			torrentCli, err = sync.NewTorrentClient(cliCtx, dst.Chain)
			if err != nil {
				return fmt.Errorf("can't create torrent: %w", err)
			}
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

		if len(chain) == 0 {
			chain = src.Chain
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

	if cliCtx.Args().Len() > 0 {
		if firstBlock, err = strconv.ParseUint(cliCtx.Args().Get(0), 10, 64); err != nil {
			return err
		}
	}

	if cliCtx.Args().Len() > 1 {
		if lastBlock, err = strconv.ParseUint(cliCtx.Args().Get(1), 10, 64); err != nil {
			return err
		}
	}

	var srcSession sync.DownloadSession
	var dstSession sync.DownloadSession

	dataDir := cliCtx.String(utils.DataDirFlag.Name)
	var tempDir string

	if len(dataDir) == 0 {
		dataDir, err := os.MkdirTemp("", "snapshot-verify-")
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

		if dst.LType == sync.RemoteFs {
			dstSession, err = rcCli.NewSession(cliCtx.Context, filepath.Join(tempDir, "dst"), dst.Src+":"+dst.Root)

			if err != nil {
				return err
			}
		}
	}

	if torrentCli != nil {
		if src != nil && src.LType == sync.TorrentFs {
			srcSession = sync.NewTorrentSession(torrentCli, chain)
		}

		if dst.LType == sync.TorrentFs {
			dstSession = sync.NewTorrentSession(torrentCli, chain)
		}
	}

	if src != nil && srcSession == nil {
		return fmt.Errorf("no src session established")
	}

	if dstSession == nil {
		return fmt.Errorf("no dst session established")
	}

	if srcSession == nil {
		srcSession = dstSession
	}

	return verfifySnapshots(srcSession, dstSession, firstBlock, lastBlock, snapTypes, torrents, hashes, manifest)
}

func verfifySnapshots(srcSession sync.DownloadSession, rcSession sync.DownloadSession, from uint64, to uint64, snapTypes []snaptype.Type, torrents, hashes, manifest bool) error {
	return fmt.Errorf("TODO")
}

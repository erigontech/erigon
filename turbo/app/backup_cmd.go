package app

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/turbo/backup"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

// nolint
var backupCommand = cli.Command{
	Name: "backup",
	Description: `Backup all databases of Erigon. 
Can do backup without stopping Erigon.
Limitations: 
- no support of Consensus DB (copy it manually if you need). Possible to implement in future.
- no support of datadir/snapshots folder. Possible to implement in future. Can copy it manually or rsync or symlink/mount.
- way to pipe output to compressor (lz4/zstd). Can compress target floder later or use zfs-with-enabled-compression.
- jwt tocken: copy it manually - if need. 
- no support of SentryDB (datadir/nodes folder). Because seems no much reason to backup it.

Example: erigon backup --datadir=<your_datadir> --to.datadir=<backup_datadir> 
`,
	Action: doBackup,
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&ToDatadirFlag,
		&BackupToPageSizeFlag,
		&BackupLabelsFlag,
		&BackupTablesFlag,
		&WarmupThreadsFlag,
	}, debug.Flags, logging.Flags),
}

var (
	ToDatadirFlag = flags.DirectoryFlag{
		Name:     "to.datadir",
		Usage:    "Target datadir",
		Required: true,
	}
	BackupLabelsFlag = cli.StringFlag{
		Name:  "lables",
		Usage: "Name of component to backup. Example: chaindata,txpool,downloader",
	}
	BackupTablesFlag = cli.StringFlag{
		Name:  "tables",
		Usage: "One of: PlainState,HashedState",
	}
	BackupToPageSizeFlag = cli.StringFlag{
		Name:  "to.pagesize",
		Usage: utils.DbPageSizeFlag.Usage,
	}
	WarmupThreadsFlag = cli.Uint64Flag{
		Name: "warmup.threads",
		Usage: `Erigon's db works as blocking-io: means it stops when read from disk. 
It means backup speed depends on 'disk latency' (not throughput). 
Can spawn many threads which will read-ahead the data and bring it to OS's PageCache. 
CloudDrives (and ssd) have bad-latency and good-parallel-throughput - then having >1k of warmup threads will help.`,
		Value: uint64(backup.ReadAheadThreads),
	}
)

func doBackup(cliCtx *cli.Context) error {
	defer log.Info("backup done")

	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	toDirs := datadir.New(cliCtx.String(ToDatadirFlag.Name))

	var targetPageSize datasize.ByteSize
	if cliCtx.IsSet(BackupToPageSizeFlag.Name) {
		targetPageSize = flags.DBPageSizeFlagUnmarshal(cliCtx, BackupToPageSizeFlag.Name, BackupToPageSizeFlag.Usage)
	}

	var lables = []kv.Label{kv.ChainDB, kv.TxPoolDB, kv.DownloaderDB}
	if cliCtx.IsSet(BackupToPageSizeFlag.Name) {
		lables = lables[:0]
		for _, l := range utils.SplitAndTrim(cliCtx.String(BackupLabelsFlag.Name)) {
			lables = append(lables, kv.UnmarshalLabel(l))
		}
	}

	var tables []string
	if cliCtx.IsSet(BackupTablesFlag.Name) {
		tables = utils.SplitAndTrim(cliCtx.String(BackupTablesFlag.Name))
	}

	readAheadThreads := backup.ReadAheadThreads
	if cliCtx.IsSet(WarmupThreadsFlag.Name) {
		readAheadThreads = int(cliCtx.Uint64(WarmupThreadsFlag.Name))
	}

	//kv.SentryDB no much reason to backup
	//TODO: add support of kv.ConsensusDB
	for _, label := range lables {
		var from, to string
		switch label {
		case kv.ChainDB:
			from, to = dirs.Chaindata, toDirs.Chaindata
		case kv.TxPoolDB:
			from, to = dirs.TxPool, toDirs.TxPool
		case kv.DownloaderDB:
			from, to = filepath.Join(dirs.Snap, "db"), filepath.Join(toDirs.Snap, "db")
		default:
			panic(fmt.Sprintf("unexpected: %+v", label))
		}

		if !dir.Exist(from) {
			continue
		}

		if len(tables) == 0 { // if not partial backup - just drop target dir, to make backup more compact/fast (instead of clean tables)
			if err := os.RemoveAll(to); err != nil {
				return fmt.Errorf("mkdir: %w, %s", err, to)
			}
		}
		if err := os.MkdirAll(to, 0740); err != nil { //owner: rw, group: r, others: -
			return fmt.Errorf("mkdir: %w, %s", err, to)
		}
		log.Info("[backup] start", "label", label)
		fromDB, toDB := backup.OpenPair(from, to, label, targetPageSize)
		if err := backup.Kv2kv(ctx, fromDB, toDB, nil, readAheadThreads); err != nil {
			return err
		}
	}

	return nil
}

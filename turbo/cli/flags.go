package cli

import (
	"fmt"
	"strings"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/spf13/pflag"
	"github.com/urfave/cli"
)

var (
	DatabaseFlag = cli.StringFlag{
		Name:  "database",
		Usage: "Which database software to use? Currently supported values: lmdb|mdbx",
		Value: "lmdb",
	}
	DatabaseVerbosityFlag = cli.IntFlag{
		Name:  "database.verbosity",
		Usage: "Enabling internal db logs. Very high verbosity levels may require recompile db.",
		Value: -1,
	}
	BatchSizeFlag = cli.StringFlag{
		Name:  "batchSize",
		Usage: "Batch size for the execution stage",
		Value: "512M",
	}
	EtlBufferSizeFlag = cli.StringFlag{
		Name:  "etl.bufferSize",
		Usage: "Buffer size for ETL operations.",
		Value: etl.BufferOptimalSize.String(),
	}

	PrivateApiAddr = cli.StringFlag{
		Name:  "private.api.addr",
		Usage: "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface",
		Value: "",
	}

	PrivateApiRateLimit = cli.IntFlag{
		Name:  "private.api.ratelimit",
		Usage: "Amount of requests server handle simultaneously - requests over this limit will wait. Increase it - if clients see 'request timeout' while server load is low - it means your 'hot data' is small or have much RAM. ",
		Value: 500,
	}

	DownloadV2Flag = cli.BoolFlag{
		Name:  "download.v2",
		Usage: "enable experimental downloader v2",
	}

	StorageModeFlag = cli.StringFlag{
		Name: "storage-mode",
		Usage: `Configures the storage mode of the app:
* h - write history to the DB
* r - write receipts to the DB
* t - write tx lookup index to the DB`,
		Value: ethdb.DefaultStorageMode.ToString(),
	}
	SnapshotModeFlag = cli.StringFlag{
		Name: "snapshot.mode",
		Usage: `Configures the snapshot mode of the app:
* h - download headers snapshot
* b - download bodies snapshot
* s - download state snapshot
* r - download receipts snapshot
`,
		Value: snapshotsync.DefaultSnapshotMode.ToString(),
	}
	SeedSnapshotsFlag = cli.BoolTFlag{
		Name:  "snapshot.seed",
		Usage: `Seed snapshot seeding(default: true)`,
	}
	//todo replace to BoolT
	SnapshotDatabaseLayoutFlag = cli.BoolFlag{
		Name:  "snapshot.layout",
		Usage: `Enable snapshot db layout(default: false)`,
	}

	ExternalSnapshotDownloaderAddrFlag = cli.StringFlag{
		Name:  "snapshot.downloader.addr",
		Usage: `enable external snapshot downloader`,
	}

	// LMDB flags
	LMDBMapSizeFlag = cli.StringFlag{
		Name:  "lmdb.mapSize",
		Usage: "Sets Memory map size. Lower it if you have issues with opening the DB",
		Value: ethdb.LMDBDefaultMapSize.String(),
	}

	// mTLS flags
	TLSFlag = cli.BoolFlag{
		Name:  "tls",
		Usage: "Enable TLS handshake",
	}
	TLSCertFlag = cli.StringFlag{
		Name:  "tls.cert",
		Usage: "Specify certificate",
		Value: "",
	}
	TLSKeyFlag = cli.StringFlag{
		Name:  "tls.key",
		Usage: "Specify key file",
		Value: "",
	}
	TLSCACertFlag = cli.StringFlag{
		Name:  "tls.cacert",
		Usage: "Specify certificate authority",
		Value: "",
	}
	SilkwormFlag = cli.StringFlag{
		Name:  "silkworm",
		Usage: "File path of libsilkworm_tg_api dynamic library (default = do not use Silkworm)",
		Value: "",
	}
)

func ApplyFlagsForEthConfig(ctx *cli.Context, cfg *ethconfig.Config) {
	cfg.EnableDownloadV2 = ctx.GlobalBool(DownloadV2Flag.Name)
	mode, err := ethdb.StorageModeFromString(ctx.GlobalString(StorageModeFlag.Name))
	if err != nil {
		utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
	}
	cfg.StorageMode = mode
	snMode, err := snapshotsync.SnapshotModeFromString(ctx.GlobalString(SnapshotModeFlag.Name))
	if err != nil {
		utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
	}
	cfg.SnapshotMode = snMode
	cfg.SnapshotSeeding = ctx.GlobalBool(SeedSnapshotsFlag.Name)
	cfg.SnapshotLayout = ctx.GlobalBool(SnapshotDatabaseLayoutFlag.Name)

	if ctx.GlobalString(BatchSizeFlag.Name) != "" {
		err := cfg.BatchSize.UnmarshalText([]byte(ctx.GlobalString(BatchSizeFlag.Name)))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
	}

	if ctx.GlobalString(EtlBufferSizeFlag.Name) != "" {
		sizeVal := datasize.ByteSize(0)
		size := &sizeVal
		err := size.UnmarshalText([]byte(ctx.GlobalString(EtlBufferSizeFlag.Name)))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
		etl.BufferOptimalSize = *size
	}

	cfg.ExternalSnapshotDownloaderAddr = ctx.GlobalString(ExternalSnapshotDownloaderAddrFlag.Name)
}
func ApplyFlagsForEthConfigCobra(f *pflag.FlagSet, cfg *ethconfig.Config) {
	if v := f.String(StorageModeFlag.Name, StorageModeFlag.Value, StorageModeFlag.Usage); v != nil {
		mode, err := ethdb.StorageModeFromString(*v)
		if err != nil {
			utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
		}
		cfg.StorageMode = mode
	}
	if v := f.String(SnapshotModeFlag.Name, SnapshotModeFlag.Value, SnapshotModeFlag.Usage); v != nil {
		snMode, err := snapshotsync.SnapshotModeFromString(*v)
		if err != nil {
			utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
		}
		cfg.SnapshotMode = snMode
	}
	if v := f.Bool(SeedSnapshotsFlag.Name, false, SeedSnapshotsFlag.Usage); v != nil {
		cfg.SnapshotSeeding = *v
	}
	if v := f.String(BatchSizeFlag.Name, BatchSizeFlag.Value, BatchSizeFlag.Usage); v != nil {
		err := cfg.BatchSize.UnmarshalText([]byte(*v))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
	}
	if v := f.String(EtlBufferSizeFlag.Name, EtlBufferSizeFlag.Value, EtlBufferSizeFlag.Usage); v != nil {
		sizeVal := datasize.ByteSize(0)
		size := &sizeVal
		err := size.UnmarshalText([]byte(*v))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
		etl.BufferOptimalSize = *size
	}

	if v := f.String(ExternalSnapshotDownloaderAddrFlag.Name, ExternalSnapshotDownloaderAddrFlag.Value, ExternalSnapshotDownloaderAddrFlag.Usage); v != nil {
		cfg.ExternalSnapshotDownloaderAddr = *v
	}
}

func ApplyFlagsForNodeConfig(ctx *cli.Context, cfg *node.Config) {

	setPrivateApi(ctx, cfg)
	cfg.DatabaseVerbosity = ethdb.DBVerbosityLvl(ctx.GlobalInt(DatabaseVerbosityFlag.Name))

	databaseFlag := ctx.GlobalString(DatabaseFlag.Name)
	cfg.MDBX = strings.EqualFold(databaseFlag, "mdbx") //case insensitive
	cfg.LMDB = strings.EqualFold(databaseFlag, "lmdb") //case insensitive
	if cfg.LMDB && ctx.GlobalString(LMDBMapSizeFlag.Name) != "" {
		err := cfg.LMDBMapSize.UnmarshalText([]byte(ctx.GlobalString(LMDBMapSizeFlag.Name)))
		if err != nil {
			log.Error("Invalid LMDB map size provided. Will use defaults",
				"lmdb.mapSize", ethdb.LMDBDefaultMapSize.HumanReadable(),
				"err", err,
			)
		} else {
			if cfg.LMDBMapSize < 1*datasize.GB {
				log.Error("Invalid LMDB map size provided. Will use defaults",
					"lmdb.mapSize", ethdb.LMDBDefaultMapSize.HumanReadable(),
					"err", "the value should be at least 1 GB",
				)
			}
		}
	}

}

// setPrivateApi populates configuration fields related to the remote
// read-only interface to the databae
func setPrivateApi(ctx *cli.Context, cfg *node.Config) {
	cfg.PrivateApiAddr = ctx.GlobalString(PrivateApiAddr.Name)
	cfg.PrivateApiRateLimit = uint32(ctx.GlobalUint64(PrivateApiRateLimit.Name))
	maxRateLimit := uint32(ethdb.ReadersLimit - 16)
	if cfg.PrivateApiRateLimit > maxRateLimit {
		log.Warn("private.api.ratelimit is too big", "force", maxRateLimit)
		cfg.PrivateApiRateLimit = maxRateLimit
	}
	if ctx.GlobalBool(TLSFlag.Name) {
		certFile := ctx.GlobalString(TLSCertFlag.Name)
		keyFile := ctx.GlobalString(TLSKeyFlag.Name)
		if certFile == "" {
			log.Warn("Could not establish TLS grpc: missing certificate")
			return
		} else if keyFile == "" {
			log.Warn("Could not establish TLS grpc: missing key file")
			return
		}
		cfg.TLSConnection = true
		cfg.TLSCertFile = certFile
		cfg.TLSKeyFile = keyFile
		cfg.TLSCACert = ctx.GlobalString(TLSCACertFlag.Name)
	}
}

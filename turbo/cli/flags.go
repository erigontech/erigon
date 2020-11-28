package cli

import (
	"fmt"
	"strings"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/urfave/cli"
)

var (
	DatabaseFlag = cli.StringFlag{
		Name:  "database",
		Usage: "Which database software to use? Currently supported values: lmdb|mdbx",
		Value: "lmdb",
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
		Usage: `Configures the storage mode of the app:
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
	LMDBMaxFreelistReuseFlag = cli.UintFlag{
		Name:  "lmdb.maxFreelistReuse",
		Usage: "Find a big enough contiguous page range for large values in freelist is hard just allocate new pages and even don't try to search if value is bigger than this limit. Measured in pages.",
		Value: ethdb.LMDBDefaultMaxFreelistReuse,
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

func ApplyFlagsForEthConfig(ctx *cli.Context, cfg *eth.Config) {
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

func ApplyFlagsForNodeConfig(ctx *cli.Context, cfg *node.Config) {

	setPrivateApi(ctx, cfg)

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

	if cfg.LMDB {
		cfg.LMDBMaxFreelistReuse = ctx.GlobalUint(LMDBMaxFreelistReuseFlag.Name)
		if cfg.LMDBMaxFreelistReuse < 16 {
			log.Error("Invalid LMDB MaxFreelistReuse provided. Will use defaults",
				"lmdb.maxFreelistReuse", ethdb.LMDBDefaultMaxFreelistReuse,
				"err", "the value should be at least 16",
			)
		}
	}
}

// setPrivateApi populates configuration fields related to the remote
// read-only interface to the databae
func setPrivateApi(ctx *cli.Context, cfg *node.Config) {
	cfg.PrivateApiAddr = ctx.GlobalString(PrivateApiAddr.Name)
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

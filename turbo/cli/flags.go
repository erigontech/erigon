package cli

import (
	"fmt"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/spf13/pflag"
	"github.com/urfave/cli"
)

var (
	DatabaseVerbosityFlag = cli.IntFlag{
		Name:  "database.verbosity",
		Usage: "Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning.",
		Value: 2,
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

	MaxPeersFlag = cli.IntFlag{
		Name:  "maxpeers",
		Usage: "Maximum number of network peers (network disabled if set to 0)",
		Value: node.DefaultConfig.P2P.MaxPeers,
	}

	StorageModeFlag = cli.StringFlag{
		Name: "storage-mode",
		Usage: `Configures the storage mode of the app:
* h - write history to the DB
* r - write receipts to the DB
* t - write tx lookup index to the DB
* c - write call traces index to the DB,
* e - write TEVM translated code to the DB`,
		Value: "default",
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
	StateStreamFlag = cli.BoolFlag{
		Name:  "state.stream",
		Usage: "Enable streaming of state changes from core to RPC daemon",
	}
)

func ApplyFlagsForEthConfig(ctx *cli.Context, cfg *ethconfig.Config) {
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
	cfg.StateStream = ctx.GlobalBool(StateStreamFlag.Name)
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
	if v := f.Bool(StateStreamFlag.Name, false, StateStreamFlag.Usage); v != nil {
		cfg.StateStream = *v
	}
}

func ApplyFlagsForNodeConfig(ctx *cli.Context, cfg *node.Config) {
	setPrivateApi(ctx, cfg)
	cfg.DatabaseVerbosity = ethdb.DBVerbosityLvl(ctx.GlobalInt(DatabaseVerbosityFlag.Name))
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

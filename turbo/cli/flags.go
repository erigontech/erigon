package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
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
	BlockDownloaderWindowFlag = cli.IntFlag{
		Name:  "blockDownloaderWindow",
		Usage: "Outstanding limit of block bodies being downloaded",
		Value: 32768,
	}

	PrivateApiAddr = cli.StringFlag{
		Name:  "private.api.addr",
		Usage: "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface",
		Value: "127.0.0.1:9090",
	}

	PrivateApiRateLimit = cli.IntFlag{
		Name:  "private.api.ratelimit",
		Usage: "Amount of requests server handle simultaneously - requests over this limit will wait. Increase it - if clients see 'request timeout' while server load is low - it means your 'hot data' is small or have much RAM. ",
		Value: kv.ReadersLimit - 128,
	}

	MaxPeersFlag = cli.IntFlag{
		Name:  "maxpeers",
		Usage: "Maximum number of network peers (network disabled if set to 0)",
		Value: node.DefaultConfig.P2P.MaxPeers,
	}

	PruneFlag = cli.StringFlag{
		Name: "prune",
		Usage: `Choose which ancient data delete from DB: 
	h - prune history (ChangeSets, HistoryIndices - used by historical state access)
	r - prune receipts (Receipts, Logs, LogTopicIndex, LogAddressIndex - used by eth_getLogs and similar RPC methods)
	t - prune transaction by it's hash index
	c - prune call traces (used by trace_* methods)
	Does delete data older than 90K block (can set another value by '--prune.*.older' flags). 
	If item is NOT in the list - means NO pruning for this data.s
	Example: --prune=hrtc`,
		Value: "disabled",
	}
	PruneHistoryFlag = cli.Uint64Flag{
		Name:  "prune.h.older",
		Usage: `Prune data after this amount of blocks (if --prune flag has 'h', then default is 90K)`,
	}
	PruneReceiptFlag = cli.Uint64Flag{
		Name:  "prune.r.older",
		Usage: `Prune data after this amount of blocks (if --prune flag has 'r', then default is 90K)`,
	}
	PruneTxIndexFlag = cli.Uint64Flag{
		Name:  "prune.t.older",
		Usage: `Prune data after this amount of blocks (if --prune flag has 't', then default is 90K)`,
	}
	PruneCallTracesFlag = cli.Uint64Flag{
		Name:  "prune.c.older",
		Usage: `Prune data after this amount of blocks (if --prune flag has 'c', then default is 90K)`,
	}
	ExperimentsFlag = cli.StringFlag{
		Name: "experiments",
		Usage: `Enable some experimental stages:
* tevm - write TEVM translated code to the DB`,
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

	// Throttling Flags
	SyncLoopThrottleFlag = cli.StringFlag{
		Name:  "sync.loop.throttle",
		Usage: "Sets the minimum time between sync loop starts (e.g. 1h30m, default is none)",
		Value: "",
	}

	BadBlockFlag = cli.IntFlag{
		Name:  "bad.block",
		Usage: "Marks block with given number bad and forces initial reorg before normal staged sync",
		Value: 0,
	}
)

func ApplyFlagsForEthConfig(ctx *cli.Context, cfg *ethconfig.Config) {
	mode, err := prune.FromCli(
		ctx.GlobalString(PruneFlag.Name),
		ctx.GlobalUint64(PruneHistoryFlag.Name),
		ctx.GlobalUint64(PruneReceiptFlag.Name),
		ctx.GlobalUint64(PruneTxIndexFlag.Name),
		ctx.GlobalUint64(PruneCallTracesFlag.Name),
		strings.Split(ctx.GlobalString(ExperimentsFlag.Name), ","),
	)
	if err != nil {
		utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
	}
	cfg.Prune = mode

	snMode, err := snapshotsync.SnapshotModeFromString(ctx.GlobalString(SnapshotModeFlag.Name))
	if err != nil {
		utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
	}
	cfg.Snapshot.Mode = snMode
	cfg.Snapshot.Seeding = ctx.GlobalBool(SeedSnapshotsFlag.Name)
	cfg.Snapshot.Enabled = ctx.GlobalBool(SnapshotDatabaseLayoutFlag.Name)

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
	cfg.BlockDownloaderWindow = ctx.GlobalInt(BlockDownloaderWindowFlag.Name)

	if ctx.GlobalString(SyncLoopThrottleFlag.Name) != "" {
		syncLoopThrottle, err := time.ParseDuration(ctx.GlobalString(SyncLoopThrottleFlag.Name))
		if err != nil {
			utils.Fatalf("Invalid time duration provided in %s: %v", SyncLoopThrottleFlag.Name, err)
		}
		cfg.SyncLoopThrottle = syncLoopThrottle
	}
	cfg.BadBlock = uint64(ctx.GlobalInt(BadBlockFlag.Name))
}

func ApplyFlagsForEthConfigCobra(f *pflag.FlagSet, cfg *ethconfig.Config) {
	if v := f.String(PruneFlag.Name, PruneFlag.Value, PruneFlag.Usage); v != nil {
		var experiments []string
		if exp := f.StringSlice(ExperimentsFlag.Name, nil, ExperimentsFlag.Usage); exp != nil {
			experiments = *exp
		}
		var exactH, exactR, exactT, exactC uint64
		if v := f.Uint64(PruneHistoryFlag.Name, PruneHistoryFlag.Value, PruneHistoryFlag.Usage); v != nil {
			exactH = *v
		}
		if v := f.Uint64(PruneReceiptFlag.Name, PruneReceiptFlag.Value, PruneReceiptFlag.Usage); v != nil {
			exactR = *v
		}
		if v := f.Uint64(PruneTxIndexFlag.Name, PruneTxIndexFlag.Value, PruneTxIndexFlag.Usage); v != nil {
			exactT = *v
		}
		if v := f.Uint64(PruneCallTracesFlag.Name, PruneCallTracesFlag.Value, PruneCallTracesFlag.Usage); v != nil {
			exactC = *v
		}
		mode, err := prune.FromCli(*v, exactH, exactR, exactT, exactC, experiments)
		if err != nil {
			utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
		}
		cfg.Prune = mode
	}
	if v := f.String(SnapshotModeFlag.Name, SnapshotModeFlag.Value, SnapshotModeFlag.Usage); v != nil {
		snMode, err := snapshotsync.SnapshotModeFromString(*v)
		if err != nil {
			utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
		}
		cfg.Snapshot.Mode = snMode
	}
	if v := f.Bool(SeedSnapshotsFlag.Name, false, SeedSnapshotsFlag.Usage); v != nil {
		cfg.Snapshot.Seeding = *v
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
	cfg.DatabaseVerbosity = kv.DBVerbosityLvl(ctx.GlobalInt(DatabaseVerbosityFlag.Name))
}

// setPrivateApi populates configuration fields related to the remote
// read-only interface to the databae
func setPrivateApi(ctx *cli.Context, cfg *node.Config) {
	cfg.PrivateApiAddr = ctx.GlobalString(PrivateApiAddr.Name)
	cfg.PrivateApiRateLimit = uint32(ctx.GlobalUint64(PrivateApiRateLimit.Name))
	maxRateLimit := uint32(kv.ReadersLimit - 128) // leave some readers for P2P
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

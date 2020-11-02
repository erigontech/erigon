package commands

import (
	"context"
	"fmt"
	"net"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/c2h5oh/datasize"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/migrations"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

var cmdStageSenders = &cobra.Command{
	Use:   "stage_senders",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		if err := stageSenders(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageExec = &cobra.Command{
	Use:   "stage_exec",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		if err := stageExec(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageIHash = &cobra.Command{
	Use:   "stage_ih",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		if err := stageIHash(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageHashState = &cobra.Command{
	Use:   "stage_hash_state",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		if err := stageHashState(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageHistory = &cobra.Command{
	Use:   "stage_history",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		if err := stageHistory(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdLogIndex = &cobra.Command{
	Use:   "stage_log_index",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		if err := stageLogIndex(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdCallTraces = &cobra.Command{
	Use:   "stage_call_traces",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		if err := stageCallTraces(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageTxLookup = &cobra.Command{
	Use:   "stage_tx_lookup",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		if err := stageTxLookup(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}
var cmdPrintStages = &cobra.Command{
	Use:   "print_stages",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, false)
		defer db.Close()

		if err := printAllStages(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdPrintMigrations = &cobra.Command{
	Use:   "print_migrations",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, false)
		defer db.Close()
		if err := printAppliedMigrations(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdRemoveMigration = &cobra.Command{
	Use:   "remove_migration",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, false)
		defer db.Close()
		if err := removeMigration(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdRunMigrations = &cobra.Command{
	Use:   "run_migrations",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		db := openDatabase(chaindata, false)
		defer db.Close()
		// Nothing to do, migrations will be applied automatically
		return nil
	},
}

var cmdShardDispatcher = &cobra.Command{
	Use:   "shard_dispatcher",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		if err := shardDispatcher(ctx); err != nil {
			log.Error("Starting Shard Dispatcher failed", "err", err)
			return err
		}
		return nil
	},
}

func init() {
	withChaindata(cmdPrintStages)
	withLmdbFlags(cmdPrintStages)
	rootCmd.AddCommand(cmdPrintStages)

	withChaindata(cmdStageSenders)
	withLmdbFlags(cmdStageSenders)
	withReset(cmdStageSenders)
	withBlock(cmdStageSenders)
	withUnwind(cmdStageSenders)
	withDatadir(cmdStageSenders)

	rootCmd.AddCommand(cmdStageSenders)

	withChaindata(cmdStageExec)
	withLmdbFlags(cmdStageExec)
	withReset(cmdStageExec)
	withBlock(cmdStageExec)
	withUnwind(cmdStageExec)
	withBatchSize(cmdStageExec)

	rootCmd.AddCommand(cmdStageExec)

	withChaindata(cmdStageIHash)
	withLmdbFlags(cmdStageIHash)
	withReset(cmdStageIHash)
	withBlock(cmdStageIHash)
	withUnwind(cmdStageIHash)
	withDatadir(cmdStageIHash)

	rootCmd.AddCommand(cmdStageIHash)

	withChaindata(cmdStageHashState)
	withLmdbFlags(cmdStageHashState)
	withReset(cmdStageHashState)
	withBlock(cmdStageHashState)
	withUnwind(cmdStageHashState)
	withDatadir(cmdStageHashState)

	rootCmd.AddCommand(cmdStageHashState)

	withChaindata(cmdStageHistory)
	withLmdbFlags(cmdStageHistory)
	withReset(cmdStageHistory)
	withBlock(cmdStageHistory)
	withUnwind(cmdStageHistory)
	withDatadir(cmdStageHistory)

	rootCmd.AddCommand(cmdStageHistory)

	withChaindata(cmdLogIndex)
	withLmdbFlags(cmdLogIndex)
	withReset(cmdLogIndex)
	withBlock(cmdLogIndex)
	withUnwind(cmdLogIndex)
	withDatadir(cmdLogIndex)

	rootCmd.AddCommand(cmdLogIndex)

	withChaindata(cmdCallTraces)
	withLmdbFlags(cmdCallTraces)
	withReset(cmdCallTraces)
	withBlock(cmdCallTraces)
	withUnwind(cmdCallTraces)
	withDatadir(cmdCallTraces)
	withShard(cmdCallTraces)

	rootCmd.AddCommand(cmdCallTraces)

	withChaindata(cmdStageTxLookup)
	withLmdbFlags(cmdStageTxLookup)
	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDatadir(cmdStageTxLookup)

	rootCmd.AddCommand(cmdStageTxLookup)

	withChaindata(cmdPrintMigrations)
	rootCmd.AddCommand(cmdPrintMigrations)

	withChaindata(cmdRemoveMigration)
	withLmdbFlags(cmdRemoveMigration)
	withMigration(cmdRemoveMigration)
	rootCmd.AddCommand(cmdRemoveMigration)

	withChaindata(cmdRunMigrations)
	withLmdbFlags(cmdRunMigrations)
	withDatadir(cmdRunMigrations)
	rootCmd.AddCommand(cmdRunMigrations)

	withDispatcher(cmdShardDispatcher)
	rootCmd.AddCommand(cmdShardDispatcher)
}

func stageSenders(db ethdb.Database, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	_, bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetSenders(db); err != nil {
			return err
		}
	}

	stage2 := progress(stages.Bodies)
	stage3 := progress(stages.Senders)
	log.Info("Stage2", "progress", stage2.BlockNumber)
	log.Info("Stage3", "progress", stage3.BlockNumber)
	ch := make(chan struct{})
	defer close(ch)

	const batchSize = 10000
	const blockSize = 4096
	n := runtime.NumCPU()

	cfg := stagedsync.Stage3Config{
		BatchSize:       batchSize,
		BlockSize:       blockSize,
		BufferSize:      (blockSize * 10 / 20) * 10000, // 20*4096
		NumOfGoroutines: n,
		ReadChLen:       4,
		Now:             time.Now(),
	}

	return stagedsync.SpawnRecoverSendersStage(cfg, stage3, db, params.MainnetChainConfig, block, tmpdir, ch)
}

func stageExec(db ethdb.Database, ctx context.Context) error {
	core.UsePlainStateExecution = true

	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}

	cc, bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset { //nolint:staticcheck
		// TODO
	}

	stage4 := progress(stages.Execution)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	ch := ctx.Done()
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: stage4.BlockNumber - unwind}
		return stagedsync.UnwindExecutionStage(u, stage4, db, false)
	}
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	return stagedsync.SpawnExecuteBlocksStage(stage4, db,
		bc.Config(), cc, bc.GetVMConfig(),
		ch,
		stagedsync.ExecuteBlockStageParams{
			ToBlock:       block, // limit execution to the specified block
			WriteReceipts: sm.Receipts,
			BatchSize:     int(batchSize),
		})

}

func stageIHash(db ethdb.Database, ctx context.Context) error {
	core.UsePlainStateExecution = true
	tmpdir := path.Join(datadir, etl.TmpDirName)

	if err := migrations.NewMigrator().Apply(db, tmpdir); err != nil {
		panic(err)
	}

	err := SetSnapshotKV(db, snapshotDir, snapshotMode)
	if err != nil {
		panic(err)
	}

	_, bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := stagedsync.ResetHashState(db); err != nil {
			return err
		}
		return nil
	}

	stage4 := progress(stages.Execution)
	stage5 := progress(stages.IntermediateHashes)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: stage5.BlockNumber - unwind}
		return stagedsync.UnwindIntermediateHashesStage(u, stage5, db, tmpdir, ch)
	}
	return stagedsync.SpawnIntermediateHashesStage(stage5, db, tmpdir, ch)
}

func stageHashState(db ethdb.Database, ctx context.Context) error {
	core.UsePlainStateExecution = true
	tmpdir := path.Join(datadir, etl.TmpDirName)

	err := SetSnapshotKV(db, snapshotDir, snapshotMode)
	if err != nil {
		panic(err)
	}

	_, bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := stagedsync.ResetHashState(db); err != nil {
			return err
		}
		return nil
	}

	stage5 := progress(stages.IntermediateHashes)
	stage6 := progress(stages.HashState)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	log.Info("Stage6", "progress", stage6.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: stage6.BlockNumber - unwind}
		return stagedsync.UnwindHashStateStage(u, stage6, db, tmpdir, ch)
	}
	return stagedsync.SpawnHashStateStage(stage6, db, tmpdir, ch)
}

func stageLogIndex(db ethdb.Database, ctx context.Context) error {
	core.UsePlainStateExecution = true
	tmpdir := path.Join(datadir, etl.TmpDirName)

	_, bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetLogIndex(db); err != nil {
			return err
		}
		return nil
	}
	execStage := progress(stages.Execution)
	s := progress(stages.LogIndex)
	log.Info("Stage exec", "progress", execStage.BlockNumber)
	log.Info("Stage log index", "progress", s.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.LogIndex, UnwindPoint: s.BlockNumber - unwind}
		return stagedsync.UnwindLogIndex(u, s, db, ch)
	}

	if err := stagedsync.SpawnLogIndex(s, db, tmpdir, ch); err != nil {
		return err
	}
	return nil
}

func stageCallTraces(db ethdb.Database, ctx context.Context) error {
	core.UsePlainStateExecution = true
	tmpdir := path.Join(datadir, etl.TmpDirName)

	_, bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetCallTraces(db); err != nil {
			return err
		}
		return nil
	}
	execStage := progress(stages.Execution)
	s := progress(stages.CallTraces)
	log.Info("Stage exec", "progress", execStage.BlockNumber)
	if block != 0 {
		s.BlockNumber = block
		log.Info("Overriding initial state", "block", block)
	}
	log.Info("Stage call traces", "progress", s.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.CallTraces, UnwindPoint: s.BlockNumber - unwind}
		return stagedsync.UnwindCallTraces(u, s, db, bc.Config(), bc, ch)
	}

	var accessBuilder stagedsync.StateAccessBuilder
	var toBlock uint64
	if dispatcherAddr != "" {
		// CREATING GRPC CLIENT CONNECTION
		var dialOpts []grpc.DialOption
		dialOpts = []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig, MinConnectTimeout: 10 * time.Minute}),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(5 * datasize.MB))),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Timeout: 10 * time.Minute,
			}),
		}

		dialOpts = append(dialOpts, grpc.WithInsecure())

		conn, err := grpc.DialContext(ctx, dispatcherAddr, dialOpts...)
		if err != nil {
			return fmt.Errorf("creating client connection to shard dispatcher: %w", err)
		}
		dispatcherClient := shards.NewDispatcherClient(conn)
		var client shards.Dispatcher_StartDispatchClient
		client, err = dispatcherClient.StartDispatch(ctx, &grpc.EmptyCallOption{})
		if err != nil {
			return fmt.Errorf("starting shard dispatch: %w", err)
		}
		accessBuilder = func(db ethdb.Database, blockNumber uint64, accountCache, storageCache, codeCache, codeSizeCache *fastcache.Cache) (state.StateReader, state.WriterWithChangeSets) {
			shard := shards.NewShard(db.(ethdb.HasTx).Tx(), blockNumber, client, accountCache, storageCache, codeCache, codeSizeCache, shardBits, byte(shardID))
			return shard, shard
		}
		toBlock = block + 10000
	}

	if err := stagedsync.SpawnCallTraces(s, db, bc.Config(), bc, tmpdir, ch,
		stagedsync.CallTracesStageParams{
			AccessBuilder: accessBuilder,
			PresetChanges: accessBuilder == nil,
			ToBlock:       toBlock,
		}); err != nil {
		return err
	}
	return nil
}

func stageHistory(db ethdb.Database, ctx context.Context) error {
	core.UsePlainStateExecution = true
	tmpdir := path.Join(datadir, etl.TmpDirName)

	err := SetSnapshotKV(db, snapshotDir, snapshotMode)
	if err != nil {
		panic(err)
	}

	_, bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetHistory(db); err != nil {
			return err
		}
		return nil
	}
	execStage := progress(stages.Execution)
	stage7 := progress(stages.AccountHistoryIndex)
	stage8 := progress(stages.StorageHistoryIndex)
	log.Info("Stage4", "progress", execStage.BlockNumber)
	log.Info("Stage7", "progress", stage7.BlockNumber)
	log.Info("Stage8", "progress", stage8.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 { //nolint:staticcheck
		// TODO
	}

	if err := stagedsync.SpawnAccountHistoryIndex(stage7, db, tmpdir, ch); err != nil {
		return err
	}
	if err := stagedsync.SpawnStorageHistoryIndex(stage8, db, tmpdir, ch); err != nil {
		return err
	}
	return nil
}

func stageTxLookup(db ethdb.Database, ctx context.Context) error {
	core.UsePlainStateExecution = true
	tmpdir := path.Join(datadir, etl.TmpDirName)

	err := SetSnapshotKV(db, snapshotDir, snapshotMode)
	if err != nil {
		panic(err)
	}

	_, bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetTxLookup(db); err != nil {
			return err
		}
		return nil
	}
	stage9 := progress(stages.TxLookup)
	log.Info("Stage9", "progress", stage9.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.TxLookup, UnwindPoint: stage9.BlockNumber - unwind}
		s := progress(stages.TxLookup)
		return stagedsync.UnwindTxLookup(u, s, db, tmpdir, ch)
	}

	return stagedsync.SpawnTxLookup(stage9, db, tmpdir, ch)
}

func printAllStages(db rawdb.DatabaseReader, _ context.Context) error {
	return printStages(db)
}

func printAppliedMigrations(db ethdb.Database, _ context.Context) error {
	applied, err := migrations.AppliedMigrations(db, false /* withPayload */)
	if err != nil {
		return err
	}
	var appliedStrs = make([]string, len(applied))
	i := 0
	for k := range applied {
		appliedStrs[i] = k
		i++
	}
	sort.Strings(appliedStrs)
	log.Info("Applied", "migrations", strings.Join(appliedStrs, " "))
	return nil
}

func removeMigration(db rawdb.DatabaseDeleter, _ context.Context) error {
	if err := db.Delete(dbutils.Migrations, []byte(migration), nil); err != nil {
		return err
	}
	return nil
}

func shardDispatcher(ctx context.Context) error {
	// STARTING GRPC SERVER
	log.Info("Starting Shard Dispatcher", "on", dispatcherAddr)
	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("Shard Dispatcher received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", dispatcherAddr)
	if err != nil {
		return fmt.Errorf("could not create Shard Dispatcher listener: %w, addr=%s", err, dispatcherAddr)
	}
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	if metrics.Enabled {
		streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
		unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	}
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())
	var grpcServer *grpc.Server
	cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.WriteBufferSize(1024),  // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(100), // to force clients reduce concurrency level
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 10 * time.Minute,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	grpcServer = grpc.NewServer(opts...)
	dispatcherServer := NewDispatcherServerImpl(1 << shardBits)
	shards.RegisterDispatcherServer(grpcServer, dispatcherServer)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}

	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Shard Dispatcher failed", "err", err1)
		}
	}()
	<-ctx.Done()
	return nil
}

type DispatcherServerImpl struct {
	shards.UnimplementedDispatcherServer
	connMutex           sync.RWMutex
	expectedConnections int
	connections         map[int]shards.Dispatcher_StartDispatchServer
	nextConnID          int
}

func NewDispatcherServerImpl(expectedConnections int) *DispatcherServerImpl {
	return &DispatcherServerImpl{
		expectedConnections: expectedConnections,
		connections:         make(map[int]shards.Dispatcher_StartDispatchServer),
	}
}

func (ds *DispatcherServerImpl) addConnection(connection shards.Dispatcher_StartDispatchServer) int {
	ds.connMutex.Lock()
	defer ds.connMutex.Unlock()
	connID := ds.nextConnID
	ds.nextConnID++
	ds.connections[connID] = connection
	return connID
}

func (ds *DispatcherServerImpl) removeConnection(connID int) {
	ds.connMutex.Lock()
	defer ds.connMutex.Unlock()
	delete(ds.connections, connID)
}

func (ds *DispatcherServerImpl) makeBroadcastList(connID int) []shards.Dispatcher_StartDispatchServer {
	ds.connMutex.RLock()
	defer ds.connMutex.RUnlock()
	var list []shards.Dispatcher_StartDispatchServer
	for id, conn := range ds.connections {
		if id != connID {
			list = append(list, conn)
		}
	}
	return list
}

func (ds *DispatcherServerImpl) StartDispatch(connection shards.Dispatcher_StartDispatchServer) error {
	connID := ds.addConnection(connection)
	defer ds.removeConnection(connID)
	for stateRead, recvErr := connection.Recv(); recvErr == nil; stateRead, recvErr = connection.Recv() {
		// Get list of connections to broadcast to
		var broadcastList []shards.Dispatcher_StartDispatchServer
		for broadcastList = ds.makeBroadcastList(connID); len(broadcastList) < ds.expectedConnections-1; broadcastList = ds.makeBroadcastList(connID) {
			log.Info("Waiting for more connections before broadcasting", "connections left", ds.expectedConnections-len(broadcastList)-1)
			time.Sleep(5 * time.Second)
		}
		if dispatcherLatency > 0 {
			time.Sleep(time.Duration(dispatcherLatency) * time.Millisecond)
		}
		for _, conn := range broadcastList {
			if err := conn.Send(stateRead); err != nil {
				return fmt.Errorf("could not send broadcas for connection id %d: %w", connID, err)
			}
		}
	}
	return nil
}

type progressFunc func(stage stages.SyncStage) *stagedsync.StageState

func newSync(quitCh <-chan struct{}, db ethdb.Database, tx ethdb.Database, hook stagedsync.ChangeSetHook) (*core.TinyChainContext, *core.BlockChain, *stagedsync.State, progressFunc) {
	chainConfig, bc, err := newBlockChain(db)
	if err != nil {
		panic(err)
	}

	cc := &core.TinyChainContext{}
	cc.SetDB(tx)
	cc.SetEngine(ethash.NewFaker())
	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	st, err := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
		stagedsync.OptionalParameters{},
	).Prepare(nil, chainConfig, cc, bc.GetVMConfig(), db, tx, "integration_test", sm, path.Join(datadir, etl.TmpDirName), int(batchSize), quitCh, nil, nil, func() error { return nil }, hook)
	if err != nil {
		panic(err)
	}

	progress := func(stage stages.SyncStage) *stagedsync.StageState {
		if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
			s, err := st.StageState(stage, tx)
			if err != nil {
				panic(err)
			}
			return s
		}
		s, err := st.StageState(stage, tx)
		if err != nil {
			panic(err)
		}
		return s
	}

	return cc, bc, st, progress
}

func newBlockChain(db ethdb.Database) (*params.ChainConfig, *core.BlockChain, error) {
	blockchain, err1 := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	if err1 != nil {
		return nil, nil, err1
	}
	return params.MainnetChainConfig, blockchain, nil
}

func SetSnapshotKV(db ethdb.Database, snapshotDir, snapshotMode string) error {
	if len(snapshotMode) > 0 && len(snapshotDir) > 0 {
		mode, err := snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			panic(err)
		}

		snapshotKV := db.(ethdb.HasKV).KV()
		snapshotKV, err = snapshotsync.WrapBySnapshots(snapshotKV, snapshotDir, mode)
		if err != nil {
			return err
		}
		db.(ethdb.HasKV).SetKV(snapshotKV)
	}
	return nil
}

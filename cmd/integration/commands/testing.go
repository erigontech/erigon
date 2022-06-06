package commands

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"syscall"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/erigon-lib/common"

	//grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	proto_testing "github.com/ledgerwatch/erigon-lib/gointerfaces/testing"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	testingAddr  string // Address of the gRPC endpoint of the integration testing server
	sentryAddr   string // Address of the gRPC endpoint of the test sentry (mimicking sentry for the integration tests)
	consAddr     string // Address of the gRPC endpoint of consensus engine to test
	consSpecFile string // Path to the specification file for the consensus engine (ideally, integration test and consensus engine use identical spec files)
)

func init() {
	cmdTestCore.Flags().StringVar(&testingAddr, "testing.api.addr", "localhost:9092", "address of the gRPC endpoint of the integration testing server")
	cmdTestCore.Flags().StringVar(&sentryAddr, "sentry.api.addr", "localhost:9091", "Address of the gRPC endpoint of the test sentry (mimicking sentry for the integration tests)")
	rootCmd.AddCommand(cmdTestCore)

	cmdTestCons.Flags().StringVar(&consAddr, "cons.api.addr", "locahost:9093", "Address of the gRPC endpoint of the consensus engine to test")
	cmdTestCons.Flags().StringVar(&consSpecFile, "cons.spec.file", "", "Specification file for the consensis engine (ideally, integration test and consensus engine use identical spec files)")
	rootCmd.AddCommand(cmdTestCons)
}

var cmdTestCore = &cobra.Command{
	Use:   "test_core",
	Short: "Test server for testing core of Erigon or equivalent component",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common.RootContext()

		if err := testCore(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdTestCons = &cobra.Command{
	Use:   "test_cons",
	Short: "Integration test for consensus engine",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common.RootContext()
		if err := testCons(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

func testCore(ctx context.Context) error {
	if _, err := grpcTestDriverServer(ctx, testingAddr); err != nil {
		return fmt.Errorf("start test driver gRPC server: %w", err)
	}
	if _, err := grpcTestSentryServer(ctx, testingAddr); err != nil {
		return fmt.Errorf("start test sentry gRPC server: %w", err)
	}
	<-ctx.Done()
	return nil
}

func grpcTestDriverServer(ctx context.Context, testingAddr string) (*TestDriverServerImpl, error) {
	// STARTING GRPC SERVER
	log.Info("Starting Test driver server", "on", testingAddr)
	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("Test driver received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", sentryAddr)
	if err != nil {
		return nil, fmt.Errorf("could not create Test driver listener: %w, addr=%s", err, testingAddr)
	}
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	//if metrics.Enabled {
	//	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
	//	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	//}
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
		// Don't drop the connection, settings accordign to this comment on GitHub
		// https://github.com/grpc/grpc-go/issues/3171#issuecomment-552796779
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	grpcServer = grpc.NewServer(opts...)

	testDriverServer := NewTestDriverServer(ctx)
	proto_testing.RegisterTestDriverServer(grpcServer, testDriverServer)
	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}
	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Test driver server fail", "err", err1)
		}
	}()
	return testDriverServer, nil
}

type TestDriverServerImpl struct {
	proto_testing.UnimplementedTestDriverServer
}

func NewTestDriverServer(_ context.Context) *TestDriverServerImpl {
	return &TestDriverServerImpl{}
}

func grpcTestSentryServer(ctx context.Context, sentryAddr string) (*TestSentryServerImpl, error) {
	// STARTING GRPC SERVER
	log.Info("Starting Test sentry server", "on", testingAddr)
	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("Test sentry received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", sentryAddr)
	if err != nil {
		return nil, fmt.Errorf("could not create Test sentry listener: %w, addr=%s", err, testingAddr)
	}
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	//if metrics.Enabled {
	//	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
	//	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	//}
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

	testSentryServer := NewTestSentryServer(ctx)
	proto_sentry.RegisterSentryServer(grpcServer, testSentryServer)
	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}
	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Test driver server fail", "err", err1)
		}
	}()
	return testSentryServer, nil
}

type TestSentryServerImpl struct {
	proto_sentry.UnimplementedSentryServer
}

func NewTestSentryServer(_ context.Context) *TestSentryServerImpl {
	return &TestSentryServerImpl{}
}

func testCons(ctx context.Context) error {
	return nil
}

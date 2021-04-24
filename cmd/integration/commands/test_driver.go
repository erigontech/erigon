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
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	proto_testing "github.com/ledgerwatch/turbo-geth/gointerfaces/testing"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var cmdTestDriver = &cobra.Command{
	Use:   "test_driver",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()

		if err := testDriver(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

func testDriver(ctx context.Context) error {
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

	testDriverServer := NewTestDriverServer(ctx)
	proto_testing.RegisterTestDriverServer(grpcServer, testDriverServer)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}
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

	testSentryServer := NewTestSentryServer(ctx)
	proto_sentry.RegisterSentryServer(grpcServer, testSentryServer)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}
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

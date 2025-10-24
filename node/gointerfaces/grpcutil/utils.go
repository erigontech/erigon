// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

func TLS(tlsCACert, tlsCertFile, tlsKeyFile string) (credentials.TransportCredentials, error) {
	// load peer cert/key, ca cert
	if tlsCACert == "" {
		if tlsCertFile == "" && tlsKeyFile == "" {
			return nil, nil
		}
		return credentials.NewServerTLSFromFile(tlsCertFile, tlsKeyFile)
	}
	var caCert []byte
	peerCert, err := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load peer cert/key error:%w", err)
	}
	caCert, err = os.ReadFile(tlsCACert)
	if err != nil {
		return nil, fmt.Errorf("read ca cert file error:%w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{peerCert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
		//nolint:gosec
		InsecureSkipVerify: true, // This is to make it work when Common Name does not match - remove when procedure is updated for common name
	}), nil
}

func NewServer(rateLimit uint32, creds credentials.TransportCredentials) *grpc.Server {
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())

	//if metrics.Enabled {
	//	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
	//	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	//}

	//cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		//grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.MaxConcurrentStreams(rateLimit), // to force clients reduce concurrency level
		// Don't drop the connection, settings accordign to this comment on GitHub
		// https://github.com/grpc/grpc-go/issues/3171#issuecomment-552796779
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
		grpc.Creds(creds),
	}
	grpcServer := grpc.NewServer(opts...)
	reflection.Register(grpcServer)

	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}

	return grpcServer
}

func Connect(creds credentials.TransportCredentials, dialAddress string) (*grpc.ClientConn, error) {
	var dialOpts []grpc.DialOption

	backoffCfg := backoff.DefaultConfig
	backoffCfg.BaseDelay = 500 * time.Millisecond
	backoffCfg.MaxDelay = 10 * time.Second
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoffCfg, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(200 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}
	if creds == nil {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	}

	//if opts.inMemConn != nil {
	//	dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
	//		return opts.inMemConn.Dial()
	//	}))
	//}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return grpc.DialContext(ctx, dialAddress, dialOpts...)
}

func IsRetryLater(err error) bool {
	if s, ok := status.FromError(err); ok {
		code := s.Code()
		return code == codes.Unavailable || code == codes.Canceled || code == codes.ResourceExhausted
	}
	return false
}

func IsEndOfStream(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Canceled || strings.Contains(s.Message(), context.Canceled.Error())
	}
	return false
}

// ErrIs - like `errors.Is` but for grpc errors
func ErrIs(err, target error) bool {
	if errors.Is(err, target) { // direct clients do return Go-style errors
		return true
	}
	if s, ok := status.FromError(err); ok { // remote clients do return GRPC-style errors
		return strings.Contains(s.Message(), target.Error())
	}
	return false
}

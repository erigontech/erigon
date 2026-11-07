// Copyright 2026 The Erigon Authors
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

package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/privateapi"
)

type versionBackend struct {
	remoteproto.UnimplementedETHBACKENDServer
	version *typesproto.VersionReply
	err     error
}

func (s *versionBackend) Version(context.Context, *emptypb.Empty) (*typesproto.VersionReply, error) {
	return s.version, s.err
}

type versionKV struct {
	remoteproto.UnimplementedKVServer
	version *typesproto.VersionReply
	err     error
}

func (s *versionKV) Version(context.Context, *emptypb.Empty) (*typesproto.VersionReply, error) {
	return s.version, s.err
}

func TestTxpoolAPIVersions(t *testing.T) {
	current := privateapi.EthBackendAPIVersion
	currentKV := remotedbserver.KvServiceAPIVersion
	for _, tc := range []struct {
		name      string
		version   *typesproto.VersionReply
		err       error
		kvVersion *typesproto.VersionReply
		kvErr     error
		cancelled bool
		wantErr   string
	}{
		{name: "legacy backend", version: &typesproto.VersionReply{Major: 4, Minor: 2}, wantErr: "incompatible ETHBACKEND versions"},
		{name: "matching version", version: current, wantErr: "label: txpool"},
		{name: "patch difference", version: &typesproto.VersionReply{Major: current.Major, Minor: current.Minor, Patch: current.Patch + 1}, wantErr: "label: txpool"},
		{name: "version RPC failure", err: status.Error(codes.Unavailable, "backend unavailable"), wantErr: "could not get ETHBACKEND version"},
		{name: "cancelled startup", version: current, cancelled: true, wantErr: "context canceled"},
		{name: "legacy KV", version: current, kvVersion: &typesproto.VersionReply{Major: 7, Minor: 3, Patch: 3}, wantErr: "incompatible KV versions"},
		{name: "KV patch difference", version: current, kvVersion: &typesproto.VersionReply{Major: currentKV.Major, Minor: currentKV.Minor, Patch: currentKV.Patch + 1}, wantErr: "label: txpool"},
		{name: "KV version RPC failure", version: current, kvErr: status.Error(codes.Unavailable, "KV unavailable"), wantErr: "could not get KV version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			remoteproto.RegisterETHBACKENDServer(server, &versionBackend{version: tc.version, err: tc.err})
			kvVersion := tc.kvVersion
			if kvVersion == nil {
				kvVersion = currentKV
			}
			remoteproto.RegisterKVServer(server, &versionKV{version: kvVersion, err: tc.kvErr})
			grpcutil.StartServerOnListener(server, listener, false, log.New(), "test backend failed")
			t.Cleanup(server.Stop)

			oldAddr, oldDir, oldSentries := privateApiAddr, datadirCli, sentryAddr
			t.Cleanup(func() { privateApiAddr, datadirCli, sentryAddr = oldAddr, oldDir, oldSentries })
			privateApiAddr = listener.Addr().String()
			sentryAddr = nil
			// Compatible backends reach this database error; incompatible ones must stop earlier.
			datadirCli = t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(datadirCli, "txpool", "mdbx.dat"), 0o755))

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			err = doTxpool(ctx, log.New())
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

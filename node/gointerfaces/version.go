// Copyright 2021 The Erigon Authors
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

package gointerfaces

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

type Version struct {
	Major, Minor, Patch uint32 // interface Version of the client - to perform compatibility check when opening
}

func VersionFromProto(r *typesproto.VersionReply) Version {
	return Version{Major: r.Major, Minor: r.Minor, Patch: r.Patch}
}

// EnsureVersion - Default policy: allow only patch difference
func EnsureVersion(local Version, remote *typesproto.VersionReply) bool {
	if remote.Major != local.Major {
		return false
	}
	if remote.Minor != local.Minor {
		return false
	}
	return true
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// VersionClient is implemented by any gRPC client that exposes a Version RPC.
type VersionClient interface {
	Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*typesproto.VersionReply, error)
}

// EnsureVersionCompatibility calls Version on the client, checks compatibility,
// and logs the result. Returns true if versions are compatible.
func EnsureVersionCompatibility(client VersionClient, expected Version, logger log.Logger) bool {
	versionReply, err := client.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		logger.Error("getting Version", "err", err)
		return false
	}
	if !EnsureVersion(expected, versionReply) {
		logger.Error("incompatible interface versions", "client", expected.String(),
			"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
		return false
	}
	logger.Info("interfaces compatible", "client", expected.String(),
		"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
	return true
}

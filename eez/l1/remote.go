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

package l1

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/remotedb"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/rulesconfig"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// HeadNotifier is a ChainReader that announces new heads.
type HeadNotifier interface {
	// Heads delivers a value after the head changes, until ctx ends.
	Heads(ctx context.Context) (<-chan struct{}, error)
}

// Remote is a Native reader of a separate Erigon L1 node over its private
// API, as the standalone rpcdaemon reads one.
type Remote struct {
	*Native
	conn    *grpc.ClientConn
	db      kv.RoDB
	backend remoteproto.ETHBACKENDClient
}

var _ HeadNotifier = (*Remote)(nil)

// DialRemote connects to the private API of the Erigon node at addr. Receipt
// regeneration uses dirs for temporary files.
func DialRemote(ctx context.Context, addr string, dirs datadir.Dirs, logger log.Logger) (*Remote, error) {
	conn, err := grpcutil.Connect(nil, addr)
	if err != nil {
		return nil, err
	}
	r := &Remote{conn: conn, backend: remoteproto.NewETHBACKENDClient(conn)}
	db, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), logger, remoteproto.NewKVClient(conn)).Open()
	if err != nil {
		conn.Close()
		return nil, err
	}
	r.db = db
	var cfg *chain.Config
	if err := db.View(ctx, func(tx kv.Tx) error {
		genesis, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		if genesis == (common.Hash{}) {
			return fmt.Errorf("%w: genesis", ErrNotFound)
		}
		cfg, err = rawdb.ReadChainConfig(tx, genesis)
		return err
	}); err != nil {
		r.Close()
		return nil, fmt.Errorf("L1 chain config: %w", err)
	}
	if cfg == nil {
		r.Close()
		return nil, fmt.Errorf("%w: L1 chain config", ErrNotFound)
	}
	blocks := freezeblocks.NewRemoteBlockReader(r.backend)
	engine := rulesconfig.CreateRulesEngineBareBones(ctx, cfg, logger)
	r.Native = NewNative(db, blocks, receipts.NewGenerator(dirs, blocks, engine, nil, time.Minute), cfg)
	return r, nil
}

// Close releases the connection.
func (r *Remote) Close() {
	if r.db != nil {
		r.db.Close()
	}
	r.conn.Close()
}

// Heads delivers a value for each header event of the node, coalescing those
// not yet received; the channel closes when the stream ends.
func (r *Remote) Heads(ctx context.Context) (<-chan struct{}, error) {
	stream, err := r.backend.Subscribe(ctx, &remoteproto.SubscribeRequest{Type: remoteproto.Event_HEADER})
	if err != nil {
		return nil, err
	}
	heads := make(chan struct{}, 1)
	go func() {
		defer close(heads)
		for {
			reply, err := stream.Recv()
			if err != nil {
				return
			}
			if reply.Type != remoteproto.Event_HEADER {
				continue
			}
			select {
			case heads <- struct{}{}:
			default:
			}
		}
	}()
	return heads, nil
}

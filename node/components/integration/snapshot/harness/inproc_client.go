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

package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/gointerfaces"
	downloaderproto "github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

// InprocClient is an in-process implementation of dl.Client that resolves
// downloads via the harness Coordinator. Production uses an anacrolix/torrent
// client or a gRPC-remote downloader; InprocClient is the no-network backing
// the realistic-flow integration tests use behind the real Downloader
// component's BindBus handler.
//
// On Download, for each item it looks up the torrent hash in the shared
// Coordinator and, if found, writes a sparse placeholder at rootDir/Path of
// the Coordinator-reported size. The placeholder exists purely so
// downloader.Provider.onDownloadRequested's os.Stat sees a real Size when it
// publishes flow.DownloadComplete. Seed and Delete are no-ops, matching
// NoopSeederClient's contract for tests that don't exercise seeding.
type InprocClient struct {
	coord   *Coordinator
	rootDir string
}

// NewInprocClient constructs an in-process client bound to a shared
// coordinator. rootDir is where placeholder files are materialised so that
// the Downloader's event bridge can stat them — pass the same directory
// that was configured on the Provider (p.dirs.Snap).
func NewInprocClient(coord *Coordinator, rootDir string) *InprocClient {
	return &InprocClient{coord: coord, rootDir: rootDir}
}

// Download satisfies dl.Client. For each item, resolves the torrent hash
// against the Coordinator and writes a sparse placeholder of the advertised
// size. Returns an error wrapping the first item that could not be resolved.
func (c *InprocClient) Download(_ context.Context, req *downloaderproto.DownloadRequest) error {
	for _, item := range req.Items {
		if item.TorrentHash == nil {
			return fmt.Errorf("inproc_client: download item %q has no torrent hash", item.Path)
		}
		hash := gointerfaces.ConvertH160toAddress(item.TorrentHash)
		fd, ok := c.coord.lookup(hash)
		if !ok {
			return fmt.Errorf("inproc_client: info-hash not registered with coordinator: %x", hash)
		}

		target := filepath.Join(c.rootDir, item.Path)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("inproc_client: mkdir for %q: %w", target, err)
		}
		f, err := os.Create(target)
		if err != nil {
			return fmt.Errorf("inproc_client: create %q: %w", target, err)
		}
		if err := f.Truncate(fd.size); err != nil {
			_ = f.Close()
			return fmt.Errorf("inproc_client: truncate %q to %d: %w", target, fd.size, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("inproc_client: close %q: %w", target, err)
		}
	}
	return nil
}

// Seed is a no-op; the Coordinator is updated by FakePeer / inproc peer
// setups before a transfer is requested.
func (c *InprocClient) Seed(_ context.Context, _ []string) error { return nil }

// Delete is a no-op; tests don't exercise the deletion path.
func (c *InprocClient) Delete(_ context.Context, _ []string) error { return nil }

var _ dl.Client = (*InprocClient)(nil)

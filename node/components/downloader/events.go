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

package downloader

// Event types for the Downloader component.
//
// The Downloader is an event-driven component:
//   - It SUBSCRIBES to file lifecycle events from the Storage/BlockReader layer
//   - It PUBLISHES download status events consumed by Stages, RPC, and plugins
//
// These types define the shared contract between the Downloader and its peers.
// They flow through the node/app/event bus once the ComponentDomain is wired
// into the startup path.

// --- Events the Downloader SUBSCRIBES to ---

// SnapshotFilesCreated is published when new snapshot files are written to disk
// (e.g., after block retiring or snapshot building). The Downloader creates
// torrents and starts seeding these files.
//
// Replaces: chainDB.OnFilesChange (create callback)
// Publisher: Storage / BlockReader component
type SnapshotFilesCreated struct {
	Paths []string // relative paths within the snapshots directory
}

// SnapshotFilesDeleted is published when snapshot files are removed from disk
// (e.g., during pruning). The Downloader stops seeding and removes torrents.
//
// Replaces: chainDB.OnFilesChange (delete callback)
// Publisher: Storage / BlockReader component
type SnapshotFilesDeleted struct {
	Paths []string // relative paths within the snapshots directory
}

// DownloadRequested asks the Downloader to fetch specific files by name.
// Used by the snapshot stage and by plugins (snapshot-manager, ccip).
//
// Replaces: downloader.Client.Download(*DownloadRequest)
// Publisher: Stages, plugins/snapshot-manager, plugins/ccip
type DownloadRequested struct {
	Items []DownloadItem
}

// DownloadItem describes a single file to download.
type DownloadItem struct {
	Path     string // target filename (relative to snapshots dir)
	InfoHash []byte // 20-byte BitTorrent v1 infohash (optional — empty means discover)
}

// --- Events the Downloader PUBLISHES ---

// SnapshotDownloadComplete is published when a batch of requested downloads
// finishes. Consumers (e.g., the snapshot stage) use this to unblock sync.
//
// Replaces: afterSnapshotDownload callback, manifestReady channel
// Consumer: Stages (OtterSync), plugins/snapshot-manager
type SnapshotDownloadComplete struct {
	Paths []string // files that were downloaded
}

// NewSnapshotAvailable is published when new snapshot data becomes available
// locally (either downloaded or created). Used to notify RPC subscribers.
//
// Replaces: events.OnNewSnapshot()
// Consumer: RPC / Notifications
type NewSnapshotAvailable struct{}

// TorrentStatus is published periodically with download/seeding progress.
// Used for monitoring and metrics.
//
// Consumer: Metrics, debug endpoints, plugins
type TorrentStatus struct {
	Downloading int    // number of active downloads
	Seeding     int    // number of files being seeded
	Peers       int    // connected torrent peers
	BytesDown   uint64 // total bytes downloaded this session
	BytesUp     uint64 // total bytes uploaded this session
}

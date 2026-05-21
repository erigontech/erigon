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

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	anatorrent "github.com/anacrolix/torrent"
	anastorage "github.com/anacrolix/torrent/storage"

	"github.com/erigontech/erigon/common/dir"
)

// CanonicalFile names one file a minority publisher must adopt and the
// canonical info-hash identifying its bytes. For a snapshot file the
// info-hash IS the content identity — a minority publisher holds the
// same Name at a different info-hash and must replace it.
type CanonicalFile struct {
	Name     string
	InfoHash [20]byte
}

// StagedFile is one canonical file fetched into a staging batch.
type StagedFile struct {
	Name     string
	InfoHash [20]byte
	Path     string // absolute path inside the staging directory
	Size     int64
}

// StagedBatch is the result of FetchCanonicalBatch: every requested
// canonical file fetched into one isolated staging directory. Nothing
// live has been touched — validation (7b-3) and the atomic cutover
// (7c) are later phases.
type StagedBatch struct {
	Dir   string
	Files []StagedFile
}

// FetchCanonicalBatch downloads every canonical file in files into an
// isolated staging directory <snapDir>/.staging-<gen>/, using the live
// torrent client with per-directory scoped storage so the node's live
// (minority) snapshot files are never touched.
//
// anacrolix keys torrents by info-hash, so a canonical file — different
// bytes, hence a different info-hash — never collides with the live
// file of the same name. The fetched torrent's pieces are verified
// against the info-hash by the torrent client, so a completed batch is
// cryptographically guaranteed to be the canonical content.
//
// Whole-batch semantics: any single fetch failure aborts the batch and
// removes the staging directory. Partial adoption would leave the state
// domain internally inconsistent (docs/plans/20260520-phase7-staged-
// adoption-design.md §7b).
//
// The caller's ctx governs the deadline — a snapshot file is far larger
// than a sidecar, so no internal timeout is imposed.
func (p *Provider) FetchCanonicalBatch(ctx context.Context, gen string, files []CanonicalFile) (*StagedBatch, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("FetchCanonicalBatch: empty batch")
	}
	if p == nil || p.Downloader == nil {
		return nil, fmt.Errorf("FetchCanonicalBatch: provider or downloader nil")
	}
	client := p.Downloader.TorrentClient()
	if client == nil {
		return nil, fmt.Errorf("FetchCanonicalBatch: torrent client nil")
	}

	stagingDir := filepath.Join(p.dirs.Snap, ".staging-"+gen)
	// Start from a clean slate: a prior partial run for the same
	// generation may have left stale files.
	if err := dir.RemoveAll(stagingDir); err != nil {
		return nil, fmt.Errorf("FetchCanonicalBatch: clearing staging dir: %w", err)
	}
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return nil, fmt.Errorf("FetchCanonicalBatch: creating staging dir: %w", err)
	}

	storage := anastorage.NewFileOpts(anastorage.NewFileClientOpts{ClientBaseDir: stagingDir})
	defer func() { _ = storage.Close() }()

	success := false
	defer func() {
		if !success {
			_ = dir.RemoveAll(stagingDir)
		}
	}()

	if p.logger != nil {
		p.logger.Info("[downloader] staging canonical batch", "gen", gen, "files", len(files), "dir", stagingDir)
	}

	staged := make([]StagedFile, 0, len(files))
	for _, cf := range files {
		sf, err := p.fetchOneCanonical(ctx, client, storage, stagingDir, cf)
		if err != nil {
			return nil, fmt.Errorf("FetchCanonicalBatch: staging %s: %w", cf.Name, err)
		}
		staged = append(staged, sf)
	}

	success = true
	return &StagedBatch{Dir: stagingDir, Files: staged}, nil
}

// fetchOneCanonical fetches a single canonical file by info-hash into
// the batch's scoped staging storage and returns its on-disk location.
// The torrent is dropped once complete — the bytes remain on disk; 7c
// re-registers them with the live downloader after the cutover rename.
func (p *Provider) fetchOneCanonical(ctx context.Context, client *anatorrent.Client, storage anastorage.ClientImplCloser, stagingDir string, cf CanonicalFile) (StagedFile, error) {
	// A canonical info-hash already known to the live client would mean
	// AddTorrentOpt returns that torrent and silently ignores the staging
	// storage — the bytes would land in the live snap dir, not staging.
	// In the minority scenario the node believes it already holds the
	// file (at the wrong hash), so the normal download path is not
	// fetching the canonical hash; treat a collision as a hard error.
	if _, ok := client.Torrent(cf.InfoHash); ok {
		return StagedFile{}, fmt.Errorf("info-hash %x already tracked by the live client", cf.InfoHash[:8])
	}

	t, _ := client.AddTorrentOpt(anatorrent.AddTorrentOpts{
		InfoHash: cf.InfoHash,
		Storage:  storage,
	})
	defer t.Drop()

	// A torrent added directly to the client (bypassing Downloader.
	// addTorrent) does not inherit the configured static peers; apply
	// them so the swarm seeders are reachable when DHT is unavailable.
	if peers := p.Downloader.StaticPeers(); len(peers) > 0 {
		t.AddPeers(peers)
	}

	select {
	case <-t.GotInfo():
	case <-ctx.Done():
		return StagedFile{}, fmt.Errorf("waiting for torrent info: %w", ctx.Err())
	}

	info := t.Info()
	if info == nil {
		return StagedFile{}, fmt.Errorf("torrent info missing after GotInfo")
	}
	if len(info.Files) != 0 {
		return StagedFile{}, fmt.Errorf("canonical torrent is multi-file (%d) — expected a single snapshot file", len(info.Files))
	}
	// info.Name is bound to the info-hash (it is part of the torrent
	// info dict). A name that disagrees with the verdict's entry means
	// the canonical hash points at a differently-named file — a verdict
	// or hash mix-up. Reject: the later cutover renames staged → live by
	// this name, so it must match.
	if info.Name != cf.Name {
		return StagedFile{}, fmt.Errorf("canonical torrent name %q does not match expected %q", info.Name, cf.Name)
	}

	t.DownloadAll()
	select {
	case <-t.Complete().On():
	case <-ctx.Done():
		return StagedFile{}, fmt.Errorf("downloading %s: %w", info.Name, ctx.Err())
	}

	path := filepath.Join(stagingDir, info.Name)
	fi, err := os.Stat(path)
	if err != nil {
		return StagedFile{}, fmt.Errorf("staged file %s missing after completion: %w", info.Name, err)
	}

	if p.logger != nil {
		p.logger.Info("[downloader] staged canonical file", "name", info.Name,
			"infoHash", fmt.Sprintf("%x", cf.InfoHash[:8]), "size", fi.Size())
	}
	return StagedFile{
		Name:     cf.Name,
		InfoHash: cf.InfoHash,
		Path:     path,
		Size:     fi.Size(),
	}, nil
}

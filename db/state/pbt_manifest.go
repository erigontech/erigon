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

package state

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/erigontech/erigon/common/dir"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
)

const PBTManifestFormat = 1

type PBTManifestPoint struct {
	Block uint64 `toml:"block"`
	TxNum uint64 `toml:"tx_num"`
}

type PBTManifestFile struct {
	Name   string `toml:"name"`
	Size   uint64 `toml:"size"`
	Blake3 string `toml:"blake3"`
}

type PBTManifest struct {
	Format     int               `toml:"format"`
	Point      PBTManifestPoint  `toml:"point"`
	PBTRoot    string            `toml:"pbt_root"`
	HexRoot    string            `toml:"hex_root"`
	TrieHash   string            `toml:"trie_hash"`
	SkipVerify bool              `toml:"skip_verify"`
	Files      []PBTManifestFile `toml:"files"`
}

type PBTManifestFileSource struct {
	Name string
	Path string
}

type PBTManifestProgressFunc func(done, total, bytes uint64)

func BuildPBTManifest(ctx context.Context, point PBTManifestPoint, pbtRoot, hexRoot, trieHash string, skipVerify bool, sources []PBTManifestFileSource, workers int, progress PBTManifestProgressFunc) (PBTManifest, error) {
	files, err := digestPBTManifestFiles(ctx, sources, workers, progress)
	if err != nil {
		return PBTManifest{}, err
	}
	return PBTManifest{
		Format:     PBTManifestFormat,
		Point:      point,
		PBTRoot:    pbtRoot,
		HexRoot:    hexRoot,
		TrieHash:   trieHash,
		SkipVerify: skipVerify,
		Files:      files,
	}, nil
}

func WritePBTManifest(path string, manifest PBTManifest) error {
	data, err := toml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("pbt manifest: encode: %w", err)
	}
	return dir.WriteFileAtomic(path, data, 0o600)
}

func PBTManifestDigest(path string) (string, error) {
	file, err := digestPBTManifestFile(context.Background(), PBTManifestFileSource{Name: filepath.Base(path), Path: path})
	if err != nil {
		return "", err
	}
	return file.Blake3, nil
}

func ReadPBTManifest(path string) (PBTManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return PBTManifest{}, fmt.Errorf("pbt manifest: read: %w", err)
	}
	var manifest PBTManifest
	if decodeErr := toml.Unmarshal(data, &manifest); decodeErr != nil {
		return PBTManifest{}, fmt.Errorf("pbt manifest: decode: %w", decodeErr)
	}
	if manifest.Format != PBTManifestFormat {
		return PBTManifest{}, fmt.Errorf("pbt manifest: unknown format %d", manifest.Format)
	}
	return manifest, nil
}

func ValidatePBTManifestFiles(ctx context.Context, manifest PBTManifest, sources []PBTManifestFileSource, workers int, progress PBTManifestProgressFunc) error {
	if err := ValidatePBTManifestFileSet(manifest, sources); err != nil {
		return err
	}

	actual, err := digestPBTManifestFiles(ctx, sources, workers, progress)
	if err != nil {
		return err
	}
	manifestByName := make(map[string]PBTManifestFile, len(manifest.Files))
	for _, file := range manifest.Files {
		manifestByName[file.Name] = file
	}
	for _, got := range actual {
		want := manifestByName[got.Name]
		if want.Size != got.Size {
			return fmt.Errorf("pbt manifest: size mismatch for %q: manifest=%d actual=%d", got.Name, want.Size, got.Size)
		}
		if want.Blake3 != got.Blake3 {
			return fmt.Errorf("pbt manifest: digest mismatch for %q: manifest=%s actual=%s", got.Name, want.Blake3, got.Blake3)
		}
	}
	return nil
}

func ValidatePBTManifestFileSet(manifest PBTManifest, sources []PBTManifestFileSource) error {
	expected := make(map[string]PBTManifestFileSource, len(sources))
	for _, source := range sources {
		if _, ok := expected[source.Name]; ok {
			return fmt.Errorf("pbt manifest: duplicate source file %q", source.Name)
		}
		expected[source.Name] = source
	}
	seen := make(map[string]struct{}, len(manifest.Files))
	for _, file := range manifest.Files {
		if _, ok := seen[file.Name]; ok {
			return fmt.Errorf("pbt manifest: duplicate file name %q", file.Name)
		}
		seen[file.Name] = struct{}{}
		if _, ok := expected[file.Name]; !ok {
			return fmt.Errorf("pbt manifest: extra file %q", file.Name)
		}
	}
	for name := range expected {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("pbt manifest: missing file %q", name)
		}
	}
	return nil
}

func digestPBTManifestFiles(ctx context.Context, sources []PBTManifestFileSource, workers int, progress PBTManifestProgressFunc) ([]PBTManifestFile, error) {
	ordered := append([]PBTManifestFileSource(nil), sources...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	files := make([]PBTManifestFile, len(ordered))
	if len(ordered) == 0 {
		return files, nil
	}
	workers = min(max(workers, 1), len(ordered))
	group, groupCtx := errgroup.WithContext(ctx)
	var next atomic.Uint64
	for range workers {
		group.Go(func() error {
			for {
				index := int(next.Add(1)) - 1
				if index >= len(ordered) {
					return nil
				}
				file, err := digestPBTManifestFile(groupCtx, ordered[index])
				if err != nil {
					return err
				}
				files[index] = file
				if progress != nil {
					progress(uint64(index+1), uint64(len(ordered)), file.Size)
				}
			}
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return files, nil
}

func digestPBTManifestFile(ctx context.Context, source PBTManifestFileSource) (PBTManifestFile, error) {
	info, err := os.Stat(source.Path)
	if errors.Is(err, os.ErrNotExist) {
		return PBTManifestFile{}, fmt.Errorf("pbt manifest: missing file %q", source.Name)
	}
	if err != nil {
		return PBTManifestFile{}, fmt.Errorf("pbt manifest: stat %q: %w", source.Name, err)
	}
	if !info.Mode().IsRegular() {
		return PBTManifestFile{}, fmt.Errorf("pbt manifest: file %q is not regular", source.Name)
	}
	file, err := os.Open(source.Path)
	if err != nil {
		return PBTManifestFile{}, fmt.Errorf("pbt manifest: open %q: %w", source.Name, err)
	}
	hash := blake3.New(32, nil)
	_, copyErr := io.CopyBuffer(hash, pbtManifestContextReader{ctx: ctx, reader: file}, make([]byte, 1<<20))
	closeErr := file.Close()
	if copyErr != nil {
		return PBTManifestFile{}, fmt.Errorf("pbt manifest: digest %q: %w", source.Name, copyErr)
	}
	if closeErr != nil {
		return PBTManifestFile{}, fmt.Errorf("pbt manifest: close %q: %w", source.Name, closeErr)
	}
	select {
	case <-ctx.Done():
		return PBTManifestFile{}, ctx.Err()
	default:
	}
	return PBTManifestFile{Name: source.Name, Size: uint64(info.Size()), Blake3: hex.EncodeToString(hash.Sum(nil))}, nil
}

type pbtManifestContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r pbtManifestContextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		select {
		case <-r.ctx.Done():
			return n, r.ctx.Err()
		default:
		}
	}
	return n, err
}

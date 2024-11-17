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

package fork_graph

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/spf13/afero"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func getBeaconStateFilename(blockRoot libcommon.Hash) string {
	return fmt.Sprintf("%x.snappy_ssz", blockRoot)
}

func getBeaconStateCacheFilename(blockRoot libcommon.Hash) string {
	return fmt.Sprintf("%x.cache", blockRoot)
}

func (f *forkGraphDisk) readBeaconStateFromDisk(blockRoot libcommon.Hash, out *state.CachingBeaconState) (bs *state.CachingBeaconState, err error) {
	var file afero.File
	file, err = f.fs.Open(getBeaconStateFilename(blockRoot))
	if err != nil {
		return
	}
	defer file.Close()

	// Read the version
	v := []byte{0}
	if _, err := file.Read(v); err != nil {
		return nil, fmt.Errorf("failed to read hard fork version: %w, root: %x", err, blockRoot)
	}
	// Read the length
	lengthBytes := make([]byte, 8)
	var n int
	n, err = io.ReadFull(file, lengthBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read length: %w, root: %x", err, blockRoot)
	}
	if n != 8 {
		return nil, fmt.Errorf("failed to read length: %d, want 8, root: %x", n, blockRoot)
	}

	f.sszSnappyBuffer = f.sszSnappyBuffer[:binary.BigEndian.Uint64(lengthBytes)]
	n, err = io.ReadFull(file, f.sszSnappyBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read snappy buffer: %w, root: %x", err, blockRoot)
	}
	f.sszSnappyBuffer = f.sszSnappyBuffer[:n]
	if out == nil {
		bs = state.New(f.beaconCfg)
	} else {
		bs = out
	}

	f.sszBuffer, err = snappy.Decode(f.sszBuffer[:0], f.sszSnappyBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to decode snappy buffer: %w, root: %x", err, blockRoot)
	}

	if err = bs.DecodeSSZ(f.sszBuffer, int(v[0])); err != nil {
		return nil, fmt.Errorf("failed to decode beacon state: %w, root: %x, len: %d, decLen: %d, bs: %+v", err, blockRoot, n, len(f.sszBuffer), bs)
	}
	// decode the cache file
	cacheFile, err := f.fs.Open(getBeaconStateCacheFilename(blockRoot))
	if err != nil {
		return
	}
	defer cacheFile.Close()

	if err := bs.DecodeCaches(cacheFile); err != nil {
		return nil, err
	}

	return
}

// dumpBeaconStateOnDisk dumps a beacon state on disk in ssz snappy format
func (f *forkGraphDisk) DumpBeaconStateOnDisk(blockRoot libcommon.Hash, bs *state.CachingBeaconState, forced bool) (err error) {
	if !forced && bs.Slot()%dumpSlotFrequency != 0 {
		return
	}
	// Truncate and then grow the buffer to the size of the state.
	f.sszBuffer, err = bs.EncodeSSZ(f.sszBuffer[:0])
	if err != nil {
		return
	}

	var dumpedFile afero.File
	dumpedFile, err = f.fs.OpenFile(getBeaconStateFilename(blockRoot), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return
	}
	defer dumpedFile.Close()

	f.sszSnappyBuffer = snappy.Encode(f.sszSnappyBuffer[:0], f.sszBuffer)

	// First write the hard fork version
	if _, err := dumpedFile.Write([]byte{byte(bs.Version())}); err != nil {
		return err
	}
	// Second write the length
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(f.sszSnappyBuffer)))
	if _, err := dumpedFile.Write(length); err != nil {
		return err
	}
	// Lastly dump the state
	if _, err := dumpedFile.Write(f.sszSnappyBuffer); err != nil {
		return err
	}

	if err = dumpedFile.Sync(); err != nil {
		return
	}

	cacheFile, err := f.fs.OpenFile(getBeaconStateCacheFilename(blockRoot), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return
	}
	defer cacheFile.Close()

	if err := bs.EncodeCaches(cacheFile); err != nil {
		return err
	}

	if err = cacheFile.Sync(); err != nil {
		return
	}

	return
}

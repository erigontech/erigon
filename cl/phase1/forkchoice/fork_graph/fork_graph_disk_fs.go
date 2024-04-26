package fork_graph

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/spf13/afero"
)

func getBeaconStateFilename(blockRoot libcommon.Hash) string {
	return fmt.Sprintf("%x.snappy_ssz", blockRoot)
}

func getBeaconStateCacheFilename(blockRoot libcommon.Hash) string {
	return fmt.Sprintf("%x.cache", blockRoot)
}

func (f *forkGraphDisk) readBeaconStateFromDisk(blockRoot libcommon.Hash) (bs *state.CachingBeaconState, err error) {
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
	// Grow the snappy buffer
	f.sszSnappyBuffer.Grow(int(binary.BigEndian.Uint64(lengthBytes)))
	// Read the snappy buffer
	sszSnappyBuffer := f.sszSnappyBuffer.Bytes()
	sszSnappyBuffer = sszSnappyBuffer[:cap(sszSnappyBuffer)]
	n, err = io.ReadFull(file, sszSnappyBuffer)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("failed to read snappy buffer: %w, root: %x", err, blockRoot)
	}

	decLen, err := snappy.DecodedLen(sszSnappyBuffer[:n])
	if err != nil {
		return nil, fmt.Errorf("failed to get decoded length: %w, root: %x, len: %d", err, blockRoot, n)
	}
	// Grow the plain ssz buffer
	f.sszBuffer.Grow(decLen)
	sszBuffer := f.sszBuffer.Bytes()
	sszBuffer, err = snappy.Decode(sszBuffer, sszSnappyBuffer[:n])
	if err != nil {
		return nil, fmt.Errorf("failed to decode snappy buffer: %w, root: %x, len: %d, decLen: %d", err, blockRoot, n, decLen)
	}
	bs = state.New(f.beaconCfg)
	err = bs.DecodeSSZ(sszBuffer, int(v[0]))
	// decode the cache file
	cacheFile, err := f.fs.Open(getBeaconStateCacheFilename(blockRoot))
	if err != nil {
		return
	}
	defer cacheFile.Close()

	reader := decompressPool.Get().(*zstd.Decoder)
	defer decompressPool.Put(reader)

	reader.Reset(cacheFile)

	if err := bs.DecodeCaches(reader); err != nil {
		return nil, err
	}

	return
}

// dumpBeaconStateOnDisk dumps a beacon state on disk in ssz snappy format
func (f *forkGraphDisk) dumpBeaconStateOnDisk(bs *state.CachingBeaconState, blockRoot libcommon.Hash) (err error) {
	// Truncate and then grow the buffer to the size of the state.
	encodingSizeSSZ := bs.EncodingSizeSSZ()
	f.sszBuffer.Grow(encodingSizeSSZ)
	f.sszBuffer.Reset()

	sszBuffer := f.sszBuffer.Bytes()
	sszBuffer, err = bs.EncodeSSZ(sszBuffer)
	if err != nil {
		return
	}
	// Grow the snappy buffer
	f.sszSnappyBuffer.Grow(snappy.MaxEncodedLen(len(sszBuffer)))
	// Compress the ssz buffer
	sszSnappyBuffer := f.sszSnappyBuffer.Bytes()
	sszSnappyBuffer = sszSnappyBuffer[:cap(sszSnappyBuffer)]
	sszSnappyBuffer = snappy.Encode(sszSnappyBuffer, sszBuffer)
	var dumpedFile afero.File
	dumpedFile, err = f.fs.OpenFile(getBeaconStateFilename(blockRoot), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return
	}
	defer dumpedFile.Close()
	// First write the hard fork version
	_, err = dumpedFile.Write([]byte{byte(bs.Version())})
	if err != nil {
		return
	}
	// Second write the length
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(sszSnappyBuffer)))
	_, err = dumpedFile.Write(length)
	if err != nil {
		return
	}
	// Lastly dump the state
	_, err = dumpedFile.Write(sszSnappyBuffer)
	if err != nil {
		return
	}

	err = dumpedFile.Sync()
	if err != nil {
		return
	}

	cacheFile, err := f.fs.OpenFile(getBeaconStateCacheFilename(blockRoot), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return
	}
	defer cacheFile.Close()

	writer := compressorPool.Get().(*zstd.Encoder)
	defer compressorPool.Put(writer)

	writer.Reset(cacheFile)
	defer writer.Close()

	if err := bs.EncodeCaches(writer); err != nil {
		return err
	}
	if err = writer.Close(); err != nil {
		return
	}
	err = cacheFile.Sync()

	return
}

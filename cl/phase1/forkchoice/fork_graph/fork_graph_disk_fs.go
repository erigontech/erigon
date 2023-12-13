package fork_graph

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/golang/snappy"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/pierrec/lz4"
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
		return nil, err
	}
	// Read the length
	lengthBytes := make([]byte, 8)
	_, err = file.Read(lengthBytes)
	if err != nil {
		return
	}
	// Grow the snappy buffer
	f.sszSnappyBuffer.Grow(int(binary.BigEndian.Uint64(lengthBytes)))
	// Read the snappy buffer
	sszSnappyBuffer := f.sszSnappyBuffer.Bytes()
	sszSnappyBuffer = sszSnappyBuffer[:cap(sszSnappyBuffer)]
	var n int
	n, err = file.Read(sszSnappyBuffer)
	if err != nil {
		return
	}

	decLen, err := snappy.DecodedLen(sszSnappyBuffer[:n])
	if err != nil {
		return
	}
	// Grow the plain ssz buffer
	f.sszBuffer.Grow(decLen)
	sszBuffer := f.sszBuffer.Bytes()
	sszBuffer, err = snappy.Decode(sszBuffer, sszSnappyBuffer[:n])
	if err != nil {
		return
	}
	bs = state.New(f.beaconCfg)
	err = bs.DecodeSSZ(sszBuffer, int(v[0]))
	// decode the cache file
	cacheFile, err := f.fs.Open(getBeaconStateCacheFilename(blockRoot))
	if err != nil {
		return
	}
	defer cacheFile.Close()

	lz4Reader := lz4PoolReaderPool.Get().(*lz4.Reader)
	defer lz4PoolReaderPool.Put(lz4Reader)

	lz4Reader.Reset(cacheFile)

	if err := bs.DecodeCaches(lz4Reader); err != nil {
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

	lz4Writer := lz4PoolWriterPool.Get().(*lz4.Writer)
	defer lz4PoolWriterPool.Put(lz4Writer)

	lz4Writer.CompressionLevel = 5
	lz4Writer.Reset(cacheFile)

	if err := bs.EncodeCaches(lz4Writer); err != nil {
		return err
	}
	if err = lz4Writer.Flush(); err != nil {
		return
	}
	err = cacheFile.Sync()

	return
}

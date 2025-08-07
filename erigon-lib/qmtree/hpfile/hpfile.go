package hpfile

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

//! # Head-prunable file
//!
//! Normal files can not be pruned\(truncated\) from the beginning to some middle position.
//! A `HPFile` use a sequence of small files to simulate one big virtual file. Thus, pruning
//! from the beginning is to delete the first several small files.
//!
//! A `HPFile` can only be read and appended. Any byteslice which was written to it is
//! immutable.
//!
//! To append a new byteslice into a `HPFile`, use the `append` function, which will return
//! the start position of this byteslice. Later, just pass this start position to `read_at`
//! for reading this byteslice out. The position passed to `read_at` must be the beginning of a
//! byteslice that was written before, instead of its middle. Do NOT try to read the later
//! half (from a middle point to the end) of a byteslice.
//!
//! A `HPFile` can also be truncated: discarding the content from a given position to the
//! end of the file. During trucation, several small files may be removed and one small file
//! may get truncated.
//!
//! A `HPFile` can serve many reader threads. If a reader thread just read random positions,
//! plain `read_at` is enough. If a reader tends to read many adjacent byteslices in sequence, it
//! can take advantage of spatial locality by using `read_at_with_pre_reader`, which uses a
//! `PreReader` to read large chunks of data from file and cache them. Each reader thread can have
//! its own `PreReader`. A `PreReader` cannot be shared by different `HPFile`s.
//!
//! A `HPFile` can serve only one writer thread. The writer thread must own a write buffer that
//! collects small pieces of written data into one big single write to the underlying OS file,
//! to avoid the cost of many syscalls writing the OS file. This write buffer must be provided
//! when calling `append` and `flush`. It is owned by the writer thead, instead of `HPFile`,
//! because we want `HPFile` to be shared between many reader threads.
//!
//! `TempDir` is used in unit test. It is a temporary directory created during a unit test
//! function, and will be deleted when this test function exits.

const (
	PRE_READ_BUF_SIZE = 512 * 1024
	IO_BLK_SIZE       = 512
)

type fileEntry struct {
	file     *file
	readOnly bool
}

type FileMap struct {
	sync.RWMutex
	files map[uint64]*fileEntry
}

// / Head-prunable file
type File struct {
	dirName        string // where we store the small files
	segmentSize    uint64 // the size of each small file
	bufferSize     int    // the write buffer's size
	fileMap        *FileMap
	largestId      atomic.Uint64
	latestFileSize atomic.Int64
	fileSize       atomic.Int64
	fileSizeOnDisk atomic.Int64
}

// / Create a `HPFile` with a directory. If this directory was used by an old HPFile, the old
// / HPFile must have the same `segmentSize` as this one.
// /
// / # Parameters
// /
// / - `wrBufSize`: The write buffer used in `append` will not exceed this size
// / - `segmentSize`: The target size of the small files
// / - `dirName`: The name of the directory used to store the small files
// / - `directio`: Enable directio for readonly files (only use this feature on Linux)
// /
// / # Returns
// /
// / A `Result` which is:
// /
// / - `Ok`: A successfully initialized `HPFile`
// / - `Err`: Encounted some file system error.
// /
func NewFile(wrBufSize uint64, segmentSize uint64, dirName string) (*File, error) {

	if segmentSize%wrBufSize != 0 {
		return nil, fmt.Errorf("Invalid segmentSize:%d writeBufferSize:%d", segmentSize, wrBufSize)
	}

	idList, largestId, err := getFileIds(dirName, segmentSize)

	if err != nil {
		return nil, err
	}

	fileMap, latestFileSize, err := loadFileMap(dirName, segmentSize, idList, largestId)

	if err != nil {
		return nil, err
	}

	fileSize := int64(largestId*segmentSize) + latestFileSize

	file := &File{
		dirName:     dirName,
		segmentSize: segmentSize,
		bufferSize:  int(wrBufSize),
		fileMap:     fileMap,
	}

	file.largestId.Store(largestId)
	file.latestFileSize.Store(latestFileSize)
	file.fileSize.Store(fileSize)
	file.fileSizeOnDisk.Store(fileSize)

	return file, nil
}

// / Create an empty `HPFile` that has no function and can only be used as placeholder.
func Empty() *File {
	return &File{
		fileMap: &FileMap{},
	}
}

// / Returns whether this `HPFile` is empty.
func (f *File) IsEmpty() bool {
	return f.segmentSize == 0
}

func getFileIds(dirName string, segmentSize uint64) ([]uint64, uint64, error) {
	var largestId uint64
	var idList []uint64

	entries, err := os.ReadDir(dirName)

	if err != nil {
		return nil, 0, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		id, err := parseFilename(segmentSize, entry.Name())

		if err != nil {
			return nil, 0, err
		}

		largestId = max(largestId, id)
		idList = append(idList, id)
	}

	return idList, largestId, nil
}

func parseFilename(segmentSize uint64, fileName string) (uint64, error) {
	parts := strings.Split(fileName, "-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("%s does not match the pattern 'FileId-segmentSize'", fileName)
	}

	id, err := strconv.Atoi(parts[0])

	if err != nil {
		return 0, fmt.Errorf("can't parse id from %s: %w", fileName, err)
	}

	size, err := strconv.Atoi(parts[1])

	if err != nil {
		return 0, fmt.Errorf("can't parse size from %s: %w", fileName, err)
	}

	if segmentSize != uint64(size) {
		return 0, fmt.Errorf("Invalid Size! %d!=%d", size, segmentSize)
	}

	return uint64(id), nil
}

func loadFileMap(dirName string, segmentSize uint64, idList []uint64, largestId uint64) (*FileMap, int64, error) {
	fileMap := FileMap{}
	var latestFileSize int64

	for _, id := range idList {
		fileName := fmt.Sprintf("%s/%d-%d", dirName, id, segmentSize)
		options := options{}
		fileEntry, err := func() (*fileEntry, error) {
			if id == largestId {
				file, err := options.read(true).write(true).open(fileName)
				if err != nil {
					return nil, err
				}
				metadata, err := file.Metadata()
				if err != nil {
					return nil, err
				}
				latestFileSize = metadata.Len()
				return &fileEntry{file, false}, nil
			}
			file, err := options.read(true).open(fileName)
			if err != nil {
				return nil, err
			}
			return &fileEntry{file, true}, nil
		}()

		if err != nil {
			return nil, 0, err
		}

		fileMap.files[id] = fileEntry
	}

	if len(idList) == 0 {
		fileName := fmt.Sprintf("%s/%d-%d", dirName, 0, segmentSize)
		options := options{}
		file, err := options.read(true).write(true).createNew(fileName)
		if err != nil {
			return nil, 0, err
		}
		fileMap.files[0] = &fileEntry{file, false}
	}

	return &fileMap, latestFileSize, nil
}

// / Returns the size of the virtual large file, including the non-flushed bytes
func (f *File) Size() int64 {
	return f.fileSize.Load()
}

// / Returns the flushed size of the virtual large file
func (f *File) SizeOnDisk() int64 {
	return f.fileSizeOnDisk.Load()
}

// / Truncate the file to make it smaller
// /
// / # Parameters
// /
// / - `size`: the size of the virtual large file after truncation. It must be smaller
// /           than the original size.
// /
// / # Returns
// /
// / A `Result` which is:
// /
// / - `Ok`: It's truncated successfully
// / - `Err`: Encounted some file system error.
// /
func (f *File) Truncate(size int64) error {
	if f.IsEmpty() {
		return nil
	}

	largestId := f.largestId.Load()

	f.fileMap.Lock()
	defer f.fileMap.Unlock()

	for size < int64(largestId*f.segmentSize) {
		delete(f.fileMap.files, largestId)

		fileName := fmt.Sprintf("%s/%d-%d", f.dirName, largestId, f.segmentSize)
		if err := os.Remove(fileName); err != nil {
			return err
		}

		f.largestId.Store(largestId - 1)
		largestId -= 1
	}

	remainingSize := size - int64(largestId*f.segmentSize)
	fileName := fmt.Sprintf("%s/%d-%d", f.dirName, largestId, f.segmentSize)
	file, err := (&options{}).read(true).write(true).open(fileName)
	if err == nil {
		return err
	}
	file.SetLen(int64(remainingSize))
	file.Seek(0, io.SeekEnd)

	f.fileMap.files[largestId] = &fileEntry{file, false}
	f.latestFileSize.Store(remainingSize)
	f.fileSize.Store(size)
	f.fileSizeOnDisk.Store(size)

	return nil
}

// / Flush the remained data in `buffer` into file system
// /
// / # Parameters
// /
// / - `buffer`: the write buffer, which is used by the client to call `append`.
// /
// / # Returns
// /
// / A `Result` which is:
// /
// / - `Ok`: It's flushed successfully
// / - `Err`: Encounted some file system error.
// /
func (f *File) Flush(buffer []byte, eof bool) error {
	if f.IsEmpty() {
		return nil
	}
	largestId := f.largestId.Load()
	entry := f.fileMap.files[largestId]
	if len(buffer) > 0 {
		tail_len := len(buffer) % IO_BLK_SIZE
		if eof && tail_len != 0 {
			// force the file size aligned with IO_BLK_SIZE
			buffer = append(buffer, make([]byte, IO_BLK_SIZE-tail_len, 0)...)
		}
		entry.file.Seek(0, io.SeekEnd)
		entry.file.WriteAll(buffer)
		f.fileSizeOnDisk.Add(int64(len(buffer)))
	}

	return entry.file.SyncAll()
}

// / Close the opened small files
func (f *File) Close() {
	f.fileMap = nil
}

// / Returns the corresponding file and in-file position given a logical offset. When we
// / use io_uring to read data from HPFile, the underlying segment files must be exposed.
// /
// / # Parameters
// /
// / - `offset`: a logical offset of this HPFile
// /
// / # Returns
// /
// / A tuple. Its first entry is the underlying File and its readonly attribute,
// / and its sencond entry is the position within this underlying File.
// /
func (f *File) GetFileAndPos(offset uint64) (*fileEntry, uint64) {
	fileId := offset / f.segmentSize
	if file, ok := f.fileMap.files[fileId]; ok {
		return file, offset % f.segmentSize
	}

	return nil, 0
}

// / Read data from file at `offset` to fill `bz`
// /
// / # Parameters
// /
// / - `offset`: the start position of a byteslice that was written before
// /
// / # Returns
// /
// / A `Result` which is:
// /
// / - `Ok`: Number of bytes that was filled into `bz`
// / - `Err`: Encounted some file system error.
// /
func (f *File) ReadAt(bz []byte, offset uint64) (int64, error) {
	fileId := offset / f.segmentSize
	pos := offset % f.segmentSize
	if entry, ok := f.fileMap.files[fileId]; ok {
		return entry.file.ReadAt(bz, int64(pos))
	}
	return 0, nil
}

// / Read at most `num_bytes` from file at `offset` to fill `buf`
// /
// / # Parameters
// /
// / - `buf`: a vector to be filled
// / - `num_bytes`: the wanted number of bytes to be read
// / - `offset`: the start position of a byteslice that was written before
// / - `pre_reader`: a PreReader used to take advantage of spatial locality
// /
// / # Returns
// /
// / A `Result` which is:
// /
// / - `Ok`: Number of bytes that was filled into `buf`
// / - `Err`: Encounted some file system error.
// /
func (f *File) ReadAtWithPreReader(buf []byte, numBytes int64, offset uint64, preReader PreReader) (int64, error) {
	if len(buf) < int(numBytes) {
		//buf.resize(num_bytes, 0)
	}

	fileId := offset / f.segmentSize
	pos := int64(offset % f.segmentSize)

	if preReader.tryRead(fileId, pos, buf[:numBytes]) {
		return numBytes, nil
	}

	entry, ok := f.fileMap.files[fileId]

	if !ok {
		return 0, fmt.Errorf("can't find file %d", fileId)
	}

	if numBytes >= PRE_READ_BUF_SIZE {
		panic("Read too many bytes")
	}

	if int64(pos)+numBytes > int64(f.segmentSize) {
		return entry.file.ReadAt(buf[:numBytes], pos)
	}

	alignedPos := (pos / IO_BLK_SIZE) * IO_BLK_SIZE
	preReader.fillSlice(fileId, alignedPos, func(slice []byte) (int64, error) {
		end := min(len(slice), int(int64(f.segmentSize)-alignedPos))
		return f.ReadAt(slice[:end], uint64(alignedPos))
	})

	if !preReader.tryRead(fileId, pos, buf[:numBytes]) {
		panic(fmt.Sprintf(
			"Internal error: cannot read data just fetched in %s fileID %d",
			f.dirName, fileId))
	}

	return numBytes, nil
}

// / Append a byteslice to the file. This byteslice may be temporarily held in
// / `buffer` before flushing.
// /
// / # Parameters
// /
// / - `bz`: the byteslice to append. It cannot be longer than `wrBufSize` specified
// /         in `HPFile::new`.
// / - `buffer`: the write buffer. It will never be larger than `wrBufSize`.
// /
// / # Returns
// /
// / A `Result` which is:
// /
// / - `Ok`: the start position where this byteslice locates in the file
// / - `Err`: Encounted some file system error.
// /
func (f *File) Append(bz []byte) (uint64, error) {
	if f.IsEmpty() {
		return 0, nil
	}

	if len(bz) > f.bufferSize {
		panic("bz is too large")
	}

	largestId := f.largestId.Load()
	startPos := f.Size()
	oldSize := f.latestFileSize.Load() + int64(len(bz))
	f.fileSize.Add(int64(len(bz)))
	splitPos := 0
	extraBytes := len(bz) - f.bufferSize
	if extraBytes > 0 {
		// flush bufferSize bytes to disk
		splitPos = len(bz) - extraBytes
		buffer := bz[0:splitPos]
		if entry, ok := f.fileMap.files[largestId]; ok {
			if _, err := entry.file.WriteAll(buffer); err != nil {
				panic("Fail to write file")
			}
			f.fileSizeOnDisk.Add(int64(len(buffer)))
		}
	}
	buffer := bz[splitPos:] //put remained bytes into buffer
	overflowByteCount := oldSize + int64(len(bz)) - int64(f.segmentSize)
	if overflowByteCount >= 0 {
		f.Flush(buffer, true)
		largestId += 1
		f.largestId.Add(1)
		//open new file as writable
		newFile := fmt.Sprintf("%d/%d-%d", f.dirName, largestId, f.segmentSize)
		file, err := (&options{}).createNew(newFile)
		if err != nil {
			(&options{}).read(true).write(true).open(newFile)
		}
		f.fileMap.files[largestId] = &fileEntry{file, false}
		f.latestFileSize.Store(overflowByteCount)
	}

	return uint64(startPos), nil
}

// / Prune from the beginning to `offset`. This part of the file cannot be read hereafter.
func (f *File) PruneHead(offset uint64) error {
	if f.IsEmpty() {
		return nil
	}

	fileId := offset / f.segmentSize
	var idsToRemove []uint64

	for id := range f.fileMap.files {
		if id < fileId {
			idsToRemove = append(idsToRemove, id)
		}
	}

	for _, id := range idsToRemove {
		delete(f.fileMap.files, id)
		fileName := fmt.Sprintf("%d/%d-%d", f.dirName, id, f.segmentSize)
		os.Remove(fileName)
	}

	return nil
}

type PreReader struct {
	buffer []byte // size is PRE_READ_BUF_SIZE
	fileId uint64
	start  int64
	end    int64
}

func newPreReader() *PreReader {
	return &PreReader{
		buffer: make([]byte, 0, PRE_READ_BUF_SIZE),
		fileId: 0,
		start:  0,
		end:    0,
	}
}

func (p *PreReader) fillSlice(fileId uint64, start int64, access func([]byte) (int64, error)) error {
	p.fileId = fileId
	p.start = start
	n, err := access(p.buffer[:])

	if err != nil {
		return err
	}

	p.end = start + n
	return nil
}

func (pr *PreReader) tryRead(fileId uint64, start int64, bz []byte) bool {
	if fileId == pr.fileId && pr.start <= start && start+int64(len(bz)) <= pr.end {
		offset := (start - pr.start)
		copy(bz, pr.buffer[offset:offset+int64(len(bz))])
		return true
	} else {
		return false
	}
}

type options struct{}

func (o *options) read(_ bool) *options {
	return o
}

func (o *options) write(_ bool) *options {
	return o
}

func (o *options) customFlags(_ int32) *options {
	return o
}

func (o *options) create(_ bool) *options {
	return o
}

func (o *options) open(path string) (*file, error) {
	return &file{}, nil
}

func (o *options) createNew(path string) (*file, error) {
	return &file{}, nil
}

type metadata struct {
	len int64
}

func (m *metadata) Len() int64 {
	return m.len
}

func (m *metadata) IsEmpty() bool {
	return m.len == 0
}

type file struct {
	sync.RWMutex
	data []byte
	pos  int
}

func (f *file) Open(path string) (*file, error) {
	return &file{}, nil
}

func (f *file) Metadata() (*metadata, error) {
	return &metadata{
		len: int64(len(f.data)),
	}, nil
}

func (f *file) CreateNew(path string) (*file, error) {
	return &file{}, nil
}

func (f *file) SetLen(l int64) error {
	if int64(len(f.data)) <= l {
		f.data = nil
	}
	f.data = f.data[:l]
	return nil
}

func (f *file) Seek(offset int64, whence int) error {
	switch whence {
	case io.SeekCurrent:

	case io.SeekStart:
	case io.SeekEnd:
	}
	return fmt.Errorf("can't seek: unknown whence value")
}

func (f *file) Write(buffer []byte) (int64, error) {
	copy(f.data[f.pos:], buffer)
	return int64(len(buffer)), nil
}

func (f *file) WriteAll(buffer []byte) (int64, error) {
	return f.Write(buffer)
}

func (f *file) SyncAll() error {
	return nil
}

func (f *file) Read(bz []byte) (int64, error) {
	//? do we need a read pos
	l := len(f.data) - f.pos
	readLen := min(len(bz), l)
	copy(bz, f.data[:readLen])
	f.pos += readLen
	return int64(readLen), nil
}

func (f *file) ReadAt(bz []byte, offset int64) (int64, error) {
	l := int64(len(f.data))
	if offset > l {
		return 0, nil
	}
	readLen := int64(min(len(bz), int(l-offset)))
	copy(bz, f.data[offset:int(offset+readLen)])
	return readLen, nil
}

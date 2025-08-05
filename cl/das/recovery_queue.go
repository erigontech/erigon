package das

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/spf13/afero"
)

type RecoveryQueue interface {
	Add(r *recoveryRequest) (bool, error)
	Take() <-chan *recoveryRequest
	Done(r *recoveryRequest) error
}

type recoveryRequest struct {
	slot      uint64
	blockRoot common.Hash
}

func (r *recoveryRequest) MarshalSSZ() ([]byte, error) {
	return ssz2.MarshalSSZ(nil, r.slot, r.blockRoot[:])
}

func (r *recoveryRequest) UnmarshalSSZ(data []byte) error {
	return ssz2.UnmarshalSSZ(data, 0, &r.slot, r.blockRoot[:])
}

func (r recoveryRequest) Filepath() (string, string) {
	// path: <base>/recovery_queue/<slot/10000>/<slot>_<block_root>.ssz
	subdir := r.slot / 10000
	dir := strconv.FormatUint(subdir, 10)
	filepath := fmt.Sprintf("%s/%d_%s.ssz", dir, r.slot, r.blockRoot.Hex())
	return dir, filepath
}

var (
	_ RecoveryQueue = (*fileBasedQueue)(nil)
)

const (
	inMemCacheSize = 2048
)

type fileBasedQueue struct {
	fs     afero.Fs
	takeCh chan *recoveryRequest

	mutex          sync.Mutex
	cache          []*recoveryRequest
	cacheIndex     int
	ongoing        map[common.Hash]struct{}
	waitNewRequest chan struct{}
}

func NewFileBasedQueue(ctx context.Context, fs afero.Fs) RecoveryQueue {
	q := &fileBasedQueue{
		fs:             fs,
		takeCh:         make(chan *recoveryRequest), // unbuffered, because we already have cache
		cache:          make([]*recoveryRequest, 0),
		ongoing:        make(map[common.Hash]struct{}),
		waitNewRequest: make(chan struct{}, 1), // logically size 1 is enough
	}
	go q.coordinate(ctx)
	return q
}

func (q *fileBasedQueue) coordinate(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			count := q.countRequests()
			log.Debug("remaining recovery requests in queue", "count", count)
		default:
		}

		// take a request
		r, err := q.takeOne()
		if err != nil {
			log.Warn("failed to take a request", "err", err)
			continue
		}
		if r == nil {
			// no more requests in cache, wait for new request
			select {
			case <-ctx.Done():
				return
			case <-q.waitNewRequest:
			}
			continue
		}
		q.takeCh <- r
	}
}

func (q *fileBasedQueue) countRequests() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	count := 0
	// loop over all files in the base directory
	dirNames, err := afero.ReadDir(q.fs, ".")
	if err != nil {
		return 0
	}
	for _, dirName := range dirNames {
		if !dirName.IsDir() {
			continue
		}
		dirPath := dirName.Name()
		files, err := afero.ReadDir(q.fs, dirPath)
		if err != nil {
			return 0
		}
		count += len(files)
	}
	return count
}

// file path: <base>/<slot/10000>/<slot>_<block_root>.ssz
func (q *fileBasedQueue) Add(r *recoveryRequest) (bool, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	dir, filepath := r.Filepath()
	if err := q.fs.MkdirAll(dir, 0755); err != nil {
		return false, err
	}
	if _, err := q.fs.Stat(filepath); err == nil {
		return false, nil
	}
	fh, err := q.fs.Create(filepath)
	if err != nil {
		return false, err
	}
	data, err := r.MarshalSSZ()
	if err != nil {
		fh.Close()
		q.fs.Remove(filepath)
		return false, err
	}
	if _, err := fh.Write(data); err != nil {
		fh.Close()
		q.fs.Remove(filepath)
		return false, err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		q.fs.Remove(filepath)
		return false, err
	}
	fh.Close()
	// notify the take goroutine to take a new request
	select {
	case q.waitNewRequest <- struct{}{}:
	default:
	}
	return true, nil
}

// takeOne takes a request from the cache or file system.
// if there is no request in cache, it will read from file system.
// the returned request is put into ongoing map before returning.
func (q *fileBasedQueue) takeOne() (*recoveryRequest, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for q.cacheIndex < len(q.cache) {
		r := q.cache[q.cacheIndex]
		q.cacheIndex++
		if _, ok := q.ongoing[r.blockRoot]; !ok {
			q.ongoing[r.blockRoot] = struct{}{}
			return r, nil
		}
	}

	// no more requests in cache, read from file
	// read dir names in ascending order by slot
	dirNames, err := afero.ReadDir(q.fs, ".")
	if err != nil {
		return nil, err
	}
	sort.Slice(dirNames, func(i, j int) bool {
		slotI, err := strconv.ParseUint(dirNames[i].Name(), 10, 64)
		if err != nil {
			return false
		}
		slotJ, err := strconv.ParseUint(dirNames[j].Name(), 10, 64)
		if err != nil {
			return false
		}
		return slotI < slotJ
	})

	q.cache = make([]*recoveryRequest, 0)
	q.cacheIndex = 0
	// put them into cache
loop:
	for _, dirName := range dirNames {
		if !dirName.IsDir() {
			continue
		}
		dirPath := dirName.Name()
		// read files in ascending order by slot
		files, err := afero.ReadDir(q.fs, dirPath)
		if err != nil {
			return nil, err
		}
		sort.Slice(files, func(i, j int) bool {
			// file name: <slot>_<block_root>.ssz
			slotI, err := strconv.ParseUint(strings.Split(files[i].Name(), "_")[0], 10, 64)
			if err != nil {
				return false
			}
			slotJ, err := strconv.ParseUint(strings.Split(files[j].Name(), "_")[0], 10, 64)
			if err != nil {
				return false
			}
			return slotI < slotJ
		})
		for _, file := range files {
			if file.IsDir() {
				// impossible
				continue
			}
			fileData, err := afero.ReadFile(q.fs, fmt.Sprintf("%s/%s", dirPath, file.Name()))
			if err != nil {
				// Log the error but continue processing other files
				log.Warn("failed to read file", "file", file.Name(), "err", err)
				continue
			}
			r := &recoveryRequest{}
			if err := r.UnmarshalSSZ(fileData); err != nil {
				// Log the error but continue processing other files
				log.Warn("failed to unmarshal file", "file", file.Name(), "err", err)
				continue
			}
			if _, ok := q.ongoing[r.blockRoot]; !ok {
				// skip if which is already taken
				q.cache = append(q.cache, r)
			}
			if len(q.cache) >= inMemCacheSize {
				break loop
			}
		}
	}

	if len(q.cache) == 0 {
		// no more requests
		return nil, nil
	}
	r := q.cache[0]
	q.cacheIndex++
	q.ongoing[r.blockRoot] = struct{}{}
	return r, nil
}

func (q *fileBasedQueue) Take() <-chan *recoveryRequest {
	return q.takeCh
}

func (q *fileBasedQueue) Done(r *recoveryRequest) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if _, ok := q.ongoing[r.blockRoot]; !ok {
		return nil
	}
	// remove the file
	dir, filepath := r.Filepath()
	if err := q.fs.Remove(filepath); err != nil {
		return err
	}
	// check if dir is empty and remove it if so
	dirFh, err := q.fs.Open(dir)
	if err != nil {
		return err
	}
	defer dirFh.Close()
	// rm dir if empty
	dirFiles, err := dirFh.Readdir(1)
	if errors.Is(err, io.EOF) || (err == nil && len(dirFiles) == 0) {
		q.fs.Remove(dir)
	} else if err != nil {
		return err
	}
	// remove from ongoing map
	delete(q.ongoing, r.blockRoot)
	return nil
}

package entity_extras

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"os"
	"path"
	"sync"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
)

// ForkableId id as a uint64, returned by `RegisterForkable`. It is dependent on
// the order of registration, and so counting on it being constant across reboots
// might be tricky.
type ForkableId uint16

// all forkable id match this
const AllForkableId = math.MaxUint16

type holder struct {
	// tag - "type" of snapshot file. e.g. tag is "bodies" for "v1.0-007300-007400-bodies.seg" file
	name                string
	snapshotDataFileTag string   // name to be used in snapshot file
	indexFileTag        []string // one indexFileTag for each index
	dirs                datadir.Dirs
	saltFile            string
	snapshotConfig      *SnapshotConfig
}

// keeping this fixed size, so that append() does not potentially re-allocate array
// to a different address. This means that the "reads" (methods on ForkableId) can
// be done without any locks.
var entityRegistry [20]holder
var curr uint16

var mu sync.RWMutex

// RegisterForkable
// name: just user-defined name for identification
// dirs: directory where snapshots have to reside
// salt: for creation of indexes.
// pre: preverified files are snapshot file lists that gets downloaded initially.
func RegisterForkable(name string, dirs datadir.Dirs, pre snapcfg.Preverified, options ...EntityIdOption) ForkableId {
	h := &holder{
		name: name,
		dirs: dirs,
	}
	for _, opt := range options {
		opt(h)
	}

	if h.snapshotDataFileTag == "" {
		h.snapshotDataFileTag = name
	}

	if h.indexFileTag == nil {
		// default
		h.indexFileTag = []string{name}
	}

	if h.saltFile == "" {
		h.saltFile = path.Join(dirs.Snap, "salt-blocks.txt")
	}

	if h.snapshotConfig == nil {
		panic("snapshotCreationConfig is required")
	}

	mu.Lock()

	entityRegistry[curr] = *h
	id := ForkableId(curr)
	h.snapshotConfig.LoadPreverified(pre)
	curr++

	mu.Unlock()

	return id
}

func Cleanup() {
	// only for tests
	mu.Lock()
	curr = 0
	mu.Unlock()
}

type EntityIdOption func(*holder)

func WithSnapshotTag(tag string) EntityIdOption {
	return func(a *holder) {
		a.snapshotDataFileTag = tag
	}
}

func WithIndexFileType(indexFileTag []string) EntityIdOption {
	return func(a *holder) {
		a.indexFileTag = indexFileTag
	}
}

// TODO: at forkable boundary, we want this to be value type
// so changes don't effect config forkables own. Once we get it in
// as value, we can use reference in other places within forkables.
func WithSnapshotConfig(cfg *SnapshotConfig) EntityIdOption {
	return func(a *holder) {
		a.snapshotConfig = cfg
	}
}

func WithSaltFile(saltFile string) EntityIdOption {
	return func(a *holder) {
		a.saltFile = saltFile
	}
}

func (a ForkableId) Id() uint64 {
	return uint64(a)
}

func (a ForkableId) Name() string {
	return entityRegistry[a].name
}

func (a ForkableId) SnapshotTag() string {
	return entityRegistry[a].snapshotDataFileTag
}

func (a ForkableId) IndexFileTag() []string {
	return entityRegistry[a].indexFileTag
}

func (a ForkableId) String() string {
	return entityRegistry[a].name
}

func (a ForkableId) Dirs() datadir.Dirs {
	return entityRegistry[a].dirs
}

func (a ForkableId) SnapshotConfig() *SnapshotConfig {
	return entityRegistry[a].snapshotConfig
}

func (a ForkableId) Salt() (uint32, error) {
	// not computing salt an EntityId inception
	// since salt file might not be downloaded yet.
	saltFile := entityRegistry[a].saltFile
	baseDir := path.Dir(saltFile)
	saltLock.RLock()
	salt, ok := saltMap[baseDir]
	saltLock.RUnlock()
	if ok {
		return salt, nil
	}

	saltLock.Lock()
	salt, err := readAndCreateSaltIfNeeded(saltFile)
	if err != nil {
		return 0, err
	}

	saltMap[baseDir] = salt
	saltLock.Unlock()

	return salt, nil
}

func (a ForkableId) MatchAll() bool {
	return a == AllForkableId
}

var saltMap = map[string]uint32{}
var saltLock sync.RWMutex

func readAndCreateSaltIfNeeded(saltFile string) (uint32, error) {
	exists, err := dir.FileExist(saltFile)
	if err != nil {
		return 0, err
	}
	baseDir := path.Dir(saltFile)

	if !exists {
		dir.MustExist(baseDir)

		saltBytes := make([]byte, 4)
		_, err := rand.Read(saltBytes)
		if err != nil {
			return 0, err
		}
		if err := dir.WriteFileWithFsync(saltFile, saltBytes, os.ModePerm); err != nil {
			return 0, err
		}
	}
	saltBytes, err := os.ReadFile(saltFile)
	if err != nil {
		return 0, err
	}
	if len(saltBytes) != 4 {
		dir.MustExist(baseDir)

		saltBytes := make([]byte, 4)
		_, err := rand.Read(saltBytes)
		if err != nil {
			return 0, err
		}
		if err := dir.WriteFileWithFsync(saltFile, saltBytes, os.ModePerm); err != nil {
			return 0, err
		}
	}

	return binary.BigEndian.Uint32(saltBytes), nil
}

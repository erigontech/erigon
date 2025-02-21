package entity_extras

import (
	"encoding/binary"
	"math/rand/v2"
	"os"
	"path"
	"sync"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
)

// EntityId id as a uint64, returned by `RegisterAppendable`. It is dependent on
// the order of registration, and so counting on it being constant across reboots
// might be tricky.
type EntityId uint16

type holder struct {
	name                   string
	snapshotNameBase       string   // name to be used in snapshot file
	indexNameBases         []string // one indexNameBase for each index
	dirs                   datadir.Dirs
	snapshotDir            string
	saltFile               string
	snapshotCreationConfig *SnapshotConfig
}

var entityRegistry []holder
var curr uint16

// RegisterEntity
// not making appendableRegistry/curr thread safe for now, since it's only expected to be setup once
// at the start and then read.
// name: just user-defined name for identification
// dirs: directory where snapshots have to reside
// salt: for creation of indexes.
// pre: preverified files are snapshot file lists that gets downloaded initially.
func RegisterEntity(name string, dirs datadir.Dirs, pre snapcfg.Preverified, options ...EntityIdOption) EntityId {
	h := &holder{
		name: name,
		dirs: dirs,
	}
	for _, opt := range options {
		opt(h)
	}

	if h.snapshotNameBase == "" {
		h.snapshotNameBase = name
	}

	if h.indexNameBases == nil {
		// default
		h.indexNameBases = []string{name}
	}

	if h.snapshotDir == "" {
		h.snapshotDir = dirs.Snap
	}

	if h.saltFile == "" {
		h.saltFile = path.Join(dirs.Snap, "salt-blocks.txt")
	}

	if h.snapshotCreationConfig == nil {
		panic("snapshotCreationConfig is required")
	}
	entityRegistry = append(entityRegistry, *h)
	id := EntityId(curr)

	h.snapshotCreationConfig.SetupConfig(id, h.snapshotDir, pre)

	curr++

	return id
}

type EntityIdOption func(*holder)

func WithSnapshotPrefix(prefix string) EntityIdOption {
	return func(a *holder) {
		a.snapshotNameBase = prefix
	}
}

func WithIndexFileType(indexFileType []string) EntityIdOption {
	return func(a *holder) {
		a.indexNameBases = indexFileType
	}
}

func WithSnapshotCreationConfig(cfg *SnapshotConfig) EntityIdOption {
	return func(a *holder) {
		a.snapshotCreationConfig = cfg
	}
}

func WithSaltFile(saltFile string) EntityIdOption {
	return func(a *holder) {
		a.saltFile = saltFile
	}
}

func WithSnapshotDir(dir string) EntityIdOption {
	return func(a *holder) {
		a.snapshotDir = dir
	}
}

func (a EntityId) Id() uint64 {
	return uint64(a)
}

func (a EntityId) Name() string {
	return entityRegistry[a].name
}

func (a EntityId) SnapshotPrefix() string {
	return entityRegistry[a].snapshotNameBase
}

func (a EntityId) IndexPrefix() []string {
	return entityRegistry[a].indexNameBases
}

func (a EntityId) String() string {
	return entityRegistry[a].name
}

func (a EntityId) Dirs() datadir.Dirs {
	return entityRegistry[a].dirs
}

func (a EntityId) SnapshotDir() string {
	return entityRegistry[a].snapshotDir
}

func (a EntityId) SnapshotConfig() *SnapshotConfig {
	return entityRegistry[a].snapshotCreationConfig
}

func (a EntityId) Salt() (uint32, error) {
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
		binary.BigEndian.PutUint32(saltBytes, rand.Uint32())
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
		binary.BigEndian.PutUint32(saltBytes, rand.Uint32())
		if err := dir.WriteFileWithFsync(saltFile, saltBytes, os.ModePerm); err != nil {
			return 0, err
		}
	}

	return binary.BigEndian.Uint32(saltBytes), nil
}

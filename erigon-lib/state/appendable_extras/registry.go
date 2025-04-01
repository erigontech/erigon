package appendable_extras

import (
	"crypto/rand"
	"encoding/binary"
	"os"
	"path"
	"sync"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
)

// AppendableId id as a uint64, returned by `RegisterAppendable`. It is dependent on
// the order of registration, and so counting on it being constant across reboots
// might be tricky.
type AppendableId uint16

type holder struct {
	name                   string
	snapshotNameBase       string   // name to be used in snapshot file
	indexNameBases         []string // one indexNameBase for each index
	dirs                   datadir.Dirs
	snapshotDir            string
	saltFile               string
	snapshotCreationConfig *SnapshotConfig
}

// keeping this fixed size, so that append() does not potentially re-allocate array
// to a different address. This means that the "reads" (methods on AppendableId) can
// be done without any locks.
var entityRegistry [20]holder
var curr uint16

var mu sync.RWMutex

// RegisterAppendable
// name: just user-defined name for identification
// dirs: directory where snapshots have to reside
// salt: for creation of indexes.
// pre: preverified files are snapshot file lists that gets downloaded initially.
func RegisterAppendable(name string, dirs datadir.Dirs, pre snapcfg.Preverified, options ...EntityIdOption) AppendableId {
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

	mu.Lock()

	entityRegistry[curr] = *h
	id := AppendableId(curr)
	h.snapshotCreationConfig.LoadPreverified(pre)
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

// TODO: at appendable boundary, we want this to be value type
// so changes don't effect config appendables own. Once we get it in
// as value, we can use reference in other places within appendables.
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

func (a AppendableId) Id() uint64 {
	return uint64(a)
}

func (a AppendableId) Name() string {
	return entityRegistry[a].name
}

func (a AppendableId) SnapshotTag() string {
	return entityRegistry[a].snapshotNameBase
}

func (a AppendableId) IndexPrefix() []string {
	return entityRegistry[a].indexNameBases
}

func (a AppendableId) String() string {
	return entityRegistry[a].name
}

func (a AppendableId) Dirs() datadir.Dirs {
	return entityRegistry[a].dirs
}

func (a AppendableId) SnapshotDir() string {
	return entityRegistry[a].snapshotDir
}

func (a AppendableId) SnapshotConfig() *SnapshotConfig {
	return entityRegistry[a].snapshotCreationConfig
}

func (a AppendableId) Salt() (uint32, error) {
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

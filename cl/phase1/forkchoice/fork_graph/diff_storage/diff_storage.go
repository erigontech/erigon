package diffstorage

import (
	"bytes"
	"io"
	"sync"

	"github.com/alecthomas/atomic"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

const maxDumps = 8 // max number of dumps to keep in memory	to prevent from memory leak during long non-finality.

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type link struct {
	from libcommon.Hash
	to   libcommon.Hash
}

// Memory storage for binary diffs
type ChainDiffStorage struct {
	dumps      sync.Map
	parent     sync.Map // maps child -> parent
	links      sync.Map // maps root -> []links
	diffFn     func(w io.Writer, old, new []byte) error
	applyFn    func(in, out []byte, diff []byte, reverse bool) ([]byte, error)
	diffs      sync.Map
	dumpsCount atomic.Int32 // prevent from memory leak during long non-finality.
}

func NewChainDiffStorage(diffFn func(w io.Writer, old, new []byte) error, applyFn func(in, out []byte, diff []byte, reverse bool) ([]byte, error)) *ChainDiffStorage {
	return &ChainDiffStorage{
		diffFn:     diffFn,
		applyFn:    applyFn,
		dumpsCount: atomic.NewInt32(0),
	}
}

func (c *ChainDiffStorage) Insert(root, parent libcommon.Hash, prevDump, dump []byte, isDump bool) error {
	c.parent.Store(root, parent)
	if isDump {
		c.dumpsCount.Add(1)
		if c.dumpsCount.Load() > maxDumps {
			*c = *NewChainDiffStorage(c.diffFn, c.applyFn)
			c.dumpsCount.Store(0)
			return nil
		}
		c.dumps.Store(root, libcommon.Copy(dump))
		return nil
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	if err := c.diffFn(buf, prevDump, dump); err != nil {
		return err
	}
	c.diffs.Store(link{from: parent, to: root}, libcommon.Copy(buf.Bytes()))

	links, _ := c.links.LoadOrStore(parent, []link{})
	c.links.Store(parent, append(links.([]link), link{from: parent, to: root}))

	return nil
}

func (c *ChainDiffStorage) Get(root libcommon.Hash) ([]byte, error) {
	dump, foundDump := c.dumps.Load(root)
	if foundDump {
		return dump.([]byte), nil
	}
	currentRoot := root
	diffs := [][]byte{}
	for !foundDump {
		parent, found := c.parent.Load(currentRoot)
		if !found {
			return nil, nil
		}
		diff, foundDiff := c.diffs.Load(link{from: parent.(libcommon.Hash), to: currentRoot})
		if !foundDiff {
			return nil, nil
		}
		diffs = append(diffs, diff.([]byte))
		currentRoot = parent.(libcommon.Hash)
		dump, foundDump = c.dumps.Load(currentRoot)
	}
	out := libcommon.Copy(dump.([]byte))
	for i := len(diffs) - 1; i >= 0; i-- {
		var err error
		out, err = c.applyFn(out, out, diffs[i], false)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *ChainDiffStorage) Delete(root libcommon.Hash) {
	if _, loaded := c.dumps.LoadAndDelete(root); loaded {
		c.dumpsCount.Add(-1)
	}
	c.parent.Delete(root)
	links, ok := c.links.Load(root)
	if ok {
		for _, link := range links.([]link) {
			c.diffs.Delete(link)
		}
	}
	c.links.Delete(root)
}

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

package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/edsrzf/mmap-go"
	"github.com/spaolacci/murmur3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
)

var UseBpsTree = true

const BtreeLogPrefix = "btree"

// DefaultBtreeM - amount of keys on leaf of BTree
// It will do log2(M) co-located-reads from data file - for binary-search inside leaf
var DefaultBtreeM = uint64(dbg.EnvInt("BT_M", 256))

const DefaultBtreeStartSkip = uint64(4) // defines smallest shard available for scan instead of binsearch

var ErrBtIndexLookupBounds = errors.New("BtIndex: lookup di bounds error")

func logBase(n, base uint64) uint64 {
	return uint64(math.Ceil(math.Log(float64(n)) / math.Log(float64(base))))
}

type markupCursor struct {
	l  uint64 //l - level
	p  uint64 //p - pos inside level
	di uint64 //di - data array index
	si uint64 //si - current, actual son index
}

type node struct {
	p   uint64 // pos inside level
	d   uint64
	s   uint64 // sons pos inside level
	fc  uint64
	key []byte
	val []byte
}

type Cursor struct {
	btt    *BtIndex
	ctx    context.Context
	getter *seg.Reader
	key    []byte
	value  []byte
	d      uint64
}

//getter should be alive all the time of cursor usage
//Key and value is valid until cursor.Next is called
//func NewCursor(ctx context.Context, k, v []byte, d uint64, g ArchiveGetter) *Cursor {
//	return &Cursor{
//		ctx:    ctx,
//		getter: g,
//		key:    common.Copy(k),
//		value:  common.Copy(v),
//		d:      d,
//	}
//}

func (c *Cursor) Key() []byte {
	return c.key
}

func (c *Cursor) Di() uint64 {
	return c.d
}

func (c *Cursor) Value() []byte {
	return c.value
}

func (c *Cursor) Next() bool {
	if !c.next() {
		return false
	}

	key, value, _, err := c.btt.dataLookup(c.d, c.getter)
	if err != nil {
		return false
	}
	c.key, c.value = key, value
	return true
}

// next returns if another key/value pair is available int that index.
// moves pointer d to next element if successful
func (c *Cursor) next() bool {
	if c.d+1 == c.btt.ef.Count() {
		return false
	}
	c.d++
	return true
}

type btAlloc struct {
	d       uint64 // depth
	M       uint64 // child limit of any node
	N       uint64
	K       uint64
	vx      []uint64   // vertex count on level
	sons    [][]uint64 // i - level; 0 <= i < d; j_k - amount, j_k+1 - child count
	cursors []markupCursor
	nodes   [][]node
	naccess uint64
	trace   bool

	dataLookup dataLookupFunc
	keyCmp     keyCmpFunc
}

func newBtAlloc(k, M uint64, trace bool, dataLookup dataLookupFunc, keyCmp keyCmpFunc) *btAlloc {
	if k == 0 {
		return nil
	}

	d := logBase(k, M)
	a := &btAlloc{
		vx:         make([]uint64, d+1),
		sons:       make([][]uint64, d+1),
		cursors:    make([]markupCursor, d),
		nodes:      make([][]node, d),
		M:          M,
		K:          k,
		d:          d,
		trace:      trace,
		dataLookup: dataLookup,
		keyCmp:     keyCmp,
	}

	if trace {
		fmt.Printf("k=%d d=%d, M=%d\n", k, d, M)
	}
	a.vx[0], a.vx[d] = 1, k

	if k < M/2 {
		a.N = k
		a.nodes = make([][]node, 1)
		return a
	}

	//nnc := func(vx uint64) uint64 {
	//	return uint64(math.Ceil(float64(vx) / float64(M)))
	//}
	nvc := func(vx uint64) uint64 {
		return uint64(math.Ceil(float64(vx) / float64(M>>1)))
	}

	for i := a.d - 1; i > 0; i-- {
		nnc := uint64(math.Ceil(float64(a.vx[i+1]) / float64(M)))
		//nvc := uint64(math.Floor(float64(a.vx[i+1]) / float64(m))-1)
		//nnc := a.vx[i+1] / M
		//nvc := a.vx[i+1] / m
		//bvc := a.vx[i+1] / (m + (m >> 1))
		a.vx[i] = min(uint64(math.Pow(float64(M), float64(i))), nnc)
	}

	ncount := uint64(0)
	pnv := uint64(0)
	for l := a.d - 1; l > 0; l-- {
		//s := nnc(a.vx[l+1])
		sh := nvc(a.vx[l+1])

		if sh&1 == 1 {
			a.sons[l] = append(a.sons[l], sh>>1, M, 1, M>>1)
		} else {
			a.sons[l] = append(a.sons[l], sh>>1, M)
		}

		for ik := 0; ik < len(a.sons[l]); ik += 2 {
			ncount += a.sons[l][ik] * a.sons[l][ik+1]
			if l == 1 {
				pnv += a.sons[l][ik]
			}
		}
	}
	a.sons[0] = []uint64{1, pnv}
	ncount += a.sons[0][0] * a.sons[0][1] // last one
	a.N = ncount

	if trace {
		for i, v := range a.sons {
			fmt.Printf("L%d=%v\n", i, v)
		}
	}

	return a
}

func (a *btAlloc) traverseDfs() {
	for l := 0; l < len(a.sons)-1; l++ {
		a.cursors[l] = markupCursor{uint64(l), 1, 0, 0}
		a.nodes[l] = make([]node, 0)
	}

	if len(a.cursors) <= 1 {
		if a.nodes[0] == nil {
			a.nodes[0] = make([]node, 0)
		}
		a.nodes[0] = append(a.nodes[0], node{d: a.K})
		a.N = a.K
		if a.trace {
			fmt.Printf("ncount=%d ∂%.5f\n", a.N, float64(a.N-a.K)/float64(a.N))
		}
		return
	}

	c := a.cursors[len(a.cursors)-1]
	pc := a.cursors[(len(a.cursors) - 2)]
	root := new(node)
	trace := false

	var di uint64
	for stop := false; !stop; {
		// fill leaves, mark parent if needed (until all grandparents not marked up until root)
		// check if eldest parent has brothers
		//     -- has bros -> fill their leaves from the bottom
		//     -- no bros  -> shift cursor (tricky)
		if di > a.K {
			a.N = di - 1 // actually filled node count
			if a.trace {
				fmt.Printf("ncount=%d ∂%.5f\n", a.N, float64(a.N-a.K)/float64(a.N))
			}
			break
		}

		bros, parents := a.sons[c.l][c.p], a.sons[c.l][c.p-1]
		for i := uint64(0); i < bros; i++ {
			c.di = di
			if trace {
				fmt.Printf("L%d |%d| d %2d s %2d\n", c.l, c.p, c.di, c.si)
			}
			c.si++
			di++

			if i == 0 {
				pc.di = di
				if trace {
					fmt.Printf("P%d |%d| d %2d s %2d\n", pc.l, pc.p, pc.di, pc.si)
				}
				pc.si++
				di++
			}
			if di > a.K {
				a.N = di - 1 // actually filled node count
				stop = true
				break
			}
		}

		a.nodes[c.l] = append(a.nodes[c.l], node{p: c.p, d: c.di, s: c.si})
		a.nodes[pc.l] = append(a.nodes[pc.l], node{p: pc.p, d: pc.di, s: pc.si, fc: uint64(len(a.nodes[c.l]) - 1)})

		pid := c.si / bros
		if pid >= parents {
			if c.p+2 >= uint64(len(a.sons[c.l])) {
				stop = true // end of row
				if trace {
					fmt.Printf("F%d |%d| d %2d\n", c.l, c.p, c.di)
				}
			} else {
				c.p += 2
				c.si = 0
				c.di = 0
			}
		}
		a.cursors[c.l] = c
		a.cursors[pc.l] = pc

		//nolint
		for l := pc.l; l >= 0; l-- {
			pc := a.cursors[l]
			uncles := a.sons[pc.l][pc.p]
			grands := a.sons[pc.l][pc.p-1]

			pi1 := pc.si / uncles
			pc.si++
			pc.di = 0

			pi2 := pc.si / uncles
			moved := pi2-pi1 != 0

			switch {
			case pc.l > 0:
				gp := a.cursors[pc.l-1]
				if gp.di == 0 {
					gp.di = di
					di++
					if trace {
						fmt.Printf("P%d |%d| d %2d s %2d\n", gp.l, gp.p, gp.di, gp.si)
					}
					a.nodes[gp.l] = append(a.nodes[gp.l], node{p: gp.p, d: gp.di, s: gp.si, fc: uint64(len(a.nodes[l]) - 1)})
					a.cursors[gp.l] = gp
				}
			default:
				if root.d == 0 {
					root.d = di
					//di++
					if trace {
						fmt.Printf("ROOT | d %2d\n", root.d)
					}
				}
			}

			//fmt.Printf("P%d |%d| d %2d s %2d pid %d\n", pc.l, pc.p, pc.di, pc.si-1)
			if pi2 >= grands { // skip one step of si due to different parental filling order
				if pc.p+2 >= uint64(len(a.sons[pc.l])) {
					if trace {
						fmt.Printf("EoRow %d |%d|\n", pc.l, pc.p)
					}
					break // end of row
				}
				//fmt.Printf("N %d d%d s%d\n", pc.l, pc.di, pc.si)
				//fmt.Printf("P%d |%d| d %2d s %2d pid %d\n", pc.l, pc.p, pc.di, pc.si, pid)
				pc.p += 2
				pc.si = 0
				pc.di = 0
			}
			a.cursors[pc.l] = pc

			if !moved {
				break
			}
		}
	}

	if a.trace {
		fmt.Printf("ncount=%d ∂%.5f\n", a.N, float64(a.N-a.K)/float64(a.N))
	}
}

func (a *btAlloc) bsKey(x []byte, l, r uint64, g *seg.Reader) (k []byte, di uint64, found bool, err error) {
	//i := 0
	var cmp int
	for l <= r {
		di = (l + r) >> 1

		cmp, k, err = a.keyCmp(x, di, g, k[:0])
		a.naccess++

		switch {
		case err != nil:
			if errors.Is(err, ErrBtIndexLookupBounds) {
				return k, 0, false, nil
			}
			return k, 0, false, err
		case cmp == 0:
			return k, di, true, err
		case cmp == -1:
			l = di + 1
		default:
			r = di
		}
		if l == r {
			break
		}
	}
	return k, l, true, nil
}

func (a *btAlloc) bsNode(i, l, r uint64, x []byte) (n node, lm int64, rm int64) {
	lm, rm = -1, -1
	var m uint64

	for l < r {
		m = (l + r) >> 1
		cmp := bytes.Compare(a.nodes[i][m].key, x)
		a.naccess++
		switch {
		case cmp == 0:
			return a.nodes[i][m], int64(m), int64(m)
		case cmp > 0:
			r = m
			rm = int64(m)
		case cmp < 0:
			lm = int64(m)
			l = m + 1
		default:
			panic(fmt.Errorf("compare error %d, %x ? %x", cmp, n.key, x))
		}
	}
	return a.nodes[i][m], lm, rm
}

// find position of key with node.di <= d at level lvl
func (a *btAlloc) seekLeast(lvl, d uint64) uint64 {
	//TODO: this seems calculatable from M and tree depth
	return uint64(sort.Search(len(a.nodes[lvl]), func(i int) bool {
		return a.nodes[lvl][i].d >= d
	}))
}

// Get returns value if found exact match of key
// TODO k as return is useless(almost)
func (a *btAlloc) Get(g *seg.Reader, key []byte) (k []byte, found bool, di uint64, err error) {
	k, di, found, err = a.Seek(g, key)
	if err != nil {
		return nil, false, 0, err
	}
	if !found || !bytes.Equal(k, key) {
		return nil, false, 0, nil
	}
	return k, found, di, nil
}

func (a *btAlloc) Seek(g *seg.Reader, seek []byte) (k []byte, di uint64, found bool, err error) {
	if a.trace {
		fmt.Printf("seek key %x\n", seek)
	}

	var (
		lm, rm     int64
		L, R       = uint64(0), uint64(len(a.nodes[0]) - 1)
		minD, maxD = uint64(0), a.K
		ln         node
	)

	for l, level := range a.nodes {
		if len(level) == 1 && l == 0 {
			ln = a.nodes[0][0]
			maxD = ln.d
			break
		}
		ln, lm, rm = a.bsNode(uint64(l), L, R, seek)
		if ln.key == nil { // should return node which is nearest to key from the left so never nil
			if a.trace {
				fmt.Printf("found nil key %x pos_range[%d-%d] naccess_ram=%d\n", l, lm, rm, a.naccess)
			}
			return nil, 0, false, fmt.Errorf("bt index nil node at level %d", l)
		}
		//fmt.Printf("b: %x, %x\n", ik, ln.key)
		cmp := bytes.Compare(ln.key, seek)
		switch cmp {
		case 1: // key > ik
			maxD = ln.d
		case -1: // key < ik
			minD = ln.d
		case 0:
			if a.trace {
				fmt.Printf("found key %x v=%x naccess_ram=%d\n", seek, ln.val /*level[m].d,*/, a.naccess)
			}
			return ln.key, ln.d, true, nil
		}

		if lm >= 0 {
			minD = a.nodes[l][lm].d
			L = level[lm].fc
		} else if l+1 != len(a.nodes) {
			L = a.seekLeast(uint64(l+1), minD)
			if L == uint64(len(a.nodes[l+1])) {
				L--
			}
		}
		if rm >= 0 {
			maxD = a.nodes[l][rm].d
			R = level[rm].fc
		} else if l+1 != len(a.nodes) {
			R = a.seekLeast(uint64(l+1), maxD)
			if R == uint64(len(a.nodes[l+1])) {
				R--
			}
		}

		if maxD-minD <= a.M+2 {
			break
		}

		if a.trace {
			fmt.Printf("range={%x d=%d p=%d} (%d, %d) L=%d naccess_ram=%d\n", ln.key, ln.d, ln.p, minD, maxD, l, a.naccess)
		}
	}

	a.naccess = 0 // reset count before actually go to disk
	if maxD-minD > a.M+2 {
		log.Warn("too big binary search", "minD", minD, "maxD", maxD, "keysCount", a.K, "key", fmt.Sprintf("%x", seek))
		//return nil, nil, 0, fmt.Errorf("too big binary search: minD=%d, maxD=%d, keysCount=%d, key=%x", minD, maxD, a.K, ik)
	}
	k, di, found, err = a.bsKey(seek, minD, maxD, g)
	if err != nil {
		if a.trace {
			fmt.Printf("key %x not found\n", seek)
		}
		return nil, 0, false, err
	}
	return k, di, found, nil
}

func (a *btAlloc) WarmUp(gr *seg.Reader) error {
	a.traverseDfs()

	for i, n := range a.nodes {
		if a.trace {
			fmt.Printf("D%d |%d| ", i, len(n))
		}
		for j, s := range n {
			if a.trace {
				fmt.Printf("%d ", s.d)
			}
			if s.d >= a.K {
				break
			}

			kb, v, _, err := a.dataLookup(s.d, gr)
			if err != nil {
				fmt.Printf("d %d not found %v\n", s.d, err)
			}
			a.nodes[i][j].key = kb
			a.nodes[i][j].val = v
		}
		if a.trace {
			fmt.Printf("\n")
		}
	}
	return nil
}

type BtIndexWriter struct {
	maxOffset  uint64
	prevOffset uint64
	minDelta   uint64
	indexW     *bufio.Writer
	indexF     *os.File
	ef         *eliasfano32.EliasFano
	collector  *etl.Collector

	args BtIndexWriterArgs

	indexFileName string
	tmpFilePath   string

	numBuf      [8]byte
	keysWritten uint64

	built   bool
	lvl     log.Lvl
	logger  log.Logger
	noFsync bool // fsync is enabled by default, but tests can manually disable
}

type BtIndexWriterArgs struct {
	IndexFile   string // File name where the index and the minimal perfect hash function will be written to
	TmpDir      string
	M           uint64
	KeyCount    int
	EtlBufLimit datasize.ByteSize
	Lvl         log.Lvl
}

// NewBtIndexWriter creates a new BtIndexWriter instance with given number of keys
// Typical bucket size is 100 - 2048, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
// salt parameters is used to randomise the hash function construction, to ensure that different Erigon instances (nodes)
// are likely to use different hash function, to collision attacks are unlikely to slow down any meaningful number of nodes at the same time
func NewBtIndexWriter(args BtIndexWriterArgs, logger log.Logger) (*BtIndexWriter, error) {
	if args.EtlBufLimit == 0 {
		args.EtlBufLimit = etl.BufferOptimalSize / 2
	}
	if args.Lvl == 0 {
		args.Lvl = log.LvlTrace
	}

	btw := &BtIndexWriter{lvl: args.Lvl, logger: logger, args: args,
		tmpFilePath: args.IndexFile + ".tmp"}

	_, fname := filepath.Split(btw.args.IndexFile)
	btw.indexFileName = fname

	btw.collector = etl.NewCollector(BtreeLogPrefix+" "+fname, btw.args.TmpDir, etl.NewSortableBuffer(btw.args.EtlBufLimit), logger)
	btw.collector.LogLvl(btw.args.Lvl)

	return btw, nil
}

func (btw *BtIndexWriter) AddKey(key []byte, offset uint64) error {
	if btw.built {
		return errors.New("cannot add keys after perfect hash function had been built")
	}

	binary.BigEndian.PutUint64(btw.numBuf[:], offset)
	if offset > btw.maxOffset {
		btw.maxOffset = offset
	}

	keepKey := false
	if btw.keysWritten > 0 {
		delta := offset - btw.prevOffset
		if btw.keysWritten == 1 || delta < btw.minDelta {
			btw.minDelta = delta
		}
		keepKey = btw.keysWritten%btw.args.M == 0
	}

	var k []byte
	if keepKey {
		k = key
	}

	if err := btw.collector.Collect(btw.numBuf[:], k); err != nil {
		return err
	}
	btw.keysWritten++
	btw.prevOffset = offset
	return nil
}

// Build has to be called after all the keys have been added, and it initiates the process
// of building the perfect hash function and writing index into a file
func (btw *BtIndexWriter) Build() error {
	if btw.built {
		return errors.New("already built")
	}
	var err error
	if btw.indexF, err = os.Create(btw.tmpFilePath); err != nil {
		return fmt.Errorf("create index file %s: %w", btw.args.IndexFile, err)
	}
	defer btw.indexF.Close()
	btw.indexW = bufio.NewWriterSize(btw.indexF, etl.BufIOSize)

	defer btw.collector.Close()
	log.Log(btw.args.Lvl, "[index] calculating", "file", btw.indexFileName)

	if btw.keysWritten > 0 {
		btw.ef = eliasfano32.NewEliasFano(btw.keysWritten, btw.maxOffset)

		nodes := make([]Node, 0, btw.keysWritten/btw.args.M)
		var ki uint64
		if err = btw.collector.Load(nil, "", func(offt, k []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			btw.ef.AddOffset(binary.BigEndian.Uint64(offt))

			if len(k) > 0 { // for every M-th key, keep the key
				nodes = append(nodes, Node{key: common.Copy(k), di: ki})
			}
			ki++ // we need to keep key ordinal so count every key
			return nil
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		btw.ef.Build()

		if err := btw.ef.Write(btw.indexW); err != nil {
			return fmt.Errorf("[index] write ef: %w", err)
		}
		if err = encodeListNodes(nodes, btw.indexW); err != nil {
			return fmt.Errorf("[index] write nodes: %w", err)
		}
	}

	btw.logger.Log(btw.args.Lvl, "[index] write", "file", btw.indexFileName)
	btw.built = true

	if err = btw.indexW.Flush(); err != nil {
		return err
	}
	if err = btw.fsync(); err != nil {
		return err
	}
	if err = btw.indexF.Close(); err != nil {
		return err
	}
	if err = os.Rename(btw.tmpFilePath, btw.args.IndexFile); err != nil {
		return err
	}
	return nil
}

func (btw *BtIndexWriter) DisableFsync() { btw.noFsync = true }

// fsync - other processes/goroutines must see only "fully-complete" (valid) files. No partial-writes.
// To achieve it: write to .tmp file then `rename` when file is ready.
// Machine may power-off right after `rename` - it means `fsync` must be before `rename`
func (btw *BtIndexWriter) fsync() error {
	if btw.noFsync {
		return nil
	}
	if err := btw.indexF.Sync(); err != nil {
		btw.logger.Warn("couldn't fsync", "err", err, "file", btw.tmpFilePath)
		return err
	}
	return nil
}

func (btw *BtIndexWriter) Close() {
	if btw.indexF != nil {
		btw.indexF.Close()
	}
	if btw.collector != nil {
		btw.collector.Close()
	}
	//if btw.offsetCollector != nil {
	//	btw.offsetCollector.Close()
	//}
}

type BtIndex struct {
	m        mmap.MMap
	data     []byte
	ef       *eliasfano32.EliasFano
	file     *os.File
	alloc    *btAlloc // pointless?
	bplus    *BpsTree
	size     int64
	modTime  time.Time
	filePath string
}

// Decompressor should be managed by caller (could be closed after index is built). When index is built, external getter should be passed to seekInFiles function
func CreateBtreeIndexWithDecompressor(indexPath string, M uint64, decompressor *seg.Decompressor, compressed seg.FileCompression, seed uint32, ps *background.ProgressSet, tmpdir string, logger log.Logger, noFsync bool) (*BtIndex, error) {
	err := BuildBtreeIndexWithDecompressor(indexPath, decompressor, compressed, ps, tmpdir, seed, logger, noFsync)
	if err != nil {
		return nil, err
	}
	return OpenBtreeIndexWithDecompressor(indexPath, M, decompressor, compressed)
}

// OpenBtreeIndexAndDataFile opens btree index file and data file and returns it along with BtIndex instance
// Mostly useful for testing
func OpenBtreeIndexAndDataFile(indexPath, dataPath string, M uint64, compressed seg.FileCompression, trace bool) (*seg.Decompressor, *BtIndex, error) {
	kv, err := seg.NewDecompressor(dataPath)
	if err != nil {
		return nil, nil, err
	}
	bt, err := OpenBtreeIndexWithDecompressor(indexPath, M, kv, compressed)
	if err != nil {
		kv.Close()
		return nil, nil, err
	}
	return kv, bt, nil
}

func BuildBtreeIndexWithDecompressor(indexPath string, kv *seg.Decompressor, compression seg.FileCompression, ps *background.ProgressSet, tmpdir string, salt uint32, logger log.Logger, noFsync bool) error {
	_, indexFileName := filepath.Split(indexPath)
	p := ps.AddNew(indexFileName, uint64(kv.Count()/2))
	defer ps.Delete(p)

	defer kv.EnableReadAhead().DisableReadAhead()
	bloomPath := strings.TrimSuffix(indexPath, ".bt") + ".kvei"

	bloom, err := NewExistenceFilter(uint64(kv.Count()/2), bloomPath)
	if err != nil {
		return err
	}
	if noFsync {
		bloom.DisableFsync()
	}

	args := BtIndexWriterArgs{
		IndexFile: indexPath,
		TmpDir:    tmpdir,
		M:         DefaultBtreeM,
	}

	iw, err := NewBtIndexWriter(args, logger)
	if err != nil {
		return err
	}
	defer iw.Close()

	getter := seg.NewReader(kv.MakeGetter(), compression)
	getter.Reset(0)

	key := make([]byte, 0, 64)
	var pos uint64

	for getter.HasNext() {
		key, _ = getter.Next(key[:0])
		err = iw.AddKey(key, pos)
		if err != nil {
			return err
		}
		hi, _ := murmur3.Sum128WithSeed(key, salt)
		bloom.AddHash(hi)
		pos, _ = getter.Skip()

		p.Processed.Add(1)
	}
	//logger.Warn("empty keys", "key lengths", ks, "total emptys", emptys, "total", kv.Count()/2)
	if err := iw.Build(); err != nil {
		return err
	}

	if bloom != nil {
		if err := bloom.Build(); err != nil {
			return err
		}
	}
	return nil
}

// For now, M is not stored inside index file.
func OpenBtreeIndexWithDecompressor(indexPath string, M uint64, kv *seg.Decompressor, compress seg.FileCompression) (bt *BtIndex, err error) {
	var validationPassed = false
	idx := &BtIndex{
		filePath: indexPath,
	}
	defer func() {
		// recover from panic if one occurred. Set err to nil if no panic
		if r := recover(); r != nil {
			// do r with only the stack trace
			err = fmt.Errorf("incomplete or not-fully downloaded file %s", indexPath)
		}
		if err != nil || !validationPassed {
			idx.Close()
			idx = nil
		}
	}()

	s, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}
	idx.size = s.Size()
	idx.modTime = s.ModTime()

	idx.file, err = os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	if idx.size == 0 {
		return idx, nil
	}

	idx.m, err = mmap.MapRegion(idx.file, int(idx.size), mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, err
	}
	idx.data = idx.m[:idx.size]

	var pos int
	if len(idx.data[pos:]) == 0 {
		return idx, nil
	}

	idx.ef, pos = eliasfano32.ReadEliasFano(idx.data[pos:])

	defer kv.EnableMadvNormal().DisableReadAhead()
	kvGetter := seg.NewReader(kv.MakeGetter(), compress)

	//fmt.Printf("open btree index %s with %d keys b+=%t data compressed %t\n", indexPath, idx.ef.Count(), UseBpsTree, idx.compressed)
	switch UseBpsTree {
	case true:
		if len(idx.data[pos:]) == 0 {
			idx.bplus = NewBpsTree(kvGetter, idx.ef, M, idx.dataLookup, idx.keyCmp)
			// fallback for files without nodes encoded
		} else {
			nodes, err := decodeListNodes(idx.data[pos:])
			if err != nil {
				return nil, err
			}
			idx.bplus = NewBpsTreeWithNodes(kvGetter, idx.ef, M, idx.dataLookup, idx.keyCmp, nodes)
		}
	default:
		idx.alloc = newBtAlloc(idx.ef.Count(), M, false, idx.dataLookup, idx.keyCmp)
		if idx.alloc != nil {
			idx.alloc.WarmUp(kvGetter)
		}
	}

	validationPassed = true
	return idx, nil
}

// dataLookup fetches key and value from data file by di (data index)
// di starts from 0 so di is never >= keyCount
func (b *BtIndex) dataLookup(di uint64, g *seg.Reader) (k, v []byte, offset uint64, err error) {
	if di >= b.ef.Count() {
		return nil, nil, 0, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di, b.FileName())
	}

	offset = b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d key not found, file: %s/%s", di, b.ef.Count(), b.FileName(), g.FileName())
	}

	k, _ = g.Next(nil)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d value not found, file: %s/%s", di, b.ef.Count(), b.FileName(), g.FileName())
	}
	v, _ = g.Next(nil)
	return k, v, offset, nil
}

// comparing `k` with item of index `di`. using buffer `kBuf` to avoid allocations
func (b *BtIndex) keyCmp(k []byte, di uint64, g *seg.Reader, resBuf []byte) (int, []byte, error) {
	if di >= b.ef.Count() {
		return 0, nil, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di+1, b.FileName())
	}

	offset := b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return 0, nil, fmt.Errorf("key at %d/%d not found, file: %s", di, b.ef.Count(), b.FileName())
	}

	resBuf, _ = g.Next(resBuf)

	//TODO: use `b.getter.Match` after https://github.com/erigontech/erigon/issues/7855
	return bytes.Compare(resBuf, k), resBuf, nil
	//return b.getter.Match(k), result, nil
}

// getter should be alive all the time of cursor usage
// Key and value is valid until cursor.Next is called
func (b *BtIndex) newCursor(ctx context.Context, k, v []byte, d uint64, g *seg.Reader) *Cursor {
	return &Cursor{
		ctx:    ctx,
		getter: g,
		key:    common.Copy(k),
		value:  common.Copy(v),
		d:      d,
		btt:    b,
	}
}

func (b *BtIndex) Size() int64 { return b.size }

func (b *BtIndex) ModTime() time.Time { return b.modTime }

func (b *BtIndex) FilePath() string { return b.filePath }

func (b *BtIndex) FileName() string { return path.Base(b.filePath) }

func (b *BtIndex) Empty() bool { return b == nil || b.ef == nil || b.ef.Count() == 0 }

func (b *BtIndex) KeyCount() uint64 {
	if b.Empty() {
		return 0
	}
	return b.ef.Count()
}

func (b *BtIndex) Close() {
	if b == nil {
		return
	}
	if b.m != nil {
		if err := b.m.Unmap(); err != nil {
			log.Log(dbg.FileCloseLogLevel, "unmap", "err", err, "file", b.FileName(), "stack", dbg.Stack())
		}
		b.m = nil
	}
	if b.file != nil {
		if err := b.file.Close(); err != nil {
			log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", b.FileName(), "stack", dbg.Stack())
		}
		b.file = nil
	}
	if b.bplus != nil {
		b.bplus.Close()
		b.bplus = nil
	}
}

// Get - exact match of key. `k == nil` - means not found
func (b *BtIndex) Get(lookup []byte, gr *seg.Reader) (k, v []byte, offsetInFile uint64, found bool, err error) {
	// TODO: optimize by "push-down" - instead of using seek+compare, alloc can have method Get which will return nil if key doesn't exists
	// alternativaly: can allocate cursor on-stack
	// 	it := Iter{} // allocation on stack
	//  it.Initialize(file)

	if b.Empty() {
		return k, v, 0, false, nil
	}

	var index uint64
	// defer func() {
	// 	fmt.Printf("[Bindex][%s] Get (%t) '%x' -> '%x' di=%d err %v\n", b.FileName(), found, lookup, v, index, err)
	// }()
	if UseBpsTree {
		if b.bplus == nil {
			panic(fmt.Errorf("Get: `b.bplus` is nil: %s", gr.FileName()))
		}
		// v is actual value, not offset.

		// weak assumption that k will be ignored and used lookup instead.
		// since fetching k and v from data file is required to use Getter.
		// Why to do Getter.Reset twice when we can get kv right there.

		k, found, index, err = b.bplus.Get(gr, lookup)
	} else {
		if b.alloc == nil {
			return k, v, 0, false, err
		}
		k, found, index, err = b.alloc.Get(gr, lookup)
	}
	if err != nil || !found {
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return k, v, offsetInFile, false, nil
		}
		return nil, nil, 0, false, err
	}

	// this comparation should be done by index get method, and in case of mismatch, key is not found
	//if !bytes.Equal(k, lookup) {
	//	return k, v, false, nil
	//}
	k, v, offsetInFile, err = b.dataLookup(index, gr)
	if err != nil {
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return k, v, offsetInFile, false, nil
		}
		return k, v, offsetInFile, false, err
	}
	return k, v, offsetInFile, true, nil
}

// Seek moves cursor to position where key >= x.
// Then if x == nil - first key returned
//
//	if x is larger than any other key in index, nil cursor is returned.
func (b *BtIndex) Seek(g *seg.Reader, x []byte) (*Cursor, error) {
	if b.Empty() {
		return nil, nil
	}
	if UseBpsTree {
		k, v, dt, _, err := b.bplus.Seek(g, x)
		if err != nil /*|| !found*/ {
			if errors.Is(err, ErrBtIndexLookupBounds) {
				return nil, nil
			}
			return nil, err
		}
		if bytes.Compare(k, x) >= 0 {
			return b.newCursor(context.Background(), k, v, dt, g), nil
		}
		return nil, nil
	}

	_, dt, found, err := b.alloc.Seek(g, x)
	if err != nil || !found {
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return nil, nil
		}
		return nil, err
	}

	k, v, _, err := b.dataLookup(dt, g)
	if err != nil {
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return nil, nil
		}
		return nil, err
	}
	return b.newCursor(context.Background(), k, v, dt, g), nil
}

func (b *BtIndex) OrdinalLookup(getter *seg.Reader, i uint64) *Cursor {
	k, v, _, err := b.dataLookup(i, getter)
	if err != nil {
		return nil
	}
	return b.newCursor(context.Background(), k, v, i, getter)
}

func (b *BtIndex) Offsets() *eliasfano32.EliasFano { return b.bplus.Offsets() }
func (b *BtIndex) Distances() (map[int]int, error) { return b.bplus.Distances() }

func commonPrefixLength(b1, b2 []byte) int {
	var i int

	// fmt.Printf("CPL %x | %x", b1, b2)
	// defer func() { fmt.Printf(" -> %x len=%d\n", b1[:i], i) }()
	for i < len(b1) && i < len(b2) {
		if b1[i] != b2[i] {
			break
		}
		i++
	}
	return i
}

type Btrie struct {
	child [256]*trieNode
}

type trieNode struct {
	child map[byte]*trieNode
	ext   []byte
	di    *uint64
}

func newTrieNode(di uint64, ext []byte) *trieNode {
	tn := &trieNode{
		child: make(map[byte]*trieNode),
		ext:   make([]byte, len(ext)),
		di:    &di,
	}
	copy(tn.ext, ext)
	return tn
}

func (tn *trieNode) nextChildTo(nibble byte) (byte, bool) {
	if len(tn.child) == 0 {
		return 0, false
	}
	for b := nibble + 1; b <= 255; b++ {
		if tn.child[b] != nil {
			return b, true
		}
	}
	return 0, false
}

// When split needed ?
//   - insert new node with partially matching extension.
//
// Split updates original node - it becomes a new branch.
//
//	Leaf or previous branch goes down with nibble=ext[cpl:] and keeps di/child.
//	new node become new leaf with nibble=ext2[cpl:]
//	 // would it work if cpl == 0, so
//
//	 extension is part of a key without [0] nibble - it becomes index nibble to access this node
func (tn *trieNode) split(ext2 []byte, cpl int, di uint64) {
	if cpl == len(tn.ext) {
		// ext2 has prefix tn.ext.
		panic("help me")
	}
	rest := newTrieNode(*tn.di, tn.ext[cpl:])
	for n, c := range tn.child {
		rest.child[n] = c
	}
	clear(tn.child)

	insLeaf := newTrieNode(di, ext2[cpl:])

	tn.child[tn.ext[cpl]] = rest
	tn.child[ext2[cpl]] = insLeaf

	fmt.Printf("split %x cpl=%d onto (rest %x) and (leaf %x)\n", tn.ext, cpl, rest.ext, insLeaf.ext)

	tn.ext = tn.ext[:cpl]
	tn.di = nil
}

func NewBtrie() *Btrie {
	return &Btrie{}
}

func (bt *Btrie) printRoot() {
	for n := 0; n < len(bt.child); n++ {
		node := bt.child[n]
		if node != nil {
			if node.di != nil {
				fmt.Printf(" %x: %p [%d] %x\n", n, node, node.di, node.ext)
			} else {
				fmt.Printf(" %x: %p [%x] branch size %d\n", n, node, node.ext, len(node.child))

			}
		}
	}
}

// lookup first di which key >= seekKey
func (bt *Btrie) Seek(key []byte) (di uint64, skey []byte) {
	if len(key) == 0 {
		return 0, nil
	}

	depth := 0
	nib := key[depth]

	fmt.Printf("Seek '%x'\n", key)

	var next, kin *trieNode
	if bt.child[nib] != nil {
		next = bt.child[nib]
		for b := nib + 1; b <= 255; b++ {
			if bt.child[b] != nil {
				kin = bt.child[b] // top level neighbour, keeping track of next largest
				break
			}
		}
	} else {
		for b := nib + 1; b <= 255; b++ {
			if bt.child[b] != nil {
				next = bt.child[b]
				break
			}
			// this next is already bigger than requested key
		}
		if next == nil {
			return 2<<63 - 1, nil
		}
	}
	_ = kin
	depth++

	for {
		fmt.Printf("next depth %d %x\n", depth, next.ext)
		cpl := commonPrefixLength(key[depth:], next.ext)
		if cpl == 0 {
			if len(next.ext) > 0 {

				// need to use partial prefix DI values. Like if we matched some part and the rest is bigger, can exit straight away
			}
		}

	}

	return 0, nil
}

// Get seeks exact match to the key.
// Key not found - di > N
// key found     - di = it's exact position
func (bt *Btrie) Get(key []byte) (di uint64) {
	depth := 0
	nib := key[depth]

	fmt.Printf("Get '%x'\n", key)

	if bt.child[nib] == nil {
		return 2<<63 - 1
	}

	depth++
	next := bt.child[nib]
	traversed := make([]byte, 0, len(key))
	traversed = append(traversed, key[:depth]...)
	for {
		fmt.Printf("next depth %d %x\n", depth, next.ext)
		cpl := commonPrefixLength(key[depth:], next.ext)
		if cpl == 0 { // full mismatch
			if len(next.ext) == 0 {
				nib := key[depth:][cpl]
				nx := next.child[nib]
				fmt.Printf("depth=%d part t=%x nibble %x\n", depth, traversed, nib)
				if nx != nil { // not found
					next = nx
					traversed = append(traversed, nib)
					// traversed = append(traversed[depth:], key[depth:depth+cpl]...)
					if depth+1 == len(key) {
						fmt.Printf("found %x depth=%d\n", traversed, depth)
						return *next.di
					}
					depth += cpl
					continue
				}
			}
			fmt.Printf("not found depth=%d key=%x t=%x suff %x\n", depth, key, traversed, next.ext)
			return 2<<63 - 1
		}
		if cpl < len(next.ext) { // partially matched but not found
			fmt.Printf("not found depth=%d key=%x t=%x rest %x\n", depth, key, traversed, next.ext[cpl:])
			return 2<<63 - 1
		}
		if cpl == len(next.ext) {
			if cpl == len(key[depth:]) { // full match

				traversed = append(traversed[depth:], key[depth:depth+cpl]...)
				fmt.Printf("found %x depth=%d\n", traversed, depth)
				return *next.di
			}
			// next.ext is a prefix of key[depth:]
			nib := key[depth:][cpl]
			nx := next.child[nib]
			fmt.Printf("depth=%d part t=%x nibble %x\n", depth, traversed, nib)
			if nx == nil { // not found
				fmt.Printf("not found depth=%d key=%x t=%x\n", depth, key, traversed)
				return 2<<63 - 1
			}
			next = nx
			traversed = append(traversed[depth:], key[depth:depth+cpl]...)
			depth += cpl
			continue
		}

		panic("wtf")
		traversed = append(traversed[depth:], key[depth:depth+cpl]...)
		depth += cpl
		fmt.Printf("depth=%d t=%x\n", depth, traversed)
		if cpl != len(next.ext) {

		}

	}

	return 0
}

func (bt *Btrie) Insert(key []byte, di uint64) {
	if len(key) == 0 {
		return // nothing to insert
	}
	ki := 0 // reflects current position in key
	nib := key[ki]
	ki++

	fmt.Printf("insert key %x di=%d\n", key, di)

	if bt.child[nib] == nil {
		bt.child[nib] = newTrieNode(di, key[ki:])
		fmt.Printf("new root-leaf key %x di=%d ext %x\n", key, bt.child[nib].di, bt.child[nib].ext)
		return
	}

	next := bt.child[nib]
	depth := 0 //
	for {
		depth++
		cpl := commonPrefixLength(key[ki:], next.ext)
		if cpl == 0 { // no matching prefix
			nib = key[ki]
			if nx := next.child[nib]; nx != nil {
				fmt.Printf("-depth=%d next [%d] %x\n", depth, nx.di, nx.ext[:])
				next = nx
				continue
			}
			// ki++
			// _ = key[ki]

			fmt.Printf("-depth=%d insert [%d] %x\n", depth, di, key[ki:])
			next.split(key[ki:], cpl, di)
			return
		}
		if cpl == len(next.ext) {
			nib = key[cpl]
			if nx := next.child[nib]; nx != nil {
				fmt.Printf("depth=%d next [%d] %x\n", depth, nx.di, nx.ext[:])
				next = nx
				continue
			}
			ki += cpl
			// _ = key[ki]

			fmt.Printf("depth=%d insert [%d] %x\n", depth, di, key[ki:])

			next.child[nib] = newTrieNode(di, key[ki:])
			return

		}

		next.split(key[ki:], cpl, di)
		ki += cpl
		return
	}
}

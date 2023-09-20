package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/edsrzf/mmap-go"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common/background"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
)

func logBase(n, base uint64) uint64 {
	return uint64(math.Ceil(math.Log(float64(n)) / math.Log(float64(base))))
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
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
	ctx context.Context
	ix  *btAlloc

	key   []byte
	value []byte
	d     uint64
}

func (a *btAlloc) newCursor(ctx context.Context, k, v []byte, d uint64) *Cursor {
	return &Cursor{
		ctx:   ctx,
		key:   common.Copy(k),
		value: common.Copy(v),
		d:     d,
		ix:    a,
	}
}

func (c *Cursor) Key() []byte {
	return c.key
}

func (c *Cursor) Ordinal() uint64 {
	return c.d
}

func (c *Cursor) Value() []byte {
	return c.value
}

func (c *Cursor) Next() bool {
	if c.d > c.ix.K-1 {
		return false
	}
	k, v, err := c.ix.dataLookup(c.d + 1)
	if err != nil {
		return false
	}
	c.key = common.Copy(k)
	c.value = common.Copy(v)
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

	dataLookup func(di uint64) ([]byte, []byte, error)
}

func newBtAlloc(k, M uint64, trace bool) *btAlloc {
	if k == 0 {
		return nil
	}

	d := logBase(k, M)
	a := &btAlloc{
		vx:      make([]uint64, d+1),
		sons:    make([][]uint64, d+1),
		cursors: make([]markupCursor, d),
		nodes:   make([][]node, d),
		M:       M,
		K:       k,
		d:       d,
		trace:   trace,
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
		a.vx[i] = min64(uint64(math.Pow(float64(M), float64(i))), nnc)
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

// nolint
// another implementation of traverseDfs supposed to be a bit cleaner but buggy yet
func (a *btAlloc) traverseTrick() {
	for l := 0; l < len(a.sons)-1; l++ {
		if len(a.sons[l]) < 2 {
			panic("invalid btree allocation markup")
		}
		a.cursors[l] = markupCursor{uint64(l), 1, 0, 0}
		a.nodes[l] = make([]node, 0)
	}

	lf := a.cursors[len(a.cursors)-1]
	c := a.cursors[(len(a.cursors) - 2)]

	var d uint64
	var fin bool

	lf.di = d
	lf.si++
	d++
	a.cursors[len(a.cursors)-1] = lf

	moved := true
	for int(c.p) <= len(a.sons[c.l]) {
		if fin || d > a.K {
			break
		}
		c, lf = a.cursors[c.l], a.cursors[lf.l]

		c.di = d
		c.si++

		sons := a.sons[lf.l][lf.p]
		for i := uint64(1); i < sons; i++ {
			lf.si++
			d++
		}
		lf.di = d
		d++

		a.nodes[lf.l] = append(a.nodes[lf.l], node{p: lf.p, s: lf.si, d: lf.di})
		a.nodes[c.l] = append(a.nodes[c.l], node{p: c.p, s: c.si, d: c.di})
		a.cursors[lf.l] = lf
		a.cursors[c.l] = c

		for l := lf.l; l >= 0; l-- {
			sc := a.cursors[l]
			sons, gsons := a.sons[sc.l][sc.p-1], a.sons[sc.l][sc.p]
			if l < c.l && moved {
				sc.di = d
				a.nodes[sc.l] = append(a.nodes[sc.l], node{d: sc.di})
				sc.si++
				d++
			}
			moved = (sc.si-1)/gsons != sc.si/gsons
			if sc.si/gsons >= sons {
				sz := uint64(len(a.sons[sc.l]) - 1)
				if sc.p+2 > sz {
					fin = l == lf.l
					break
				} else {
					sc.p += 2
					sc.si, sc.di = 0, 0
				}
				//moved = true
			}
			if l == lf.l {
				sc.si++
				sc.di = d
				d++
			}
			a.cursors[l] = sc
			if l == 0 {
				break
			}
		}
		moved = false
	}
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

func (a *btAlloc) bsKey(x []byte, l, r uint64) (*Cursor, error) {
	for l <= r {
		di := (l + r) >> 1

		mk, value, err := a.dataLookup(di)
		a.naccess++

		cmp := bytes.Compare(mk, x)
		switch {
		case err != nil:
			if errors.Is(err, ErrBtIndexLookupBounds) {
				return nil, nil
			}
			return nil, err
		case cmp == 0:
			return a.newCursor(context.TODO(), mk, value, di), nil
		case cmp == -1:
			l = di + 1
		default:
			r = di
		}
		if l == r {
			break
		}
	}
	k, v, err := a.dataLookup(l)
	if err != nil {
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return nil, nil
		}
		return nil, fmt.Errorf("key >= %x was not found. %w", x, err)
	}
	return a.newCursor(context.TODO(), k, v, l), nil
}

func (a *btAlloc) bsNode(i, l, r uint64, x []byte) (n node, lm int64, rm int64) {
	lm, rm = -1, -1
	var m uint64

	for l < r {
		m = (l + r) >> 1

		a.naccess++
		cmp := bytes.Compare(a.nodes[i][m].key, x)
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
	for i := range a.nodes[lvl] {
		if a.nodes[lvl][i].d >= d {
			return uint64(i)
		}
	}
	return uint64(len(a.nodes[lvl]))
}

func (a *btAlloc) Seek(ik []byte) (*Cursor, error) {
	if a.trace {
		fmt.Printf("seek key %x\n", ik)
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
		ln, lm, rm = a.bsNode(uint64(l), L, R, ik)
		if ln.key == nil { // should return node which is nearest to key from the left so never nil
			if a.trace {
				fmt.Printf("found nil key %x pos_range[%d-%d] naccess_ram=%d\n", l, lm, rm, a.naccess)
			}
			return nil, fmt.Errorf("bt index nil node at level %d", l)
		}

		switch bytes.Compare(ln.key, ik) {
		case 1: // key > ik
			maxD = ln.d
		case -1: // key < ik
			minD = ln.d
		case 0:
			if a.trace {
				fmt.Printf("found key %x v=%x naccess_ram=%d\n", ik, ln.val /*level[m].d,*/, a.naccess)
			}
			return a.newCursor(context.TODO(), common.Copy(ln.key), common.Copy(ln.val), ln.d), nil
		}

		if rm-lm >= 1 {
			break
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

		if a.trace {
			fmt.Printf("range={%x d=%d p=%d} (%d, %d) L=%d naccess_ram=%d\n", ln.key, ln.d, ln.p, minD, maxD, l, a.naccess)
		}
	}

	a.naccess = 0 // reset count before actually go to disk
	cursor, err := a.bsKey(ik, minD, maxD)
	if err != nil {
		if a.trace {
			fmt.Printf("key %x not found\n", ik)
		}
		return nil, err
	}

	if a.trace {
		fmt.Printf("finally found key %x v=%x naccess_disk=%d\n", cursor.key, cursor.value, a.naccess)
	}
	return cursor, nil
}

func (a *btAlloc) fillSearchMx() {
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

			kb, v, err := a.dataLookup(s.d)
			if err != nil {
				fmt.Printf("d %d not found %v\n", s.d, err)
			}
			a.nodes[i][j].key = common.Copy(kb)
			a.nodes[i][j].val = common.Copy(v)
		}
		if a.trace {
			fmt.Printf("\n")
		}
	}
}

// deprecated
type BtIndexReader struct {
	index *BtIndex
}

func NewBtIndexReader(index *BtIndex) *BtIndexReader {
	return &BtIndexReader{
		index: index,
	}
}

// Lookup wraps index Lookup
func (r *BtIndexReader) Lookup(key []byte) uint64 {
	if r.index != nil {
		return r.index.Lookup(key)
	}
	return 0
}

func (r *BtIndexReader) Lookup2(key1, key2 []byte) uint64 {
	fk := make([]byte, 52)
	copy(fk[:length.Addr], key1)
	copy(fk[length.Addr:], key2)

	if r.index != nil {
		return r.index.Lookup(fk)
	}
	return 0
}

func (r *BtIndexReader) Seek(x []byte) (*Cursor, error) {
	if r.index != nil {
		cursor, err := r.index.alloc.Seek(x)
		if err != nil {
			return nil, fmt.Errorf("seek key %x: %w", x, err)
		}
		return cursor, nil
	}
	return nil, fmt.Errorf("seek has been failed")
}

func (r *BtIndexReader) Empty() bool {
	return r.index.Empty()
}

type BtIndexWriter struct {
	built           bool
	lvl             log.Lvl
	maxOffset       uint64
	prevOffset      uint64
	minDelta        uint64
	indexW          *bufio.Writer
	indexF          *os.File
	bucketCollector *etl.Collector // Collector that sorts by buckets

	indexFileName          string
	indexFile, tmpFilePath string

	tmpDir      string
	numBuf      [8]byte
	keyCount    uint64
	etlBufLimit datasize.ByteSize
	bytesPerRec int
	logger      log.Logger
	noFsync     bool // fsync is enabled by default, but tests can manually disable
}

type BtIndexWriterArgs struct {
	IndexFile   string // File name where the index and the minimal perfect hash function will be written to
	TmpDir      string
	KeyCount    int
	EtlBufLimit datasize.ByteSize
}

const BtreeLogPrefix = "btree"

// NewBtIndexWriter creates a new BtIndexWriter instance with given number of keys
// Typical bucket size is 100 - 2048, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
// salt parameters is used to randomise the hash function construction, to ensure that different Erigon instances (nodes)
// are likely to use different hash function, to collision attacks are unlikely to slow down any meaningful number of nodes at the same time
func NewBtIndexWriter(args BtIndexWriterArgs, logger log.Logger) (*BtIndexWriter, error) {
	btw := &BtIndexWriter{lvl: log.LvlDebug, logger: logger}
	btw.tmpDir = args.TmpDir
	btw.indexFile = args.IndexFile
	btw.tmpFilePath = args.IndexFile + ".tmp"

	_, fname := filepath.Split(btw.indexFile)
	btw.indexFileName = fname
	btw.etlBufLimit = args.EtlBufLimit
	if btw.etlBufLimit == 0 {
		btw.etlBufLimit = etl.BufferOptimalSize
	}

	btw.bucketCollector = etl.NewCollector(BtreeLogPrefix+" "+fname, btw.tmpDir, etl.NewSortableBuffer(btw.etlBufLimit), logger)
	btw.bucketCollector.LogLvl(log.LvlDebug)

	btw.maxOffset = 0
	return btw, nil
}

// loadFuncBucket is required to satisfy the type etl.LoadFunc type, to use with collector.Load
func (btw *BtIndexWriter) loadFuncBucket(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
	// k is the BigEndian encoding of the bucket number, and the v is the key that is assigned into that bucket
	//if uint64(len(btw.vals)) >= btw.batchSizeLimit {
	//	if err := btw.drainBatch(); err != nil {
	//		return err
	//	}
	//}

	// if _, err := btw.indexW.Write(k); err != nil {
	// 	return err
	// }
	if _, err := btw.indexW.Write(v[8-btw.bytesPerRec:]); err != nil {
		return err
	}

	//btw.keys = append(btw.keys, binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(k[8:]))
	//btw.vals = append(btw.vals, binary.BigEndian.Uint64(v))
	return nil
}

// Build has to be called after all the keys have been added, and it initiates the process
// of building the perfect hash function and writing index into a file
func (btw *BtIndexWriter) Build() error {
	if btw.built {
		return fmt.Errorf("already built")
	}
	//if btw.keysAdded != btw.keyCount {
	//	return fmt.Errorf("expected keys %d, got %d", btw.keyCount, btw.keysAdded)
	//}
	var err error
	if btw.indexF, err = os.Create(btw.tmpFilePath); err != nil {
		return fmt.Errorf("create index file %s: %w", btw.indexFile, err)
	}
	defer btw.indexF.Close()
	btw.indexW = bufio.NewWriterSize(btw.indexF, etl.BufIOSize)

	// Write number of keys
	binary.BigEndian.PutUint64(btw.numBuf[:], btw.keyCount)
	if _, err = btw.indexW.Write(btw.numBuf[:]); err != nil {
		return fmt.Errorf("write number of keys: %w", err)
	}
	// Write number of bytes per index record
	btw.bytesPerRec = common.BitLenToByteLen(bits.Len64(btw.maxOffset))
	if err = btw.indexW.WriteByte(byte(btw.bytesPerRec)); err != nil {
		return fmt.Errorf("write bytes per record: %w", err)
	}

	defer btw.bucketCollector.Close()
	log.Log(btw.lvl, "[index] calculating", "file", btw.indexFileName)
	if err := btw.bucketCollector.Load(nil, "", btw.loadFuncBucket, etl.TransformArgs{}); err != nil {
		return err
	}

	btw.logger.Log(btw.lvl, "[index] write", "file", btw.indexFileName)
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
	if err = os.Rename(btw.tmpFilePath, btw.indexFile); err != nil {
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
	if btw.bucketCollector != nil {
		btw.bucketCollector.Close()
	}
	//if btw.offsetCollector != nil {
	//	btw.offsetCollector.Close()
	//}
}

func (btw *BtIndexWriter) AddKey(key []byte, offset uint64) error {
	if btw.built {
		return fmt.Errorf("cannot add keys after perfect hash function had been built")
	}

	binary.BigEndian.PutUint64(btw.numBuf[:], offset)
	if offset > btw.maxOffset {
		btw.maxOffset = offset
	}
	if btw.keyCount > 0 {
		delta := offset - btw.prevOffset
		if btw.keyCount == 1 || delta < btw.minDelta {
			btw.minDelta = delta
		}
	}

	if err := btw.bucketCollector.Collect(key, btw.numBuf[:]); err != nil {
		return err
	}
	btw.keyCount++
	btw.prevOffset = offset
	return nil
}

type BtIndex struct {
	alloc        *btAlloc
	m            mmap.MMap
	data         []byte
	file         *os.File
	size         int64
	modTime      time.Time
	filePath     string
	keyCount     uint64
	bytesPerRec  int
	dataoffset   uint64
	auxBuf       []byte
	decompressor *compress.Decompressor
	getter       *compress.Getter
}

func CreateBtreeIndex(indexPath, dataPath string, M uint64, logger log.Logger) (*BtIndex, error) {
	err := BuildBtreeIndex(dataPath, indexPath, logger)
	if err != nil {
		return nil, err
	}
	return OpenBtreeIndex(indexPath, dataPath, M)
}

var DefaultBtreeM = uint64(2048)

func CreateBtreeIndexWithDecompressor(indexPath string, M uint64, decompressor *compress.Decompressor, p *background.Progress, tmpdir string, logger log.Logger) (*BtIndex, error) {
	err := BuildBtreeIndexWithDecompressor(indexPath, decompressor, p, tmpdir, logger)
	if err != nil {
		return nil, err
	}
	return OpenBtreeIndexWithDecompressor(indexPath, M, decompressor)
}

func BuildBtreeIndexWithDecompressor(indexPath string, kv *compress.Decompressor, p *background.Progress, tmpdir string, logger log.Logger) error {
	defer kv.EnableReadAhead().DisableReadAhead()

	args := BtIndexWriterArgs{
		IndexFile: indexPath,
		TmpDir:    tmpdir,
	}

	iw, err := NewBtIndexWriter(args, logger)
	if err != nil {
		return err
	}

	getter := kv.MakeGetter()
	getter.Reset(0)

	key := make([]byte, 0, 64)
	ks := make(map[int]int)

	var pos, kp uint64
	emptys := 0
	for getter.HasNext() {
		p.Processed.Add(1)
		key, kp = getter.Next(key[:0])
		err = iw.AddKey(key, pos)
		if err != nil {
			return err
		}

		pos, _ = getter.Skip()
		if pos-kp == 1 {
			ks[len(key)]++
			emptys++
		}
	}
	//fmt.Printf("emptys %d %#+v\n", emptys, ks)

	if err := iw.Build(); err != nil {
		return err
	}
	iw.Close()
	return nil
}

// Opens .kv at dataPath and generates index over it to file 'indexPath'
func BuildBtreeIndex(dataPath, indexPath string, logger log.Logger) error {
	decomp, err := compress.NewDecompressor(dataPath)
	if err != nil {
		return err
	}
	defer decomp.Close()

	defer decomp.EnableReadAhead().DisableReadAhead()

	args := BtIndexWriterArgs{
		IndexFile: indexPath,
		TmpDir:    filepath.Dir(indexPath),
	}

	iw, err := NewBtIndexWriter(args, logger)
	if err != nil {
		return err
	}
	defer iw.Close()

	getter := decomp.MakeGetter()
	getter.Reset(0)

	key := make([]byte, 0, 64)

	var pos uint64
	for getter.HasNext() {
		key, _ = getter.Next(key[:0])
		err = iw.AddKey(key, pos)
		if err != nil {
			return err
		}

		pos, _ = getter.Skip()
	}
	decomp.Close()

	if err := iw.Build(); err != nil {
		return err
	}
	iw.Close()
	return nil
}

func OpenBtreeIndexWithDecompressor(indexPath string, M uint64, kv *compress.Decompressor) (*BtIndex, error) {
	s, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}

	idx := &BtIndex{
		filePath: indexPath,
		size:     s.Size(),
		modTime:  s.ModTime(),
		auxBuf:   make([]byte, 64),
	}

	idx.file, err = os.Open(indexPath)
	if err != nil {
		return nil, err
	}

	idx.m, err = mmap.MapRegion(idx.file, int(idx.size), mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, err
	}
	idx.data = idx.m[:idx.size]

	// Read number of keys and bytes per record
	pos := 8
	idx.keyCount = binary.BigEndian.Uint64(idx.data[:pos])
	if idx.keyCount == 0 {
		return idx, nil
	}
	idx.bytesPerRec = int(idx.data[pos])
	pos += 1

	//p := (*[]byte)(unsafe.Pointer(&idx.data[pos]))
	//l := int(idx.keyCount)*idx.bytesPerRec + (16 * int(idx.keyCount))

	idx.getter = kv.MakeGetter()

	idx.dataoffset = uint64(pos)
	idx.alloc = newBtAlloc(idx.keyCount, M, false)
	if idx.alloc != nil {
		idx.alloc.dataLookup = idx.dataLookup
		idx.alloc.traverseDfs()
		defer idx.decompressor.EnableReadAhead().DisableReadAhead()
		idx.alloc.fillSearchMx()
	}
	return idx, nil
}

func OpenBtreeIndex(indexPath, dataPath string, M uint64) (*BtIndex, error) {
	s, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}

	idx := &BtIndex{
		filePath: indexPath,
		size:     s.Size(),
		modTime:  s.ModTime(),
		auxBuf:   make([]byte, 64),
	}

	idx.file, err = os.Open(indexPath)
	if err != nil {
		return nil, err
	}

	idx.m, err = mmap.MapRegion(idx.file, int(idx.size), mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, err
	}
	idx.data = idx.m[:idx.size]

	// Read number of keys and bytes per record
	pos := 8
	idx.keyCount = binary.BigEndian.Uint64(idx.data[:pos])
	idx.bytesPerRec = int(idx.data[pos])
	pos += 1

	// offset := int(idx.keyCount) * idx.bytesPerRec //+ (idx.keySize * int(idx.keyCount))
	// if offset < 0 {
	// 	return nil, fmt.Errorf("offset is: %d which is below zero, the file: %s is broken", offset, indexPath)
	// }

	//p := (*[]byte)(unsafe.Pointer(&idx.data[pos]))
	//l := int(idx.keyCount)*idx.bytesPerRec + (16 * int(idx.keyCount))

	idx.decompressor, err = compress.NewDecompressor(dataPath)
	if err != nil {
		idx.Close()
		return nil, err
	}
	idx.getter = idx.decompressor.MakeGetter()

	idx.dataoffset = uint64(pos)
	idx.alloc = newBtAlloc(idx.keyCount, M, false)
	if idx.alloc != nil {
		idx.alloc.dataLookup = idx.dataLookup
		idx.alloc.traverseDfs()
		defer idx.decompressor.EnableReadAhead().DisableReadAhead()
		idx.alloc.fillSearchMx()
	}
	return idx, nil
}

var ErrBtIndexLookupBounds = errors.New("BtIndex: lookup di bounds error")

// dataLookup fetches key and value from data file by di (data index)
// di starts from 0 so di is never >= keyCount
func (b *BtIndex) dataLookup(di uint64) ([]byte, []byte, error) {
	if di >= b.keyCount {
		return nil, nil, fmt.Errorf("%w: keyCount=%d, item %d requested. file: %s", ErrBtIndexLookupBounds, b.keyCount, di+1, b.FileName())
	}
	p := int(b.dataoffset) + int(di)*b.bytesPerRec
	if len(b.data) < p+b.bytesPerRec {
		return nil, nil, fmt.Errorf("data lookup gone too far (%d after %d). keyCount=%d, requesed item %d. file: %s", p+b.bytesPerRec-len(b.data), len(b.data), b.keyCount, di, b.FileName())
	}

	var aux [8]byte
	dst := aux[8-b.bytesPerRec:]
	copy(dst, b.data[p:p+b.bytesPerRec])

	offset := binary.BigEndian.Uint64(aux[:])
	b.getter.Reset(offset)
	if !b.getter.HasNext() {
		return nil, nil, fmt.Errorf("pair %d not found. keyCount=%d. file: %s", di, b.keyCount, b.FileName())
	}

	key, kp := b.getter.Next(nil)

	if !b.getter.HasNext() {
		return nil, nil, fmt.Errorf("pair %d not found. keyCount=%d. file: %s", di, b.keyCount, b.FileName())
	}
	val, vp := b.getter.Next(nil)
	_, _ = kp, vp
	return key, val, nil
}

func (b *BtIndex) Size() int64 { return b.size }

func (b *BtIndex) ModTime() time.Time { return b.modTime }

func (b *BtIndex) FilePath() string { return b.filePath }

func (b *BtIndex) FileName() string { return path.Base(b.filePath) }

func (b *BtIndex) Empty() bool { return b == nil || b.keyCount == 0 }

func (b *BtIndex) KeyCount() uint64 { return b.keyCount }

func (b *BtIndex) Close() {
	if b == nil {
		return
	}
	if b.file != nil {
		if err := b.m.Unmap(); err != nil {
			log.Log(dbg.FileCloseLogLevel, "unmap", "err", err, "file", b.FileName(), "stack", dbg.Stack())
		}
		b.m = nil
		if err := b.file.Close(); err != nil {
			log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", b.FileName(), "stack", dbg.Stack())
		}
		b.file = nil
	}
	if b.decompressor != nil {
		b.decompressor.Close()
		b.decompressor = nil
	}
}

func (b *BtIndex) Seek(x []byte) (*Cursor, error) {
	if b.alloc == nil {
		return nil, nil
	}
	cursor, err := b.alloc.Seek(x)
	if err != nil {
		return nil, fmt.Errorf("seek key %x: %w", x, err)
	}
	// cursor could be nil along with err if nothing found
	return cursor, nil
}

// deprecated
func (b *BtIndex) Lookup(key []byte) uint64 {
	if b.alloc == nil {
		return 0
	}
	cursor, err := b.alloc.Seek(key)
	if err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(cursor.value)
}

func (b *BtIndex) OrdinalLookup(i uint64) *Cursor {
	if b.alloc == nil {
		return nil
	}
	if i > b.alloc.K {
		return nil
	}
	k, v, err := b.dataLookup(i)
	if err != nil {
		return nil
	}

	return &Cursor{
		key: k, value: v, d: i, ix: b.alloc,
	}
}

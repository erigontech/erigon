package db

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/log/v3"
)

var logger = log.New()

const (
	PageSize               = 4096
	MdbxMagic       uint64 = 0x59659DBDEF4C11
	MdbxDataVersion int    = 2

	BranchPageFlag   uint16 = 1
	LeafPageFlag     uint16 = 2
	OverflowPageFlag uint16 = 4

	// DupSortPagePlag uint16 = 8

	BigDataNodeFlag uint16 = 1
	SubDbNodeFlag   uint16 = 2
	DupDataNodeFlag uint16 = 4

	HeaderSize int = 20

	MdbxDataFile = "mdbx.dat"
)

var (
	colors = map[string]string{
		"green":  "#d5f5e3",
		"orange": "#f6ddcc",
		"purple": "#e8daef",
		"yellow": "#f9e79f",
	}
)

/* -------------------------- Helper functions -------------------------- */

// Checks if page flag is a LEAF
func isLeaf(flag uint16) bool {
	return flag&LeafPageFlag != 0
}

// Checks if page flag is a BRANCH
func isBranch(flag uint16) bool {
	return flag&BranchPageFlag != 0
}

// Checks if page flag is a OVERFLOW
func isOverflow(flag uint16) bool {
	return flag&OverflowPageFlag != 0
}

// Checks if node flag is a BIGDATA
func isBigData(flag uint16) bool {
	return flag&BigDataNodeFlag != 0
}

// Checks if node flag is a SUBDATA
func isSubDB(flag uint16) bool {
	return flag&SubDbNodeFlag != 0
}

// Checks if node flag is a DUPDATA
func isDupData(flag uint16) bool {
	return flag&DupDataNodeFlag != 0
}

// Reads 2 bytes starting from the position, converts them to uint16
func _16(page []byte, pos int) uint16 {
	return binary.LittleEndian.Uint16(page[pos:])
}

// Reads 4 bytes starting from the position, converts them to uint32
func _32(page []byte, pos int) uint32 {
	return binary.LittleEndian.Uint32(page[pos:])
}

// Reads 8 bytes starting from the position, converts them to uint64
func _64(page []byte, pos int) uint64 {
	return binary.LittleEndian.Uint64(page[pos:])
}

// Converts slice of int into a string
// all decreasing subsequances with size of 3 or more are written in short
// example1: 8, 7, 6, 5, 4 -> 8-4(5)
// example2: 8, 7, 5, 4, 3 -> 8, 7, 5-3(3)
// example3: 8, 7, 5, 4, 3, 2 -> 8, 7, 5-2(4)
// example4: 8, 7, 5, 4, 2, 1 -> 8, 7, 5, 4, 2, 1
func pagesToString(pages []uint32) (out string) {

	if len(pages) == 1 {
		out += fmt.Sprint(pages[0])
		return
	}

	if len(pages) == 2 {
		out += fmt.Sprint(pages[0])
		out += ", "
		out += fmt.Sprint(pages[1])
		return
	}

	added := 0
	container := []uint32{pages[0]}
	last := 0 // last item in container
	for i := 1; i < len(pages); i++ {

		if added >= 14 {
			out += fmt.Sprintf("...%d more pages...", len(pages)-added)
			return
		}

		pgno := pages[i]

		if container[last]-pgno == 1 {
			container = append(container, pgno)
			last++
		} else {
			if last > 1 {
				out += fmt.Sprintf("%d-%d(%d)", container[0], container[last], len(container))

				if i < len(pages) {
					out += ", "
				}

				added++
				container = []uint32{pgno}
				last = 0

			} else {

				for i, n := range container {
					if i < len(container)-1 {
						out += fmt.Sprintf("%d, ", n)
					} else {
						out += fmt.Sprintf("%d", n)
					}
				}

				if i < len(pages) {
					out += ", "
				}

				added++
				container = []uint32{pgno}
				last = 0
			}
		}

	}
	if last > 1 {
		out += fmt.Sprintf("%d-%d(%d)", container[0], container[last], len(container))
	} else {

		for i, n := range container {
			if i < len(container)-1 {
				out += fmt.Sprintf("%d, ", n)
			} else {
				out += fmt.Sprintf("%d", n)
			}
		}

	}
	return
}

/* ------------------- Core structures and their methods ------------------- */

// Common header for all page types. The page type depends on flags
type header struct {
	txnID        uint64
	leaf2keySize uint16
	flag         uint16
	lower        uint16
	upper        uint16
	pageID       uint32

	// ptrs  []uint16
	nodes []*mdbx_node
}

/* Information about a single database in the environment. */
type mdbx_db struct {
	flags     uint16 /* see mdbx_dbi_open */
	depth     uint16 /* depth of this tree */
	xsize     uint32 /* key-size for MDBX_DUPFIXED (LEAF2 pages) */
	rootID    uint32 /* the root page of this tree */
	branches  uint32 /* number of internal pages */
	leafs     uint32 /* number of leaf pages */
	overflows uint32 /* number of overflow pages */
	seq       uint64 //nolint
	entries   uint64 /* number of data items */
	txnID     uint64 /* txnid of last committed modification */
}

// nolint // database size-related parameters, used as placeholder, doesn't have any meaning in this code
type mdbx_geo struct {
	grow_pv   uint16 //nolint
	shrink_pv uint16 //nolint
	lower     uint32 //nolint
	upper     uint32 //nolint
	now       uint32 //nolint
	next      uint32 //nolint
}

/* used as placeholder, doesn't have any meaning in this code */
type mdbx_canary struct {
	field1 uint64 //nolint
	field2 uint64 //nolint
	field3 uint64 //nolint
	field4 uint64 //nolint
}

/* Meta page content.
 * A meta page is the start point for accessing a database snapshot.
 * Pages 0-1 are meta pages. Transaction N writes meta page (N % 2). */
type mdbx_meta struct {
	/* Stamp identifying this as an MDBX file.
	 * It must be set to MDBX_MAGIC with MDBX_DATA_VERSION. */
	magic        uint64
	version      int
	txnID_a      uint64   /* txnid that committed this page, the first of a two-phase-update pair */
	flags        uint16   /* extra DB flags, zero (nothing) for now */
	validataorID uint     //nolint
	extraHeader  uint     //nolint
	geo          mdbx_geo //nolint
	/* first is free space, 2nd is main db */
	/* The size of pages used in this DB */
	dbs          [2]*mdbx_db
	canary       mdbx_canary
	dataSyncSign uint64 //nolint
	txnID_b      uint64 /* txnid that committed this page, the second of a two-phase-update pair */
	pagesRetired uint64 //nolint
	x, y         uint64 //nolint
}

// Header for a single key/data pair within a page
type mdbx_node struct {
	pgno  uint32 // page number in overflow cases
	dsize uint32 // data size
	flag  uint16 // node flags
	ksize uint16 // key size

	data []byte /* key and data are appended here */
	// valid bool
}

type freeList struct {
	txnID uint64
	count int
	pages []uint32
}

// func (f *freeList) print() {
// 	fmt.Printf("Freelist { txnID: %v, count: %d, freePages: %v } \n", f.txnID, f.count, f.pages)
// }

// Reads HeaderSize bytes from provided page, constracts a header
func (h *header) fromBytes(page []byte, isMetaPage bool) {
	pos := 0
	h.txnID = _64(page, pos)
	pos += 8
	h.leaf2keySize = _16(page, pos)
	pos += 2
	h.flag = _16(page, pos)
	pos += 2
	h.lower = _16(page, pos)
	pos += 2
	h.upper = _16(page, pos)
	pos += 2
	h.pageID = _32(page, pos)
	pos += 4

	// if its not a meta page and its not an overflow page
	// get all pointers and nodes
	if !isMetaPage && !isOverflow(h.flag) {

		numNodes := h.lower >> 1 // number of nodes on a page
		for ; numNodes > 0; numNodes -= 1 {
			ptr := _16(page, pos)               // ptr without Header offset
			nodePtr := ptr + uint16(HeaderSize) // with Header offset
			if ptr == 0 {                       // extra safety
				break
			}

			node := new(mdbx_node)
			node.fromBytes(page, int(nodePtr), h.flag, false)

			// h.ptrs = append(h.ptrs, nodePtr)
			h.nodes = append(h.nodes, node)
			pos += 2
		}
	}
}

// func (h *header) print() {
// 	fmt.Printf("Header { txnID: %v, leaf2keySize: %v, flags: %v, lowerFree: %v, upperFree: %v, pageID: %v, ptrs: %v }\n", h.txnID, h.leaf2keySize, h.flag, h.lower, h.upper, h.pageID, h.ptrs)
// }

// func (h *header) print_nodes() {
// 	for _, node := range h.nodes {
// 		node.print()
// 	}
// }

func (db *mdbx_db) init(page []byte, pos *int) {
	db.flags = _16(page, *pos)
	*pos += 2
	db.depth = _16(page, *pos)
	*pos += 2
	db.xsize = _32(page, *pos)
	*pos += 4
	db.rootID = _32(page, *pos)
	*pos += 4
	db.branches = _32(page, *pos)
	*pos += 4
	db.leafs = _32(page, *pos)
	*pos += 4
	db.overflows = _32(page, *pos)
	*pos += 4
	*pos += 8 // seq
	db.entries = _64(page, *pos)
	*pos += 8
	db.txnID = _64(page, *pos)
	*pos += 8
}

// func (db *mdbx_db) toString() string {
// 	return fmt.Sprintf("{ flags: %v, depth: %v, xsize: %v, rootID: %v, branches: %v, leafs: %v, overflows: %v, entries: %v, txnID: %v }", db.flags, db.depth, db.xsize, db.rootID, db.branches, db.leafs, db.overflows, db.entries, db.txnID)
// }

// func (db *mdbx_db) print() {
// 	fmt.Printf("MDBX_DB { flags: %v, depth: %v, xsize: %v, rootID: %v, branches: %v, leafs: %v, overflows: %v, entries: %v, txnID: %v }\n", db.flags, db.depth, db.xsize, db.rootID, db.branches, db.leafs, db.overflows, db.entries, db.txnID)
// }

func (m *mdbx_meta) readMeta(page []byte) error {
	pos := HeaderSize

	magicAndVersion := _64(page, pos)
	pos += 8

	m.magic = magicAndVersion >> 8
	m.version = int(magicAndVersion & 0x000000000000000F)

	m.txnID_a = _64(page, pos)
	pos += 8

	m.flags = _16(page, pos)
	pos += 2

	pos += 1                 // validator ID
	pos += 1                 // extra header
	pos += (2 * 2) + (4 * 4) // geo

	m.dbs[0] = new(mdbx_db)
	m.dbs[0].init(page, &pos)
	m.dbs[1] = new(mdbx_db)
	m.dbs[1].init(page, &pos)

	m.canary = mdbx_canary{0, 0, 0, 0}
	pos += 4 * 8

	pos += 8 // dataSyncSign

	m.txnID_b = _64(page, pos)
	pos += 8

	pos += 3 * 8 // pagesRetired, x, y

	return nil
}

// func (m *mdbx_meta) print() {
// 	fmt.Printf("Meta { magic: %v, version %v, txnID_a: %v, flags: %v, freeDB: %v, mainDB: %v, txnID_b: %v }\n", m.magic, m.version, m.txnID_a, m.flags, m.dbs[0].toString(), m.dbs[1].toString(), m.txnID_b)
// }

func (n *mdbx_node) fromBytes(page []byte, offset int, pageFlag uint16, isSubPage bool) {

	n.dsize = _32(page, offset)
	n.flag = _16(page, offset+4)
	n.ksize = _16(page, offset+6)

	if isBranch(pageFlag) {
		n.data = page[offset+8 : offset+8+int(n.ksize)]
		n.pgno = n.dsize
		return
	}

	// if it's a sub-page node
	if isSubPage {
		// here data is the value's bytes
		n.data = page[offset+8 : offset+8+int(n.ksize)]
		return
	}

	if isBigData(n.flag) {
		n.data = page[offset+8 : offset+8+int(n.ksize)+4]
		n.pgno = _32(n.data, int(n.ksize))
		return
	}

	n.data = page[offset+8 : offset+8+int(n.ksize)+int(n.dsize)]
}

// func (n *mdbx_node) print() {
// 	if isBigData(n.flag) {
// 		fmt.Printf("Node { pgno: %v, dsize: %v, flag: %v, ksize: %v, data: %v }\n", n.pgno, n.dsize, n.flag, n.ksize, n.data)
// 	} else {
// 		fmt.Printf("Node { dsize: %v, flag: %v, ksize: %v, data: %v }\n", n.dsize, n.flag, n.ksize, n.data)
// 	}
// }

func (n *mdbx_node) getFreeList() freeList {
	pos := 0
	txnID := _64(n.data, pos)
	pos += 8

	count := _32(n.data, pos)
	pos += 4

	var pages []uint32
	for ; pos < len(n.data); pos += 4 {
		pages = append(pages, _32(n.data, pos))
	}

	return freeList{txnID, int(count), pages}
}

func (n *mdbx_node) getSubDB() (key string, subDB *mdbx_db) {

	pos := 0
	key = string(n.data[pos : pos+int(n.ksize)])
	pos += int(n.ksize)

	subDB = new(mdbx_db)
	subDB.init(n.data, &pos)

	return
}

func (n *mdbx_node) getKV() (key string, value string) {
	// _assert(n.dsize > 0, "n.dsize > 0")
	// _assert(n.ksize > 0, "n.ksize > 0")

	key = string(n.data[:n.ksize])
	value = string(n.data[n.ksize:])

	return
}

// func (n *mdbx_node) toString() string {
// 	if isBigData(n.flag) {
// 		return fmt.Sprintf("Node { pgno: %v, dsize: %v, flag: %v, ksize: %v, data: %v }\n", n.pgno, n.dsize, n.flag, n.ksize, n.data)
// 	} else {
// 		return fmt.Sprintf("Node { dsize: %v, flag: %v, ksize: %v, data: %v }\n", n.dsize, n.flag, n.ksize, n.data)
// 	}
// }

/* ----------------------- DB generator functions ----------------------- */

// Generates an empty database and returns the file name
func nothing(kv kv.RwDB, _ kv.RwTx) (bool, error) {
	return true, nil
}

// Generates a database with single table and two key-value pair in "t" DBI, and returns the file name
func generate2(tx kv.RwTx, entries int) error {
	c, err := tx.RwCursor("t")
	if err != nil {
		return err
	}
	defer c.Close()
	for i := 0; i < entries; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Append([]byte(k), []byte("very_short_value")); err != nil {
			return err
		}
	}
	return nil
}

// Generates a database with 100 (maximum) of DBIs to produce branches in MAIN_DBI
func generate3(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	for i := 0; i < 61; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.CreateBucket(k); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Generates a database with one table, containing 1 short and 1 long (more than one page) values
func generate4(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	c, err := tx.RwCursor("t")
	if err != nil {
		return false, err
	}
	defer c.Close()
	if err := c.Append([]byte("k1"), []byte("very_short_value")); err != nil {
		return false, err
	}
	if err := c.Append([]byte("k2"), []byte(strings.Repeat("long_value_", 1000))); err != nil {
		return false, err
	}
	return true, nil
}

// Generates a database with one table, containing some DupSort values
func generate5(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	c, err := tx.RwCursorDupSort("t")
	if err != nil {
		return false, err
	}
	defer c.Close()
	if err := c.AppendDup([]byte("key1"), []byte("value11")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key1"), []byte("value12")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key1"), []byte("value13")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key2"), []byte("value21")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key2"), []byte("value22")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key3"), []byte("value31")); err != nil {
		return false, err
	}
	return true, nil
}

// Generate a database with one table, containing lots of dupsort values
func generate6(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	c, err := tx.RwCursorDupSort("t")
	if err != nil {
		return false, err
	}
	defer c.Close()
	for i := 0; i < 1000; i++ {
		v := fmt.Sprintf("dupval_%05d", i)
		if err := c.AppendDup([]byte("key1"), []byte(v)); err != nil {
			return false, err
		}
	}
	if err := c.AppendDup([]byte("key2"), []byte("value21")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key2"), []byte("value22")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key3"), []byte("value31")); err != nil {
		return false, err
	}
	return true, nil
}

func dropT(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	if err := tx.ClearBucket("t"); err != nil {
		return false, err
	}
	return true, nil
}

func generate7(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	c1, err := tx.RwCursor("t1")
	if err != nil {
		return false, err
	}
	defer c1.Close()
	c2, err := tx.RwCursor("t2")
	if err != nil {
		return false, err
	}
	defer c2.Close()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c1.Append([]byte(k), []byte("very_short_value")); err != nil {
			return false, err
		}
		if err := c2.Append([]byte(k), []byte("very_short_value")); err != nil {
			return false, err
		}
	}
	return true, nil
}

func dropT1(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	if err := tx.ClearBucket("t1"); err != nil {
		return false, err
	}
	return true, nil
}

func dropT2(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	if err := tx.ClearBucket("t2"); err != nil {
		return false, err
	}
	return true, nil
}

// Generates a database with 100 (maximum) of DBIs to produce branches in MAIN_DBI
func generate8(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.CreateBucket(k); err != nil {
			return false, err
		}
	}
	return false, nil
}

func generate9(tx kv.RwTx, entries int) error {
	var cs []kv.RwCursor
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		c, err := tx.RwCursor(k)
		if err != nil {
			return err
		}
		defer c.Close()
		cs = append(cs, c)
	}
	for i := 0; i < entries; i++ {
		k := fmt.Sprintf("%08d", i)
		for _, c := range cs {
			if err := c.Append([]byte(k), []byte("very_short_value")); err != nil {
				return err
			}
		}
	}
	return nil
}

func dropAll(_ kv.RwDB, tx kv.RwTx) (bool, error) {
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.DropBucket(k); err != nil {
			return false, err
		}
	}
	return true, nil
}

// dropGradually drops every other table in its own transaction
func dropGradually(db kv.RwDB, tx kv.RwTx) (bool, error) {
	tx.Rollback()
	for i := 0; i < 100; i += 2 {
		k := fmt.Sprintf("table_%05d", i)
		if err := db.Update(context.Background(), func(tx1 kv.RwTx) error {
			return tx1.DropBucket(k)
		}); err != nil {
			return false, err
		}
	}
	return true, nil
}

func change1(tx kv.RwTx) (bool, error) {
	c, err := tx.RwCursor("t")
	if err != nil {
		return false, err
	}
	defer c.Close()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Put([]byte(k), []byte("another_short_value_1")); err != nil {
			return false, err
		}
	}
	return true, nil
}

func change2(tx kv.RwTx) (bool, error) {
	c, err := tx.RwCursor("t")
	if err != nil {
		return false, err
	}
	defer c.Close()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Put([]byte(k), []byte("another_short_value_2")); err != nil {
			return false, err
		}
	}
	return true, nil
}

func change3(tx kv.RwTx) (bool, error) {
	c, err := tx.RwCursor("t")
	if err != nil {
		return false, err
	}
	defer c.Close()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Put([]byte(k), []byte("another_short_value_3")); err != nil {
			return false, err
		}
	}
	return true, nil
}

func launchReader(kv kv.RwDB, tx kv.Tx, expectVal string, startCh chan struct{}, errorCh chan error) (bool, error) {
	tx.Rollback()
	tx1, err1 := kv.BeginRo(context.Background())
	if err1 != nil {
		return false, err1
	}
	// Wait for the signal to start reading
	go func() {
		defer debug.LogPanic()
		defer tx1.Rollback()
		<-startCh
		c, err := tx1.Cursor("t")
		if err != nil {
			errorCh <- err
			return
		}
		defer c.Close()
		for i := 0; i < 1000; i++ {
			k := fmt.Sprintf("%05d", i)
			var v []byte
			if _, v, err = c.SeekExact([]byte(k)); err != nil {
				errorCh <- err
				return
			}
			if !bytes.Equal(v, []byte(expectVal)) {
				errorCh <- fmt.Errorf("expected value: %s, got %s", expectVal, v)
				return
			}
		}
		errorCh <- nil
	}()
	return false, nil
}

func startReader(tx kv.Tx, startCh chan struct{}) (bool, error) {
	tx.Rollback()
	startCh <- struct{}{}
	return false, nil
}

func checkReader(tx kv.Tx, errorCh chan error) (bool, error) {
	tx.Rollback()
	if err := <-errorCh; err != nil {
		return false, err
	}
	return false, nil
}

func defragSteps(filename string, bucketsCfg kv.TableCfg, generateFs ...func(kv.RwDB, kv.RwTx) (bool, error)) error {
	dir, err := os.MkdirTemp(".", "db-vis")
	if err != nil {
		return fmt.Errorf("creating temp dir for db visualisation: %w", err)
	}
	defer os.RemoveAll(dir)
	var db kv.RwDB
	db, err = kv2.NewMDBX(logger).Path(dir).WithTableCfg(func(kv.TableCfg) kv.TableCfg {
		return bucketsCfg
	}).Open()
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()
	for gi, generateF := range generateFs {
		var display bool
		if err = db.Update(context.Background(), func(tx kv.RwTx) error {
			var err1 error
			//nolint:scopelint
			display, err1 = generateF(db, tx)
			return err1
		}); err != nil {
			return fmt.Errorf("generating data in temp db - function %d, file: %s: %w", gi, filename, err)
		}
		if display {
			var f *os.File
			if f, err = os.Create(fmt.Sprintf("%s_%d.dot", filename, gi)); err != nil {
				return fmt.Errorf("open %s: %w", filename, err)
			}
			defer f.Close()
			w := bufio.NewWriter(f)
			defer w.Flush()
			if err = TextInfo(dir, w); err != nil {
				return fmt.Errorf("textInfo for %s_%d: %w", filename, gi, err)
			}
			if err = w.Flush(); err != nil {
				return fmt.Errorf("flush %s_%d: %w", filename, gi, err)
			}
			if err = f.Close(); err != nil {
				return fmt.Errorf("close %s_%d: %w", filename, gi, err)
			}
			// nolint:gosec
			cmd := exec.Command("dot", "-Tpng:gd", "-o", fmt.Sprintf("%s_%d.png", filename, gi), fmt.Sprintf("%s_%d.dot", filename, gi))
			var output []byte
			if output, err = cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("dot generation error: %w, output: %s", err, output)
			}
		}
	}
	return nil
}

func Defrag() error {
	emptyBucketCfg := make(kv.TableCfg)
	fmt.Println("------------------- 1 -------------------")
	if err := defragSteps("vis1", emptyBucketCfg, nothing); err != nil {
		return err
	}

	oneBucketCfg := make(kv.TableCfg)
	oneBucketCfg["t"] = kv.TableCfgItem{}
	fmt.Println("------------------- 2 -------------------")
	if err := defragSteps("vis2", oneBucketCfg, func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return true, generate2(tx, 2) }); err != nil {
		return err
	}
	fmt.Println("------------------- 3 -------------------")
	if err := defragSteps("vis3", emptyBucketCfg, generate3); err != nil {
		return err
	}
	fmt.Println("------------------- 4 -------------------")
	if err := defragSteps("vis4", oneBucketCfg, func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return true, generate2(tx, 200) }); err != nil {
		return err
	}
	fmt.Println("------------------- 5 -------------------")
	if err := defragSteps("vis5", oneBucketCfg, generate4); err != nil {
		return err
	}
	oneDupSortCfg := make(kv.TableCfg)
	oneDupSortCfg["t"] = kv.TableCfgItem{Flags: kv.DupSort}
	fmt.Println("------------------- 6 -------------------")
	if err := defragSteps("vis6", oneDupSortCfg, generate5); err != nil {
		return err
	}
	fmt.Println("------------------- 7 -------------------")
	if err := defragSteps("vis7", oneDupSortCfg, generate6); err != nil {
		return err
	}
	fmt.Println("------------------- 8 -------------------")
	if err := defragSteps("vis8", oneDupSortCfg, func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return true, generate2(tx, 1000) }, dropT); err != nil {
		return err
	}

	twoBucketCfg := make(kv.TableCfg)
	twoBucketCfg["t1"] = kv.TableCfgItem{}
	twoBucketCfg["t2"] = kv.TableCfgItem{}
	fmt.Println("------------------- 9 -------------------")
	if err := defragSteps("vis9", twoBucketCfg, generate7); err != nil {
		return err
	}
	fmt.Println("------------------- 10 -------------------")
	if err := defragSteps("vis10", twoBucketCfg, generate7, dropT1); err != nil {
		return err
	}
	fmt.Println("------------------- 11 -------------------")
	if err := defragSteps("vis11", twoBucketCfg, generate7, dropT1, dropT2); err != nil {
		return err
	}
	manyBucketCfg := make(kv.TableCfg)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		manyBucketCfg[k] = kv.TableCfgItem{IsDeprecated: true}
	}
	fmt.Println("------------------- 12 -------------------")
	if err := defragSteps("vis12", manyBucketCfg, generate8, func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return true, generate9(tx, 1000) }, dropGradually); err != nil {
		return err
	}
	fmt.Println("------------------- 13 -------------------")
	if err := defragSteps("vis13", manyBucketCfg, generate8, func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return false, generate9(tx, 10000) }, dropAll); err != nil {
		return err
	}
	fmt.Println("------------------- 14 -------------------")
	if err := defragSteps("vis14", manyBucketCfg, generate8, func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return false, generate9(tx, 300000) }, dropGradually); err != nil {
		return err
	}
	fmt.Println("------------------- 15 -------------------")
	if err := defragSteps("vis15", oneBucketCfg,
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return true, generate2(tx, 1000) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change1(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change2(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change3(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change2(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change3(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change2(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change3(tx) },
	); err != nil {
		return err
	}

	readerStartCh := make(chan struct{})
	readerErrorCh := make(chan error)

	fmt.Println("------------------- 16 -------------------")
	if err := defragSteps("vis16", oneBucketCfg,
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return true, generate2(tx, 1000) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change1(tx) },
		func(kv kv.RwDB, tx kv.RwTx) (bool, error) {
			return launchReader(kv, tx, "another_short_value_1", readerStartCh, readerErrorCh)
		},
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change2(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change3(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change2(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change3(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change2(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) { return change3(tx) },
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) {
			return startReader(tx, readerStartCh)
		},
		func(_ kv.RwDB, tx kv.RwTx) (bool, error) {
			return checkReader(tx, readerErrorCh)
		},
	); err != nil {
		return err
	}
	return nil
}

func TextInfo(chaindata string, visStream io.Writer) error {
	log.Info("Text Info", "db", chaindata)
	fmt.Fprint(visStream, "digraph lmdb {\nrankdir=LR\n")
	datafile := filepath.Join(chaindata, MdbxDataFile)

	f, err := os.Open(datafile)
	if err != nil {
		return fmt.Errorf("opening %v: %w", MdbxDataFile, err)
	}
	defer f.Close()
	var meta [PageSize]byte
	// Read meta page 0
	if _, err = f.ReadAt(meta[:], 0*PageSize); err != nil {
		return fmt.Errorf("reading meta page 0: %w", err)
	}

	header1 := new(header)
	header1.fromBytes(meta[:], true)
	// header1.print()

	if header1.pageID != 0 {
		return fmt.Errorf("meta page 0 has wrong page number: %d != %d", header1.pageID, 0)
	}

	meta1 := new(mdbx_meta)
	err = meta1.readMeta(meta[:])
	// meta1.print()

	if err != nil {
		return fmt.Errorf("reading meta page 0: %w", err)
	}

	// Read meta page 1
	if _, err = f.ReadAt(meta[:], 1*PageSize); err != nil {
		return fmt.Errorf("reading meta page 1: %w", err)
	}

	header2 := new(header)
	header2.fromBytes(meta[:], true)
	// header2.print()

	if header2.pageID != 1 {
		return fmt.Errorf("meta page 1 has wrong page number: %d != %d", header2.pageID, 0)
	}

	meta2 := new(mdbx_meta)
	err = meta2.readMeta(meta[:])
	// meta2.print()

	if err != nil {
		return fmt.Errorf("reading meta page 1: %w", err)
	}

	var freeRoot, mainRoot uint32
	var freeDepth, mainDepth uint16

	if meta1.txnID_b > meta2.txnID_b {
		freeRoot = meta1.dbs[0].rootID
		freeDepth = meta1.dbs[0].depth
		mainRoot = meta1.dbs[1].rootID
		mainDepth = meta1.dbs[1].depth
	} else {
		freeRoot = meta2.dbs[0].rootID
		freeDepth = meta2.dbs[0].depth
		mainRoot = meta2.dbs[1].rootID
		mainDepth = meta2.dbs[1].depth
	}

	log.Info("FREE_DBI", "root page ID", freeRoot, "depth", freeDepth)
	log.Info("MAIN_DBI", "root page ID", mainRoot, "depth", mainDepth)

	level := 0
	blockID := 0
	fmt.Fprintf(visStream, "block_%d [shape=Mrecord label=\"<free_root>FREE ROOT|<p_%d>MAIN ROOT\"]\n", blockID, mainRoot)

	if mainRoot == 0xffffffff {
		log.Info("empty mainlist")
	} else {
		readPages(f, visStream, mainRoot, &blockID, 0, &level)
	}

	if freeRoot == 0xffffffff {
		log.Info("empty freelist")
	} else {
		freeDBPages(f, visStream, freeRoot)
	}

	if freeRoot != 0xffffffff {
		fmt.Fprint(visStream, "block_0:free_root -> free_pages\n")
	}

	fmt.Fprint(visStream, "}\n")
	return nil
}

// checks the node flag against conditions and takes appropriate measures
func _conditions(f io.ReaderAt, visStream io.Writer, node *mdbx_node, _header *header, blockID *int, parentBlock int, thisLevel *int, out *string) {

	// _isLeaf := isLeaf(hFlag)
	_isBranch := isBranch(_header.flag)
	// _isOverflow := isOverflow(hFlag)

	_isBigData := isBigData(node.flag)
	_isSubDB := isSubDB(node.flag)
	_isDupData := isDupData(node.flag)

	if _isBranch {

		readPages(f, visStream, node.pgno, blockID, parentBlock, thisLevel)
		*out += fmt.Sprintf("<p_%d>%d", node.pgno, node.pgno)

		return
	}

	if _isSubDB {
		key, subDB := node.getSubDB()

		if subDB.rootID != 0xffffffff {
			if _isDupData {
				*out += fmt.Sprintf("<p_%d>%s:SUBDB(%d)", subDB.rootID, key, subDB.rootID)
			} else {
				*out += fmt.Sprintf("<p_%d>%s=%d", subDB.rootID, key, subDB.rootID)
			}
		} else {
			*out += key
		}

		readPages(f, visStream, subDB.rootID, blockID, parentBlock, thisLevel)
		return
	}

	if _isBigData {

		// how many pages we need to read?
		pgCount := int(math.Round(float64(node.dsize+uint32(HeaderSize)) / float64(PageSize)))
		key := string(node.data[:node.ksize])

		*out += fmt.Sprintf("<%s>%s:OVERFLOW(%d)", key, key, pgCount)

		fmt.Fprintf(visStream, "ovf_%s_%d [shape=record style=filled fillcolor=\"%s\" label=\"Overflow %d pages\"]", key, pgCount, colors["yellow"], pgCount)

		fmt.Fprintf(visStream, "block_%d:%s -> ovf_%s_%d\n", *blockID, key, key, pgCount)

		return
	}

	// data has duplicates, means that node data contains sub-page
	if _isDupData {

		subHeader := new(header)
		subHeader.fromBytes(node.data[node.ksize:], false)

		key := string(node.data[:node.ksize])
		*out += fmt.Sprintf("{%s:", key)

		for _, subNode := range subHeader.nodes {
			val := string(subNode.data[:subNode.ksize])
			*out += fmt.Sprintf("|%s", val)
		}

		*out += "}"
		return
	}

	// here we are reached a leaf node

	k, v := node.getKV()

	if !isSubDB(node.flag) {
		*out += fmt.Sprintf("<%s>%s:%s", k, k, v)
	}
}

// Recursively goes over pages if pages are BRANCH or SUBDB
func readPages(f io.ReaderAt, visStream io.Writer, pgno uint32, blockID *int, parentBlock int, level *int) error {
	var page [PageSize]byte
	if _, err := f.ReadAt(page[:], int64(pgno*PageSize)); err != nil {
		return fmt.Errorf("reading page: %v, error: %w", pgno, err)
	}

	_header := new(header)
	_header.fromBytes(page[:], false)
	// _header.print()

	_isLeaf := isLeaf(_header.flag)
	_isBranch := isBranch(_header.flag)
	// _isOverflow := isOverflow(_header.flag)

	thisLevel := *level + 1
	*blockID++
	pBlock := *blockID

	fillcolor := ""
	if _isBranch {
		fillcolor = colors["purple"]
	} else {
		if _isLeaf && thisLevel != 1 {
			fillcolor = colors["green"]
		} else if _isLeaf && thisLevel == 1 {
			fillcolor = colors["orange"]
		} else {
			fillcolor = colors["yellow"]
		}
	}

	out := fmt.Sprintf("block_%d [shape=record style=filled fillcolor=\"%s\" label=\"", *blockID, fillcolor)

	l := len(_header.nodes)
	if l > 0 {
		fmt.Fprintf(visStream, "block_%d:p_%d -> block_%d\n", parentBlock, pgno, *blockID)
	}

	if _isLeaf || _isBranch {

		if l > 5 {
			for i, n := range []int{0, 1, 2, l - 2, l - 1} {
				if n != 2 {
					node := _header.nodes[n]
					// node.print()

					_conditions(f, visStream, node, _header, blockID, pBlock, &thisLevel, &out)

					if i < 4 {
						out += "|"
					}

				} else {
					out += fmt.Sprintf("...%d more records here...", l-4)
					out += "|"
				}
			}

		} else {
			for i, node := range _header.nodes {
				_conditions(f, visStream, node, _header, blockID, pBlock, &thisLevel, &out)

				if i < l-1 {
					out += "|"
				}

			}
		}
	}

	out += "\"]\n"
	if l > 0 {
		fmt.Fprint(visStream, out)
	}

	return nil
}

func freeDBPages(f io.ReaderAt, visStream io.Writer, freeRoot uint32) error {
	var page [PageSize]byte
	if _, err := f.ReadAt(page[:], int64(freeRoot*PageSize)); err != nil {
		return fmt.Errorf("reading page: %v, error: %w", freeRoot, err)
	}

	_header := new(header)
	_header.fromBytes(page[:], false)
	// _header.print()

	out := "free_pages [shape=record style=filled fillcolor=\"#AED6F1\" label=\""

	l := len(_header.nodes)

	if l > 0 {
		fmt.Fprint(visStream, out)
	}

	out = ""

	if isLeaf(_header.flag) || isBranch(_header.flag) {

		for i, node := range _header.nodes {
			if !isBigData(node.flag) {
				list := node.getFreeList()
				// list.print()
				out += fmt.Sprintf("txid(%v)=", list.txnID)
				out += pagesToString(list.pages)
			} else {
				txnID := _64(node.data, 0)

				overflowPages := int(math.Ceil(float64(node.dsize+uint32(HeaderSize)) / float64(PageSize)))

				out += fmt.Sprintf("txid(%v)", txnID)
				out += fmt.Sprintf("(ON %d OVERFLOW PAGES)=", overflowPages)
				for i := 0; i < overflowPages; i++ {
					out += fmt.Sprintf("%d", int(node.pgno)+i)
					if i+1 < overflowPages {
						out += ", "
					}
				}
			}

			if i < l-1 {
				out += "|"
			}
		}
	}

	out += "\"]\n"
	fmt.Fprint(visStream, out)

	return nil
}

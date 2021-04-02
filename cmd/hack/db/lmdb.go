package db

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const PageSize = 4096
const MdbMagic uint32 = 0xBEEFC0DE
const MdbDataVersion uint32 = 1

const BranchPageFlag uint16 = 1
const LeafPageFlag uint16 = 2
const OverflowPageFlag uint16 = 4

//const DupSortPagePlag uint16 = 8

const BigDataNodeFlag uint16 = 1
const SubDbNodeFlag uint16 = 2
const DupDataNodeFlag uint16 = 4

const HeaderSize int = 16

// Generates an empty database and returns the file name
func nothing(kv ethdb.RwKV, _ ethdb.RwTx) (bool, error) {
	return true, nil
}

// Generates a database with single table and two key-value pair in "t" DBI, and returns the file name
func generate2(tx ethdb.RwTx, entries int) error {
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
func generate3(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
	for i := 0; i < 61; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.(ethdb.BucketMigrator).CreateBucket(k); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Generates a database with one table, containing 1 short and 1 long (more than one page) values
func generate4(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
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
func generate5(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
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
func generate6(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
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

func dropT(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
	if err := tx.(ethdb.BucketMigrator).ClearBucket("t"); err != nil {
		return false, err
	}
	return true, nil
}

func generate7(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
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

func dropT1(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
	if err := tx.(ethdb.BucketMigrator).ClearBucket("t1"); err != nil {
		return false, err
	}
	return true, nil
}

func dropT2(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
	if err := tx.(ethdb.BucketMigrator).ClearBucket("t2"); err != nil {
		return false, err
	}
	return true, nil
}

// Generates a database with 100 (maximum) of DBIs to produce branches in MAIN_DBI
func generate8(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.(ethdb.BucketMigrator).CreateBucket(k); err != nil {
			return false, err
		}
	}
	return false, nil
}

func generate9(tx ethdb.RwTx, entries int) error {
	var cs []ethdb.RwCursor
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

func dropAll(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.(ethdb.BucketMigrator).DropBucket(k); err != nil {
			return false, err
		}
	}
	return true, nil
}

// dropGradually drops every other table in its own transaction
func dropGradually(kv ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
	tx.Rollback()
	for i := 0; i < 100; i += 2 {
		k := fmt.Sprintf("table_%05d", i)
		if err := kv.Update(context.Background(), func(tx1 ethdb.RwTx) error {
			return tx1.(ethdb.BucketMigrator).DropBucket(k)
		}); err != nil {
			return false, err
		}
	}
	return true, nil
}

func change1(tx ethdb.RwTx) (bool, error) {
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

func change2(tx ethdb.RwTx) (bool, error) {
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

func change3(tx ethdb.RwTx) (bool, error) {
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

func launchReader(kv ethdb.RwKV, tx ethdb.Tx, expectVal string, startCh chan struct{}, errorCh chan error) (bool, error) {
	tx.Rollback()
	tx1, err1 := kv.Begin(context.Background())
	if err1 != nil {
		return false, err1
	}
	// Wait for the signal to start reading
	go func() {
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

func startReader(tx ethdb.Tx, startCh chan struct{}) (bool, error) {
	tx.Rollback()
	startCh <- struct{}{}
	return false, nil
}

func checkReader(tx ethdb.Tx, errorCh chan error) (bool, error) {
	tx.Rollback()
	if err := <-errorCh; err != nil {
		return false, err
	}
	return false, nil
}

func defragSteps(filename string, bucketsCfg dbutils.BucketsCfg, generateFs ...func(ethdb.RwKV, ethdb.RwTx) (bool, error)) error {
	dir, err := ioutil.TempDir(".", "lmdb-vis")
	if err != nil {
		return fmt.Errorf("creating temp dir for lmdb visualisation: %w", err)
	}
	defer os.RemoveAll(dir)
	var kv ethdb.RwKV
	kv, err = ethdb.NewLMDB().Path(dir).WithBucketsConfig(func(dbutils.BucketsCfg) dbutils.BucketsCfg {
		return bucketsCfg
	}).Open()
	if err != nil {
		return fmt.Errorf("opening LMDB database: %w", err)
	}
	defer kv.Close()
	for gi, generateF := range generateFs {
		var display bool
		if err = kv.Update(context.Background(), func(tx ethdb.RwTx) error {
			var err1 error
			//nolint:scopelint
			display, err1 = generateF(kv, tx)
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
			//nolint:gosec
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
	emptyBucketCfg := make(dbutils.BucketsCfg)
	if err := defragSteps("vis1", emptyBucketCfg, nothing); err != nil {
		return err
	}
	oneBucketCfg := make(dbutils.BucketsCfg)
	oneBucketCfg["t"] = dbutils.BucketConfigItem{}
	if err := defragSteps("vis2", oneBucketCfg, func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return true, generate2(tx, 2) }); err != nil {
		return err
	}
	if err := defragSteps("vis3", emptyBucketCfg, generate3); err != nil {
		return err
	}
	if err := defragSteps("vis4", oneBucketCfg, func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return true, generate2(tx, 200) }); err != nil {
		return err
	}
	if err := defragSteps("vis5", oneBucketCfg, generate4); err != nil {
		return err
	}
	oneDupSortCfg := make(dbutils.BucketsCfg)
	oneDupSortCfg["t"] = dbutils.BucketConfigItem{Flags: dbutils.DupSort}
	if err := defragSteps("vis6", oneDupSortCfg, generate5); err != nil {
		return err
	}
	if err := defragSteps("vis7", oneDupSortCfg, generate6); err != nil {
		return err
	}
	if err := defragSteps("vis8", oneDupSortCfg, func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return true, generate2(tx, 1000) }, dropT); err != nil {
		return err
	}
	twoBucketCfg := make(dbutils.BucketsCfg)
	twoBucketCfg["t1"] = dbutils.BucketConfigItem{}
	twoBucketCfg["t2"] = dbutils.BucketConfigItem{}
	if err := defragSteps("vis9", twoBucketCfg, generate7); err != nil {
		return err
	}
	if err := defragSteps("vis10", twoBucketCfg, generate7, dropT1); err != nil {
		return err
	}
	if err := defragSteps("vis11", twoBucketCfg, generate7, dropT1, dropT2); err != nil {
		return err
	}
	manyBucketCfg := make(dbutils.BucketsCfg)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		manyBucketCfg[k] = dbutils.BucketConfigItem{IsDeprecated: true}
	}
	if err := defragSteps("vis12", manyBucketCfg, generate8, func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return true, generate9(tx, 1000) }, dropGradually); err != nil {
		return err
	}
	if err := defragSteps("vis13", manyBucketCfg, generate8, func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return false, generate9(tx, 10000) }, dropAll); err != nil {
		return err
	}
	if err := defragSteps("vis14", manyBucketCfg, generate8, func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return false, generate9(tx, 300000) }, dropGradually); err != nil {
		return err
	}
	if err := defragSteps("vis15", oneBucketCfg,
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return true, generate2(tx, 1000) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change1(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change2(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change3(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change2(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change3(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change2(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change3(tx) },
	); err != nil {
		return err
	}
	readerStartCh := make(chan struct{})
	readerErrorCh := make(chan error)
	if err := defragSteps("vis16", oneBucketCfg,
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return true, generate2(tx, 1000) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change1(tx) },
		func(kv ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
			return launchReader(kv, tx, "another_short_value_1", readerStartCh, readerErrorCh)
		},
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change2(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change3(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change2(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change3(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change2(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) { return change3(tx) },
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
			return startReader(tx, readerStartCh)
		},
		func(_ ethdb.RwKV, tx ethdb.RwTx) (bool, error) {
			return checkReader(tx, readerErrorCh)
		},
	); err != nil {
		return err
	}
	return nil
}

func TextInfo(chaindata string, visStream io.Writer) error {
	log.Info("Text Info", "db", chaindata)
	fmt.Fprintf(visStream, "digraph lmdb {\nrankdir=LR\n")
	datafile := path.Join(chaindata, "data.mdb")
	f, err := os.Open(datafile)
	if err != nil {
		return fmt.Errorf("opening data.mdb: %v", err)
	}
	defer f.Close()
	var meta [PageSize]byte
	// Read meta page 0
	if _, err = f.ReadAt(meta[:], 0*PageSize); err != nil {
		return fmt.Errorf("reading meta page 0: %v", err)
	}
	pos, pageID, _, _ := readPageHeader(meta[:], 0)
	if pageID != 0 {
		return fmt.Errorf("meta page 0 has wrong page ID: %d != %d", pageID, 0)
	}
	var freeRoot0, mainRoot0, txnID0 uint64
	var freeDepth0, mainDepth0 uint16
	freeRoot0, freeDepth0, mainRoot0, mainDepth0, txnID0, err = readMetaPage(meta[:], pos)
	if err != nil {
		return fmt.Errorf("reading meta page 0: %v", err)
	}

	// Read meta page 0
	if _, err = f.ReadAt(meta[:], 1*PageSize); err != nil {
		return fmt.Errorf("reading meta page 1: %v", err)
	}
	pos, pageID, _, _ = readPageHeader(meta[:], 0)
	if pageID != 1 {
		return fmt.Errorf("meta page 1 has wrong page ID: %d != %d", pageID, 1)
	}
	var freeRoot1, mainRoot1, txnID1 uint64
	var freeDepth1, mainDepth1 uint16
	freeRoot1, freeDepth1, mainRoot1, mainDepth1, txnID1, err = readMetaPage(meta[:], pos)
	if err != nil {
		return fmt.Errorf("reading meta page 1: %v", err)
	}

	var freeRoot, mainRoot uint64
	var freeDepth, mainDepth uint16
	if txnID0 > txnID1 {
		freeRoot = freeRoot0
		freeDepth = freeDepth0
		mainRoot = mainRoot0
		mainDepth = mainDepth0
	} else {
		freeRoot = freeRoot1
		freeDepth = freeDepth1
		mainRoot = mainRoot1
		mainDepth = mainDepth1
	}
	log.Info("FREE_DBI", "root page ID", freeRoot, "depth", freeDepth)
	log.Info("MAIN_DBI", "root page ID", mainRoot, "depth", mainDepth)

	fmt.Fprintf(visStream, "meta [shape=Mrecord label=\"<free_root>FREE_DBI")
	if freeRoot != 0xffffffffffffffff {
		fmt.Fprintf(visStream, "=%d", freeRoot)
	}
	fmt.Fprintf(visStream, "|<main_root>MAIN_DBI")
	if mainRoot != 0xffffffffffffffff {
		fmt.Fprintf(visStream, "=%d", mainRoot)
	}
	fmt.Fprintf(visStream, "\"];\n")

	var freelist = make(map[uint64]bool)
	if freeRoot == 0xffffffffffffffff {
		log.Info("empty freelist")
	} else {
		if freelist, err = readFreelist(f, freeRoot, freeDepth, visStream); err != nil {
			return err
		}
		fmt.Fprintf(visStream, "meta:free_root -> p_%d;\n", freeRoot)
	}
	var exclude = make(map[uint64]struct{})
	var maintree = make(map[uint64]struct{})
	if mainRoot == 0xffffffffffffffff {
		log.Info("empty maintree")
	} else {
		if maintree, err = readMainTree(f, mainRoot, mainDepth, visStream, exclude); err != nil {
			return err
		}
		fmt.Fprintf(visStream, "meta:main_root -> p_%d;\n", mainRoot)
	}

	// Now scan all non meta and non-freelist pages
	// First pass - calculate exclusions
	pageID = 2
	_, mainOk := maintree[pageID]
	for _, ok := freelist[pageID]; ok || mainOk; _, ok = freelist[pageID] {
		pageID++
		_, mainOk = maintree[pageID]
	}
	count := 0
	var pagePtrs = make(map[uint64][]uint64)
	for _, err = f.ReadAt(meta[:], int64(pageID)*PageSize); err == nil; _, err = f.ReadAt(meta[:], int64(pageID)*PageSize) {
		var pageNum int
		if pageNum, err = scanPage(meta[:], ioutil.Discard, pagePtrs, exclude); err != nil {
			return err
		}
		count += pageNum
		if count%(1024*256) == 0 {
			log.Info("Scaned", "Gb", count/(1024*256))
		}
		pageID += uint64(pageNum)
		_, mainOk = maintree[pageID]
		for _, ok := freelist[pageID]; ok || mainOk; _, ok = freelist[pageID] {
			pageID++
			_, mainOk = maintree[pageID]
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	// Second pass - generate visualisations
	pageID = 2
	_, mainOk = maintree[pageID]
	for _, ok := freelist[pageID]; ok || mainOk; _, ok = freelist[pageID] {
		pageID++
		_, mainOk = maintree[pageID]
	}
	count = 0
	for _, err = f.ReadAt(meta[:], int64(pageID)*PageSize); err == nil; _, err = f.ReadAt(meta[:], int64(pageID)*PageSize) {
		var pageNum int
		var w io.Writer
		if _, ok := exclude[pageID]; ok {
			w = ioutil.Discard
		} else {
			w = visStream
		}
		if pageNum, err = scanPage(meta[:], w, pagePtrs, exclude); err != nil {
			return err
		}
		count += pageNum
		if count%(1024*256) == 0 {
			log.Info("Scaned", "Gb", count/(1024*256))
		}
		pageID += uint64(pageNum)
		_, mainOk = maintree[pageID]
		for _, ok := freelist[pageID]; ok || mainOk; _, ok = freelist[pageID] {
			pageID++
			_, mainOk = maintree[pageID]
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	log.Info("Scaned", "pages", count, "page after last", pageID)
	fmt.Fprintf(visStream, "}\n")
	return nil
}

func excludeTree(pageID uint64, exclude map[uint64]struct{}, pagePtrs map[uint64][]uint64) {
	exclude[pageID] = struct{}{}
	for _, p := range pagePtrs[pageID] {
		excludeTree(p, exclude, pagePtrs)
	}
}

// scanPage goes over the page, and writes its graphviz representation into the
// provided visStream. It returns 1 for "regular" pages, and number greater than 1
// for pages that are overflow pages, where returned value is number of overflow
// pages in total in this sequence
func scanPage(page []byte, visStream io.Writer, pagePtrs map[uint64][]uint64, exclude map[uint64]struct{}) (int, error) {
	pos, pageID, flags, lowerFree := readPageHeader(page, 0)
	if flags&LeafPageFlag != 0 {
		num := (lowerFree - pos) / 2
		interesting := make(map[int]struct{}) // Lines that have something interesting in them
		// First pass - figuring out interesting lines
		interesting[0] = struct{}{}
		interesting[1] = struct{}{}
		interesting[num-1] = struct{}{}
		interesting[num-2] = struct{}{}
		for i := 0; i < num; i++ {
			nodePtr := int(binary.LittleEndian.Uint16(page[HeaderSize+i*2:]))
			nodeFlags := binary.LittleEndian.Uint16(page[nodePtr+4:])
			keySize := int(binary.LittleEndian.Uint16(page[nodePtr+6:]))
			if nodeFlags&BigDataNodeFlag > 0 {
				interesting[i-1] = struct{}{}
				interesting[i] = struct{}{}
				interesting[i+1] = struct{}{}
				overflowPageID := binary.LittleEndian.Uint64(page[nodePtr+8+keySize:])
				fmt.Fprintf(visStream, "p_%d:n%d -> p_%d;\n", pageID, i, overflowPageID)
				pagePtrs[pageID] = append(pagePtrs[pageID], overflowPageID)
			} else if nodeFlags&SubDbNodeFlag > 0 {
				interesting[i-1] = struct{}{}
				interesting[i] = struct{}{}
				interesting[i+1] = struct{}{}
				pagePtr := binary.LittleEndian.Uint64(page[nodePtr+8+keySize+40:])
				fmt.Fprintf(visStream, "p_%d:n%d -> p_%d;\n", pageID, i, pagePtr)
				pagePtrs[pageID] = append(pagePtrs[pageID], pagePtr)
			} else if nodeFlags&DupDataNodeFlag > 0 {
				interesting[i-1] = struct{}{}
				interesting[i] = struct{}{}
				interesting[i+1] = struct{}{}
			}
		}
		fmt.Fprintf(visStream, "p_%d [shape=record style=filled fillcolor=\"#D5F5E3\" label=\"", pageID)
		for i := 0; i < num; i++ {
			_, i0 := interesting[i]
			_, i1 := interesting[i+1]
			if !i0 && !i1 {
				continue
			}
			if !i0 {
				count := 0
				for j := i; !i0; _, i0 = interesting[j] {
					count++
					j--
				}
				fmt.Fprintf(visStream, "|... %d more records here ...", count)
				continue
			}
			if i > 0 {
				fmt.Fprintf(visStream, "|")
			}
			nodePtr := int(binary.LittleEndian.Uint16(page[HeaderSize+i*2:]))
			dataSize := int(binary.LittleEndian.Uint32(page[nodePtr:]))
			nodeFlags := binary.LittleEndian.Uint16(page[nodePtr+4:])
			keySize := int(binary.LittleEndian.Uint16(page[nodePtr+6:]))
			key := string(page[nodePtr+8 : nodePtr+8+keySize])
			if nodeFlags&BigDataNodeFlag > 0 {
				overflowPageID := binary.LittleEndian.Uint64(page[nodePtr+8+keySize:])
				fmt.Fprintf(visStream, "<n%d>%s:OVERFLOW(%d)", i, key, overflowPageID)
			} else if nodeFlags&SubDbNodeFlag > 0 {
				pagePtr := binary.LittleEndian.Uint64(page[nodePtr+8+keySize+40:])
				fmt.Fprintf(visStream, "<n%d>%s:SUBDB(%d)", i, key, pagePtr)
			} else if nodeFlags&DupDataNodeFlag > 0 {
				val := page[nodePtr+8+keySize : nodePtr+8+keySize+dataSize]
				// val needs to be treated like a leaf page
				pos1, _, _, lowerFree1 := readPageHeader(val, 0)
				num1 := (lowerFree1 - pos1) / 2
				fmt.Fprintf(visStream, "{%s:|", key)
				for j := 0; j < num1; j++ {
					if j > 0 {
						fmt.Fprintf(visStream, "|")
					}
					nodePtr1 := int(binary.LittleEndian.Uint16(val[HeaderSize+j*2:]))
					keySize1 := int(binary.LittleEndian.Uint16(val[nodePtr1+6:]))
					key1 := string(val[nodePtr1+8 : nodePtr1+8+keySize1])
					fmt.Fprintf(visStream, "%s", key1)
				}
				fmt.Fprintf(visStream, "}")
			} else {
				val := string(page[nodePtr+8+keySize : nodePtr+8+keySize+dataSize])
				fmt.Fprintf(visStream, "%s:%s", key, val)
			}
		}
		fmt.Fprintf(visStream, "\"];\n")
	} else if flags&BranchPageFlag != 0 {
		num := (lowerFree - pos) / 2
		fmt.Fprintf(visStream, "p_%d [shape=record style=filled fillcolor=\"#F6DDCC\" label=\"", pageID)
		for i := 0; i < num; i++ {
			nodePtr := int(binary.LittleEndian.Uint16(page[HeaderSize+i*2:]))
			pagePtr := binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
			// Branch page itself is excluded
			if _, ok := exclude[pageID]; ok {
				excludeTree(pagePtr, exclude, pagePtrs)
			}
			if num > 5 && i > 2 && i < num-2 {
				excludeTree(pagePtr, exclude, pagePtrs)
				continue
			}
			if num > 5 && i == 2 {
				fmt.Fprintf(visStream, "|... %d more records here ...", num-4)
				excludeTree(pagePtr, exclude, pagePtrs)
				continue
			}
			if i > 0 {
				fmt.Fprintf(visStream, "|")
			}
			fmt.Fprintf(visStream, "<n%d>%d", i, pagePtr)
		}
		fmt.Fprintf(visStream, "\"];\n")
		for i := 0; i < num; i++ {
			nodePtr := int(binary.LittleEndian.Uint16(page[HeaderSize+i*2:]))
			pagePtr := binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
			pagePtrs[pageID] = append(pagePtrs[pageID], pagePtr)
			if num > 5 && i >= 2 && i < num-2 {
				continue
			}
			fmt.Fprintf(visStream, "p_%d:n%d -> p_%d;\n", pageID, i, pagePtr)
		}
	} else if flags&OverflowPageFlag != 0 {
		_, _, overflowNum := readOverflowPageHeader(page[:], 0)
		fmt.Fprintf(visStream, "p_%d [shape=record style=filled fillcolor=\"#D5F5E3\" label=\"Overflow %d pages\"];", pageID, overflowNum)
		return overflowNum, nil
	} else {
		return 0, fmt.Errorf("unimplemented processing for page type, flags: %d", flags)
	}
	return 1, nil
}

func readMainTree(f io.ReaderAt, mainRoot uint64, mainDepth uint16, visStream io.Writer, exclude map[uint64]struct{}) (map[uint64]struct{}, error) {
	var maintree = make(map[uint64]struct{})
	var mainEntries int
	var pages [8][PageSize]byte // Stack of pages
	var pageIDs [8]uint64
	var numKeys [8]int
	var indices [8]int
	var visbufs [8]strings.Builder
	var top int
	var pos int
	var pageID uint64
	pageIDs[0] = mainRoot
	for top >= 0 {
		branch := top < int(mainDepth)-1
		i := indices[top]
		num := numKeys[top]
		page := &pages[top]
		pageID = pageIDs[top]
		if num == 0 {
			maintree[pageID] = struct{}{}
			if _, err := f.ReadAt(page[:], int64(pageID*PageSize)); err != nil {
				return nil, fmt.Errorf("reading FREE_DBI page: %v", err)
			}
			var flags uint16
			var lowerFree int
			pos, _, flags, lowerFree = readPageHeader(page[:], 0)
			branchFlag := flags&BranchPageFlag > 0
			if branchFlag && !branch {
				return nil, fmt.Errorf("unexpected branch page on level %d of FREE_DBI", top)
			}
			if !branchFlag && branch {
				return nil, fmt.Errorf("expected branch page on level %d of FREE_DBI", top)
			}
			num = (lowerFree - pos) / 2
			i = 0
			numKeys[top] = num
			indices[top] = i
			visbufs[top].Reset()
			if branch {
				fmt.Fprintf(&visbufs[top], "p_%d [shape=record style=filled fillcolor=\"#E8DAEF\" label=\"", pageID)
			} else {
				// Perform the entire loop here
				fmt.Fprintf(&visbufs[top], "p_%d [shape=record style=filled fillcolor=\"#F9E79F\" label=\"", pageID)
				for i = 0; i < num; i++ {
					nodePtr := int(binary.LittleEndian.Uint16(page[HeaderSize+i*2:]))
					mainEntries++
					dataSize := int(binary.LittleEndian.Uint32(page[nodePtr:]))
					flags := binary.LittleEndian.Uint16(page[nodePtr+4:])
					keySize := int(binary.LittleEndian.Uint16(page[nodePtr+6:]))
					if flags&BigDataNodeFlag > 0 {
						return nil, fmt.Errorf("unexpected overflow pages")
					}
					if dataSize != 48 {
						return nil, fmt.Errorf("expected datasize 48, got: %d", dataSize)
					}
					tableName := string(page[nodePtr+8 : nodePtr+8+keySize])
					pagePtr := binary.LittleEndian.Uint64(page[nodePtr+8+keySize+40:])
					if num > 5 && i > 2 && i < num-2 {
						exclude[pagePtr] = struct{}{}
						continue
					}
					if num > 5 && i == 2 {
						fmt.Fprintf(&visbufs[top], "|... %d more records here ...", num-4)
						exclude[pagePtr] = struct{}{}
						continue
					}
					if i > 0 {
						fmt.Fprintf(&visbufs[top], "|")
					}
					if pagePtr != 0xffffffffffffffff {
						fmt.Fprintf(&visbufs[top], "<n%d>%s=%d", i, tableName, pagePtr)
						fmt.Fprintf(visStream, "p_%d:n%d -> p_%d;\n", pageID, i, pagePtr)
					} else {
						fmt.Fprintf(&visbufs[top], "%s", tableName)
					}
				}
				fmt.Fprintf(&visbufs[top], "\"];\n")
				if _, err := visStream.Write([]byte(visbufs[top].String())); err != nil {
					return nil, fmt.Errorf("writing buffer into main stream: %w", err)
				}
				top--
			}
		} else if i < num {
			nodePtr := int(binary.LittleEndian.Uint16(page[HeaderSize+i*2:]))
			indices[top] = i + 1
			if branch {
				if i > 0 {
					fmt.Fprintf(&visbufs[top], "|")
				}
				pagePtr := binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
				fmt.Fprintf(&visbufs[top], "<n%d>%d", i, pagePtr)
				fmt.Fprintf(visStream, "p_%d:n%d -> p_%d;\n", pageID, i, pagePtr)
				top++
				indices[top] = 0
				numKeys[top] = 0
				pageIDs[top] = pagePtr
			} else {
				if i > 0 {
					fmt.Fprintf(&visbufs[top], "|")
				}
				mainEntries++
				dataSize := int(binary.LittleEndian.Uint32(page[nodePtr:]))
				flags := binary.LittleEndian.Uint16(page[nodePtr+4:])
				keySize := int(binary.LittleEndian.Uint16(page[nodePtr+6:]))
				if flags&BigDataNodeFlag > 0 {
					return nil, fmt.Errorf("unexpected overflow pages")
				}
				if dataSize != 48 {
					return nil, fmt.Errorf("expected datasize 48, got: %d", dataSize)
				}
				tableName := string(page[nodePtr+8 : nodePtr+8+keySize])
				pagePtr := binary.LittleEndian.Uint64(page[nodePtr+8+keySize+40:])
				if pagePtr != 0xffffffffffffffff {
					fmt.Fprintf(&visbufs[top], "<n%d>%s=%d", i, tableName, pagePtr)
					fmt.Fprintf(visStream, "p_%d:n%d -> p_%d;\n", pageID, i, pagePtr)
				} else {
					fmt.Fprintf(&visbufs[top], "%s", tableName)
				}
			}
		} else {
			fmt.Fprintf(&visbufs[top], "\"];\n")
			if _, err := visStream.Write([]byte(visbufs[top].String())); err != nil {
				return nil, fmt.Errorf("writing buffer into main stream: %w", err)
			}
			top--
		}
	}
	log.Info("Main tree", "entries", mainEntries)
	return maintree, nil
}

// Returns a map of pageIDs to bool. If value is true, this page is free. If value is false,
// this page is a part of freelist structure itself
func readFreelist(f io.ReaderAt, freeRoot uint64, freeDepth uint16, visStream io.Writer) (map[uint64]bool, error) {
	var freelist = make(map[uint64]bool)
	var freepages int
	var freeEntries int
	var overflows int
	var pages [8][PageSize]byte // Stack of pages
	var pageIDs [8]uint64
	var numKeys [8]int
	var indices [8]int
	var visbufs [8]strings.Builder
	var top int
	var pos int
	var pageID uint64
	var overflow [PageSize]byte
	pageIDs[0] = freeRoot
	for top >= 0 {
		branch := top < int(freeDepth)-1
		i := indices[top]
		num := numKeys[top]
		page := &pages[top]
		pageID = pageIDs[top]
		if num == 0 {
			freelist[pageID] = false
			if _, err := f.ReadAt(page[:], int64(pageID*PageSize)); err != nil {
				return nil, fmt.Errorf("reading FREE_DBI page: %v", err)
			}
			var flags uint16
			var lowerFree int
			pos, _, flags, lowerFree = readPageHeader(page[:], 0)
			branchFlag := flags&BranchPageFlag > 0
			if branchFlag && !branch {
				return nil, fmt.Errorf("unexpected branch page on level %d of FREE_DBI", top)
			}
			if !branchFlag && branch {
				return nil, fmt.Errorf("expected branch page on level %d of FREE_DBI", top)
			}
			num = (lowerFree - pos) / 2
			i = 0
			numKeys[top] = num
			indices[top] = i
			visbufs[top].Reset()
			if branch {
				fmt.Fprintf(&visbufs[top], "p_%d [shape=record style=filled fillcolor=\"#AED6F1\" label=\"", pageID)
			} else {

				fmt.Fprintf(&visbufs[top], "p_%d [shape=record style=filled fillcolor=\"#AED6F1\" label=\"", pageID)
			}
		} else if i < num {
			nodePtr := int(binary.LittleEndian.Uint16(page[HeaderSize+i*2:]))
			indices[top] = i + 1
			if branch {
				if i > 0 {
					fmt.Fprintf(&visbufs[top], "|")
				}
				pagePtr := binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
				fmt.Fprintf(&visbufs[top], "<n%d>%d", i, pagePtr)
				fmt.Fprintf(visStream, "p_%d:n%d -> p_%d;\n", pageID, i, pagePtr)
				top++
				indices[top] = 0
				numKeys[top] = 0
				pageIDs[top] = binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
			} else {
				freeEntries++
				dataSize := int(binary.LittleEndian.Uint32(page[nodePtr:]))
				nodeFlags := binary.LittleEndian.Uint16(page[nodePtr+4:])
				keySize := int(binary.LittleEndian.Uint16(page[nodePtr+6:]))
				var pageList []uint64
				var overflowNum int
				if nodeFlags&BigDataNodeFlag > 0 {
					overflowPageID := binary.LittleEndian.Uint64(page[nodePtr+8+keySize:])
					freelist[overflowPageID] = false
					if _, err := f.ReadAt(overflow[:], int64(overflowPageID*PageSize)); err != nil {
						return nil, fmt.Errorf("reading FREE_DBI overflow page: %v", err)
					}
					_, _, overflowNum = readOverflowPageHeader(overflow[:], 0)
					overflows += overflowNum
					left := dataSize - 8
					// Start with pos + 8 because first 8 bytes is the size of the list
					for j := HeaderSize + 8; j < PageSize && left > 0; j += 8 {
						pn := binary.LittleEndian.Uint64(overflow[j:])
						freepages++
						freelist[pn] = true
						pageList = append(pageList, pn)
						left -= 8
					}
					for k := 1; k < overflowNum; k++ {
						freelist[overflowPageID+uint64(k)] = false
						if _, err := f.ReadAt(overflow[:], int64((overflowPageID+uint64(k))*PageSize)); err != nil {
							return nil, fmt.Errorf("reading FREE_DBI overflow page: %v", err)
						}
						for j := 0; j < PageSize && left > 0; j += 8 {
							pn := binary.LittleEndian.Uint64(overflow[j:])
							freepages++
							freelist[pn] = true
							pageList = append(pageList, pn)
							left -= 8
						}
					}
				} else {
					// First 8 bytes is the size of the list
					for j := nodePtr + 8 + keySize + 8; j < nodePtr+8+keySize+dataSize; j += 8 {
						pn := binary.LittleEndian.Uint64(page[j:])
						pageList = append(pageList, pn)
						freepages++
						freelist[pn] = true
					}
				}
				if i > 0 {
					fmt.Fprintf(&visbufs[top], "|")
				}
				txID := binary.LittleEndian.Uint64(page[nodePtr+8:])
				if overflowNum > 0 {
					fmt.Fprintf(&visbufs[top], "txid(%d)(ON %d OVERFLOW PAGES)=", txID, overflowNum)
				} else {
					fmt.Fprintf(&visbufs[top], "txid(%d)=", txID)
				}
				var sb strings.Builder
				runLength := 0
				maxGap := 0
				var prevPn uint64
				for _, pn := range pageList {
					if pn+1 == prevPn {
						runLength++
					} else {
						if prevPn > 0 {
							if runLength > 0 {
								fmt.Fprintf(&sb, "-%d(%d),", prevPn, runLength+1)
							} else {
								fmt.Fprintf(&sb, ",")
							}
						}
						fmt.Fprintf(&sb, "%d", pn)
						runLength = 0
					}
					prevPn = pn
					if runLength+1 > maxGap {
						maxGap = runLength + 1
					}
				}
				if runLength > 0 {
					fmt.Fprintf(&sb, "-%d(%d)", prevPn, runLength+1)
				}
				s := sb.String()
				if len(s) > 40 {
					fmt.Fprintf(&visbufs[top], "%s ... (max gap %d)", s[:40], maxGap)
				} else {
					fmt.Fprintf(&visbufs[top], "%s", s)
				}
			}
		} else {
			fmt.Fprintf(&visbufs[top], "\"];\n")
			if _, err := visStream.Write([]byte(visbufs[top].String())); err != nil {
				return nil, fmt.Errorf("writing buffer into main stream: %w", err)
			}
			top--
		}
	}
	log.Info("Freelist", "pages", freepages, "entries", freeEntries, "overflows", overflows)
	return freelist, nil
}

func readPageHeader(page []byte, pos int) (newpos int, pageID uint64, flags uint16, lowerFree int) {
	pageID = binary.LittleEndian.Uint64(page[pos:])
	pos += 8
	pos += 2 // Padding
	flags = binary.LittleEndian.Uint16(page[pos:])
	pos += 2
	lowerFree = int(binary.LittleEndian.Uint16(page[pos:]))
	pos += 4 // Overflow page number / lower upper bound of free space
	newpos = pos
	return
}

func readMetaPage(page []byte, pos int) (freeRoot uint64, freeDepth uint16, mainRoot uint64, mainDepth uint16, txnID uint64, err error) {
	magic := binary.LittleEndian.Uint32(page[pos:])
	if magic != MdbMagic {
		err = fmt.Errorf("meta page has wrong magic: %X != %X", magic, MdbMagic)
		return
	}
	pos += 4
	version := binary.LittleEndian.Uint32(page[pos:])
	if version != MdbDataVersion {
		err = fmt.Errorf("meta page has wrong version: %d != %d", version, MdbDataVersion)
		return
	}
	pos += 4
	pos += 8 // Fixed address
	pos += 8 // Map size
	pos, freeRoot, freeDepth = readDbRecord(page[:], pos)
	pos, mainRoot, mainDepth = readDbRecord(page[:], pos)
	pos += 8 // Last page
	txnID = binary.LittleEndian.Uint64(page[pos:])
	return
}

func readDbRecord(page []byte, pos int) (newpos int, rootPageID uint64, depth uint16) {
	pos += 4 // Padding (key size for fixed key databases)
	pos += 2 // Flags
	depth = binary.LittleEndian.Uint16(page[pos:])
	pos += 2 // Depth
	pos += 8 // Number of branch pages
	pos += 8 // Number of leaf pages
	pos += 8 // Number of overflow pages
	pos += 8 // Number of entries
	rootPageID = binary.LittleEndian.Uint64(page[pos:])
	pos += 8
	newpos = pos
	return
}

func readOverflowPageHeader(page []byte, pos int) (newpos int, flags uint16, overflowNum int) {
	pos += 8 // Page ID
	pos += 2 // Padding
	flags = binary.LittleEndian.Uint16(page[pos:])
	pos += 2
	overflowNum = int(binary.LittleEndian.Uint32(page[pos:]))
	pos += 4 // Overflow page number / lower upper bound of free space
	newpos = pos
	return
}

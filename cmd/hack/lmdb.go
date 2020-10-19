package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
)

const PageSize = 4096
const MdbMagic uint32 = 0xBEEFC0DE
const MdbDataVersion uint32 = 1
const BranchPageFlag uint16 = 1
const BigDataFlag uint16 = 1

func defrag(chaindata string) error {
	fmt.Printf("Defrag %s\n", chaindata)
	datafile := path.Join(chaindata, "data.mdb")
	f, err := os.Open(datafile)
	if err != nil {
		return fmt.Errorf("opening data.mdb: %v", err)
	}
	defer f.Close()
	var page [PageSize]byte
	// Read meta page 0
	if _, err = f.ReadAt(page[:], 0*PageSize); err != nil {
		return fmt.Errorf("reading meta page 0: %v", err)
	}
	pos, pageID, _, _ := readPageHeader(page[:], 0)
	if pageID != 0 {
		return fmt.Errorf("meta page 0 has wrong page ID: %d != %d", pageID, 0)
	}
	var freeRoot0, mainRoot0, txnID0 uint64
	var freeDepth0, mainDepth0 uint16
	pos, freeRoot0, freeDepth0, mainRoot0, mainDepth0, txnID0, err = readMetaPage(page[:], pos)
	if err != nil {
		return fmt.Errorf("reading meta page 0: %v", err)
	}

	// Read meta page 0
	if _, err = f.ReadAt(page[:], 1*PageSize); err != nil {
		return fmt.Errorf("reading meta page 1: %v", err)
	}
	pos, pageID, _, _ = readPageHeader(page[:], 0)
	if pageID != 1 {
		return fmt.Errorf("meta page 1 has wrong page ID: %d != %d", pageID, 1)
	}
	var freeRoot1, mainRoot1, txnID1 uint64
	var freeDepth1, mainDepth1 uint16
	pos, freeRoot1, freeDepth1, mainRoot1, mainDepth1, txnID1, err = readMetaPage(page[:], pos)
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
	fmt.Printf("FREE_DBI root page ID: %d, depth: %d\n", freeRoot, freeDepth)
	fmt.Printf("MAIN_DBI root page ID: %d, depth: %d\n", mainRoot, mainDepth)

	// Read root page of FREE_DBI
	if _, err = f.ReadAt(page[:], int64(freeRoot*PageSize)); err != nil {
		return fmt.Errorf("reading FREE_DBI root page: %v", err)
	}
	var flags uint16
	var lowerFree int
	pos, _, flags, lowerFree = readPageHeader(page[:], 0)
	fmt.Printf("FREE_DBI root page is branch page: %t\n", flags&BranchPageFlag > 0)
	numKeys := (lowerFree - pos) / 2
	fmt.Printf("FREE_DBI number of keys: %d\n", numKeys)
	var childPageID uint64
	for i := 0; i < numKeys; i++ {
		nodePtr := binary.LittleEndian.Uint16(page[pos+i*2:])
		fmt.Printf("Nodeptr[%d]=%d\n", i, nodePtr)
		// Only 6 bytes are used for child page ID
		childPageID = binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
		fmt.Printf("Child page ID: %d\n", childPageID)
	}

	if _, err = f.ReadAt(page[:], int64(childPageID*PageSize)); err != nil {
		return fmt.Errorf("reading FREE_DBI level2 page: %v", err)
	}
	pos, _, flags, lowerFree = readPageHeader(page[:], 0)
	fmt.Printf("FREE_DBI level2 page is branch page: %t\n", flags&BranchPageFlag > 0)
	numKeys = (lowerFree - pos) / 2
	fmt.Printf("FREE_DBI number of keys: %d\n", numKeys)
	for i := 0; i < numKeys; i++ {
		nodePtr := binary.LittleEndian.Uint16(page[pos+i*2:])
		fmt.Printf("Nodeptr[%d]=%d\n", i, nodePtr)
		// Only 6 bytes are used for child page ID
		childPageID = binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
		fmt.Printf("Child page ID: %d\n", childPageID)
	}

	if _, err = f.ReadAt(page[:], int64(childPageID*PageSize)); err != nil {
		return fmt.Errorf("reading FREE_DBI level3 page: %v", err)
	}
	pos, _, flags, lowerFree = readPageHeader(page[:], 0)
	fmt.Printf("FREE_DBI level3 page is branch page: %t\n", flags&BranchPageFlag > 0)
	numKeys = (lowerFree - pos) / 2
	fmt.Printf("FREE_DBI number of keys: %d\n", numKeys)
	var overflowPageID uint64
	for i := 0; i < numKeys; i++ {
		nodePtr := binary.LittleEndian.Uint16(page[pos+i*2:])
		fmt.Printf("Nodeptr[%d]=%d\n", i, nodePtr)
		dataSize := binary.LittleEndian.Uint32(page[nodePtr:])
		flags := binary.LittleEndian.Uint16(page[nodePtr+4:])
		keySize := binary.LittleEndian.Uint16(page[nodePtr+6:])
		// Only 6 bytes are used for child page ID
		//childPageID := binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
		fmt.Printf("Key size: %d, flags: %x, data size: %d\n", keySize, flags, dataSize)
		if flags&BigDataFlag > 0 {
			overflowPageID = binary.LittleEndian.Uint64(page[nodePtr+8+keySize:])
			fmt.Printf("Overflow pageID: %d\n", overflowPageID)
		}
	}

	if _, err = f.ReadAt(page[:], int64(overflowPageID*PageSize)); err != nil {
		return fmt.Errorf("reading FREE_DBI overflow page: %v", err)
	}
	var overflowNum int
	pos, flags, overflowNum = readOverflowPageHeader(page[:], 0)
	fmt.Printf("Number of overflow pages: %d\n", overflowNum)
	for i := 1; i < overflowNum; i++ {
		if _, err = f.ReadAt(page[:], int64((overflowPageID+1)*PageSize)); err != nil {
			return fmt.Errorf("reading FREE_DBI overflow page: %v", err)
		}
		pos, flags, overflowNum = readOverflowPageHeader(page[:], 0)
		fmt.Printf("Number of overflow pages: %d\n", overflowNum)
	}
	return nil
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

func readMetaPage(page []byte, pos int) (newpos int, freeRoot uint64, freeDepth uint16, mainRoot uint64, mainDepth uint16, txnID uint64, err error) {
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
	pos += 8
	newpos = pos
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

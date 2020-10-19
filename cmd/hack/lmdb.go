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
const HeaderSize int = 16

func defrag(chaindata string) error {
	fmt.Printf("Defrag %s\n", chaindata)
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
	pos, freeRoot0, freeDepth0, mainRoot0, mainDepth0, txnID0, err = readMetaPage(meta[:], pos)
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
	pos, freeRoot1, freeDepth1, mainRoot1, mainDepth1, txnID1, err = readMetaPage(meta[:], pos)
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

	var freelist []uint64
	var freeEntries int
	var overflows int
	var pages [8][PageSize]byte // Stack of pages
	var pageIDs [8]uint64
	var numKeys [8]int
	var indices [8]int
	var top int
	pageIDs[0] = freeRoot
	for top >= 0 {
		branch := top < int(freeDepth)-1
		i := indices[top]
		num := numKeys[top]
		page := &pages[top]
		if num == 0 {
			pageID = pageIDs[top]
			//fmt.Printf("top %d, num == 0, pageID = %d\n", top, pageID)
			if _, err = f.ReadAt(page[:], int64(pageID*PageSize)); err != nil {
				return fmt.Errorf("reading FREE_DBI page: %v", err)
			}
			var flags uint16
			var lowerFree int
			pos, _, flags, lowerFree = readPageHeader(page[:], 0)
			branchFlag := flags&BranchPageFlag > 0
			if branchFlag && !branch {
				return fmt.Errorf("unexpected branch page on level %d of FREE_DBI", top)
			}
			if !branchFlag && branch {
				return fmt.Errorf("expected branch page on level %d of FREE_DBI", top)
			}
			num = (lowerFree - pos) / 2
			i = 0
			numKeys[top] = num
			indices[top] = i
		} else if i < num {
			nodePtr := int(binary.LittleEndian.Uint16(page[HeaderSize+i*2:]))
			//fmt.Printf("top %d, i %d, num %d, nodePtr %d\n", top, i, num, nodePtr)
			i++
			indices[top] = i
			if branch {
				top++
				indices[top] = 0
				numKeys[top] = 0
				pageIDs[top] = binary.LittleEndian.Uint64(page[nodePtr:]) & 0xFFFFFFFFFFFF
			} else {
				freeEntries++
				dataSize := int(binary.LittleEndian.Uint32(page[nodePtr:]))
				flags := binary.LittleEndian.Uint16(page[nodePtr+4:])
				keySize := int(binary.LittleEndian.Uint16(page[nodePtr+6:]))
				if flags&BigDataFlag > 0 {
					overflowPageID := binary.LittleEndian.Uint64(page[nodePtr+8+keySize:])
					if _, err = f.ReadAt(meta[:], int64(overflowPageID*PageSize)); err != nil {
						return fmt.Errorf("reading FREE_DBI overflow page: %v", err)
					}
					var overflowNum int
					pos, flags, overflowNum = readOverflowPageHeader(meta[:], 0)
					overflows += overflowNum
					left := dataSize - 8
					// Start with pos + 8 because first 8 bytes is the size of the list
					for j := pos + 8; j < PageSize && left > 0; j += 8 {
						freelist = append(freelist, binary.LittleEndian.Uint64(meta[j:]))
						left -= 8
					}
					for i := 1; i < overflowNum; i++ {
						if _, err = f.ReadAt(meta[:], int64((overflowPageID+uint64(i))*PageSize)); err != nil {
							return fmt.Errorf("reading FREE_DBI overflow page: %v", err)
						}
						for j := 0; j < PageSize && left > 0; j += 8 {
							freelist = append(freelist, binary.LittleEndian.Uint64(meta[j:]))
							left -= 8
						}
					}
				} else {
					// First 8 bytes is the size of the list
					for j := nodePtr + 8 + keySize + 8; j < nodePtr+8+keySize+dataSize; j += 8 {
						freelist = append(freelist, binary.LittleEndian.Uint64(meta[j:]))
					}
				}
			}
		} else {
			//fmt.Printf("top %d, i %d, num %d\n", top, i, num)
			top--
		}
	}
	fmt.Printf("Size of freelist: %d, entries: %d, overflows: %d\n", len(freelist), freeEntries, overflows)
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

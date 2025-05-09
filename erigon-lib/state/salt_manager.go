package state

import (
	"crypto/rand"
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	rand2 "golang.org/x/exp/rand"
)

type SaltManager struct {
	stateMmap, blockMmap *mmap.MMap
	stateFile, blockFile *os.File
	genState, genBlock   bool
	dirs                 datadir.Dirs
}

func NewSaltManager(dirs datadir.Dirs, genState, genBlock bool) *SaltManager {
	m := &SaltManager{
		genState: genState,
		genBlock: genBlock,
		dirs:     dirs,
	}

	m.blockFile, m.blockMmap = m.loadSalt("salt-block.txt", genBlock, m.newBlockSaltGen)
	m.stateFile, m.stateMmap = m.loadSalt("salt-state.txt", genState, m.newStateSaltGen)
	return m
}

func (m *SaltManager) StateSalt() *uint32 {
	return m.getSalt(m.stateMmap)
}

func (m *SaltManager) BlockSalt() *uint32 {
	return m.getSalt(m.blockMmap)
}

func (m *SaltManager) getSalt(mmap *mmap.MMap) *uint32 {
	if mmap == nil {
		return nil
	}

	v := binary.BigEndian.Uint32(*mmap)
	return &v
}

func (m *SaltManager) loadSalt(name string, gen bool, newSaltBytesGen func() []byte) (*os.File, *mmap.MMap) {
	filename := filepath.Join(m.dirs.Snap, name)
	exists, err := dir.FileExist(filename)
	if err != nil {
		panic(err)
	}
	if !exists {
		if gen {
			if err := dir.WriteFileWithFsync(filename, newSaltBytesGen(), os.ModePerm); err != nil {
				panic(err)
			}
		} else {
			return nil, nil
		}
	}

	// now exists, get mmap
	osFile, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	fmmap, err := mmap.Map(osFile, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}

	// TODO: look at decompressor, it has two mmapHandles (one for windows)

	return osFile, &fmmap
}

func (m *SaltManager) Close() {
	if m.stateMmap != nil {
		m.stateMmap.Unmap()
	}
	if m.blockMmap != nil {
		m.blockMmap.Unmap()
	}
	if m.stateFile != nil {
		m.stateFile.Close()
	}
	if m.blockFile != nil {
		m.blockFile.Close()
	}
}

func (m *SaltManager) newStateSaltGen() []byte {
	saltV := rand2.Uint32()
	saltBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(saltBytes, saltV)
	return saltBytes
}

func (m *SaltManager) newBlockSaltGen() []byte {
	// taken from ReadAndCreateSaltIfNeeded
	var buf [4]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		panic(err)
	}
	v := binary.LittleEndian.Uint32(buf[:])
	saltBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(saltBytes, v)
	return saltBytes
}

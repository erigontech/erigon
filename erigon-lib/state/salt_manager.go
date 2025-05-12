package state

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	rand2 "golang.org/x/exp/rand"
)

var DEFAULT_SALT = [4]byte{0xff, 0xff, 0xff, 0xff}

type SaltManager struct {
	stateMmap, blockMmap *mmap.MMap
	stateFile, blockFile *os.File
	genState, genBlock   bool
	dirs                 datadir.Dirs
	logger               log.Logger
}

func NewSaltManager(dirs datadir.Dirs, genState, genBlock bool, logger log.Logger) *SaltManager {
	m := &SaltManager{
		genState: genState,
		genBlock: genBlock,
		dirs:     dirs,
		logger:   logger,
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
	dst := make([]byte, 4)
	mmap.Lock()
	if len(*mmap) < 4 {
		m.logger.Warn("salt file mmap is %d; shouldn't happen, maybe it got deleted?", len(*mmap))
		mmap.Unlock()
		return nil
	}
	copy(dst, (*mmap)[:4])
	mmap.Unlock()
	if bytes.Equal(dst, DEFAULT_SALT[:]) {
		return nil
	}
	v := binary.BigEndian.Uint32(dst)
	return &v
}

func (m *SaltManager) loadSalt(name string, gen bool, newSaltBytesGen func() []byte) (*os.File, *mmap.MMap) {
	filename := filepath.Join(m.dirs.Snap, name)
	exists, err := dir.FileExist(filename)
	if err != nil {
		panic(err)
	}
	if !exists {
		var salt []byte
		if gen {
			salt = newSaltBytesGen()
		} else {
			salt = DEFAULT_SALT[:]
		}

		if err := dir.WriteFileWithFsync(filename, salt, os.ModePerm); err != nil {
			panic(err)
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

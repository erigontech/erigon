package qmtree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/OneOfOne/xxhash"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/qmtree/hpfile"
)

/*
We store top levels(1~255) and the leaves(2048~4095), while the middle levels are ignored to save disk
When we need to read the nodes in ignored levels, we must compute their value on-the-fly from leaves
Level_11  =  1
Level_10  =  2~3
Level_9   =  4~7
Level_8   =  8~15
Level_7   =  16~31
Level_6   =  32~63
Level_5   =  64~127
Level_4   =  128~255
Level_3   X  256~511
Level_2   X  512~1023
Level_1   X  1024~2047
Level_0   =  2048~4095
*/

const (
	IGNORED_COUNT    = 2048 - 256 // the count of ignored nodes
	TWIG_FULL_LENGTH = 4095       // the total count of all the internal nodes and the leaves
	TWIG_ENTRY_COUNT = TWIG_FULL_LENGTH - IGNORED_COUNT
	TWIG_SIZE        = 12 + TWIG_ENTRY_COUNT*32
	HPFILE_RANGE     = 1 << 51 // 2048TB

)

type TwigFile struct {
	file   *hpfile.File
	hasher Hasher
}

func NewTwigFile(bufferSize int, segmentSize uint64, dirName string, hasher Hasher) (*TwigFile, error) {
	file, err := hpfile.NewFile(bufferSize, segmentSize, dirName)
	if err != nil {
		return nil, err
	}
	return &TwigFile{
		file:   file,
		hasher: hasher,
	}, nil
}

func EmptyTwigFile() *TwigFile {
	return &TwigFile{
		file: hpfile.Empty(),
	}
}

func (tf *TwigFile) IsEmpty() bool {
	return tf.file.IsEmpty()
}

func (tf *TwigFile) appendTwig(mTree []common.Hash, lastEntryEndPos int64) error {
	if tf.file.IsEmpty() {
		return nil
	}
	if lastEntryEndPos < 0 {
		panic(fmt.Sprintf("Invalid last entry end position: %d", lastEntryEndPos))
	}
	if len(mTree) != TWIG_FULL_LENGTH+1 {
		panic(fmt.Sprintf("len(m_tree): %d != %d", len(mTree), TWIG_FULL_LENGTH))
	}
	// last_entry_end_pos and its 32b hash need 12 bytes
	buf := [12]byte{}
	binary.LittleEndian.PutUint64(buf[0:8], uint64(lastEntryEndPos))
	digest := xxhash.Checksum32(buf[0:8])
	binary.LittleEndian.PutUint32(buf[8:], digest)
	if _, err := tf.file.Append(buf[:]); err != nil {
		return err
	}

	// only write the higher levels and leaf nodes, middle levels are ignored
	for i := 1; i < 256; i++ {
		if _, err := tf.file.Append(mTree[i][:]); err != nil {
			return err
		}
	}
	for i := 2048; i < TWIG_FULL_LENGTH+1; i++ {
		if _, err := tf.file.Append(mTree[i][:]); err != nil {
			return err
		}
	}

	return nil
}

func (tf *TwigFile) GetFirstEntryPos(twigId uint64) (int64, error) {
	if twigId == 0 {
		return 0, nil
	}
	// the end pos of previous twig's last entry is the
	// pos of current twig's first entry
	twigId -= 1
	buf := [12]byte{}
	tf.file.ReadAt(buf[:], (twigId*TWIG_SIZE)%HPFILE_RANGE)
	digest := [4]byte{}
	binary.LittleEndian.PutUint32(digest[:], xxhash.Checksum32(buf[0:8]))
	if !bytes.Equal(buf[8:], digest[:]) {
		return 0, errors.New("checksum Error")
	}
	return int64(binary.LittleEndian.Uint64(buf[0:8])), nil
}

// for the ignored middle layer, we must calculate the node's value from leaves in a range
func GetLeafRange(hashId uint64) (uint64, uint64, error) {
	if 256 <= hashId && hashId < 512 {
		// level_3 : 256~511
		return hashId * 8, hashId*8 + 8, nil
	} else if hashId < 1024 {
		//level_2 : 512~1023
		return (hashId / 2) * 8, (hashId/2)*8 + 8, nil
	} else if hashId < 2048 {
		//level_1 : 1024~2047
		return (hashId / 4) * 8, (hashId/4)*8 + 8, nil
	} else {
		return 0, 0, errors.New("Invalid hashid")
	}
}

func (tf *TwigFile) GetHashNodeInIgnoreRange(twigId uint64, hashId uint64, cache map[uint64]common.Hash) (common.Hash, error) {
	start, end, err := GetLeafRange(hashId)
	if err != nil {
		return common.Hash{}, err
	}
	buf := [32 * 8]byte{}
	offset := twigId*TWIG_SIZE + 12 + (start-1 /*because hashId starts from 1*/)*32 - IGNORED_COUNT*32
	offset = (offset) % HPFILE_RANGE
	numBytesRead, err := tf.file.ReadAt(buf[:], offset)
	if err != nil {
		return common.Hash{}, err
	}
	if numBytesRead != int64(len(buf)) {
		// Cannot read them in one call because of file-crossing
		for i := range uint64(8) {
			//read them in 8 steps in case of file-crossing
			if _, err = tf.file.ReadAt(buf[i*32:i*32+32], offset+(i*32)); err != nil {
				return common.Hash{}, err
			}
		}
	}
	// recover a little cone with 8 leaves into cache
	// this little cone will be queried by 'get_proof'
	level := uint8(0)
	for i := start / 2; i < end/2; i++ {
		off := ((i - start/2) * 64)
		v := tf.hasher.hash2(level, buf[off:off+32], buf[off+32:off+64])
		cache[i] = v
	}
	level = 1
	for i := start / 4; i < end/4; i++ {
		left, right := cache[(i*2)], cache[(i*2+1)]
		v := tf.hasher.hash2(level, left[:], right[:])
		cache[i] = v
	}
	level = 2
	id := start / 8
	left, right := cache[(id*2)], cache[(id*2+1)]
	v := tf.hasher.hash2(level, left[:], right[:])
	cache[id] = v
	return cache[hashId], nil
}

func (tf *TwigFile) GetHashRoot(twigId uint64) (common.Hash, error) {
	return tf.GetHashNode(twigId, 1, map[uint64]common.Hash{})
}

func (tf *TwigFile) GetHashNode(twigId uint64, hashId uint64, cache map[uint64]common.Hash) (common.Hash, error) {
	if hashId <= 0 || hashId >= 4096 {
		return common.Hash{}, fmt.Errorf("Invalid hashId: %d", hashId)
	}
	if hashId >= 256 && hashId < 2048 {
		return tf.GetHashNodeInIgnoreRange(twigId, hashId, cache)
	}
	offset := twigId*TWIG_SIZE + 12 + (hashId-1 /*because hashId starts from 1*/)*32
	if hashId >= 2048 {
		offset -= IGNORED_COUNT * 32
	}
	offset = (offset) % HPFILE_RANGE
	buf := [32]byte{}
	_, err := tf.file.ReadAt(buf[:], offset)
	if err != nil {
		return common.Hash{}, err
	}
	return buf, err
}

func (tf *TwigFile) Truncate(size int64) error {
	return tf.file.Truncate(size)
}

func (tf *TwigFile) Close() {
	tf.file.Close()
}

func (tf *TwigFile) PruneHead(off uint64) error {
	return tf.file.PruneHead(off)
}

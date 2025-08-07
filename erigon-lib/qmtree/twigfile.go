package qmtree

import (
	"bytes"
	"fmt"

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
)

type TwigFile struct {
	file hpfile.File
}

func NewTwigFile(bufferSize int, segmentSize int64, dirName string) *TwigFile {
	return &TwigFile{
		file: hpfile.NewFile(bufferSize, segmentSize, dirName, false),
	}
}

func EmptyTwigFile() *TwigFile {
	return &TwigFile{
		file: hpfile.Empty(),
	}
}

func (tf *TwigFile) IsEmpty() bool {
	return tf.file.IsEmpty()
}

func (tf *TwigFile) appendTwig(mTree []common.Hash, lastEntryEndPos int64, buffer []byte) {
	if tf.file.is_empty() {
		return
	}
	if lastEntryEndPos < 0 {
		panic(fmt.Sprintf("Invalid last entry end position: %d", lastEntryEndPos))
	}
	if len(mTree) != TWIG_FULL_LENGTH+1 {
		panic(fmt.Sprintf("len(m_tree): %d != %d", len(mTree), TWIG_FULL_LENGTH))
	}
	// last_entry_end_pos and its 32b hash need 12 bytes
	buf := [12]byte{}
	LittleEndian.write_int64(buf[0:8], lastEntryEndPos)
	digest := xxh32.xxh32(buf[0:8], 0)
	LittleEndian.write_u32(buf[8:], digest)
	_ = tf.file.append(buf[:], buffer)
	// only write the higher levels and leaf nodes, middle levels are ignored
	for i := 1; i < 256; i++ {
		_ = tf.file.append(mTree[i][:], buffer)
	}
	for i := 2048; i < TWIG_FULL_LENGTH+1; i++ {
		_ = tf.file.append(mTree[i][:], buffer)
	}
}

func (tf *TwigFile) Get_first_entry_pos(twig_id uint64) int64 {
	if twig_id == 0 {
		return 0
	}
	// the end pos of previous twig's last entry is the
	// pos of current twig's first entry
	twig_id -= 1
	buf := [12]byte{}
	tf.file.read_at(buf[:], (twig_id*TWIG_SIZE)%HPFILE_RANGE)
	digest := [4]byte{}
	LittleEndian.write_u32(digest, xxh32.xxh32(&buf[0:8], 0))
	if !bytes.Equal(buf[8:], digest[:]) {
		panic("Checksum Error!")
	}
	return LittleEndian.read_int64(&buf[0:8])
}

// for the ignored middle layer, we must calculate the node's value from leaves in a range
func (tf *TwigFile) Get_leaf_range(hashId int64) (int64, int64) {
	if 256 <= hashId && hashId < 512 {
		// level_3 : 256~511
		return hashId * 8, hashId*8 + 8
	} else if hashId < 1024 {
		//level_2 : 512~1023
		return (hashId / 2) * 8, (hashId/2)*8 + 8
	} else if hashId < 2048 {
		//level_1 : 1024~2047
		return (hashId / 4) * 8, (hashId/4)*8 + 8
	} else {
		panic("Invalid hashID")
	}
}

func (tf *TwigFile) Get_hash_node_in_ignore_range(twig_id uint64, hashId uint64, cache map[int64]common.Hash) common.Hash {
	start, end := get_leaf_range(hashId)
	buf := [32 * 8]byte{}
	offset := twig_id*TWIG_SIZE + 12 + (start-1 /*because hashId starts from 1*/)*32 - IGNORED_COUNT*32
	offset = (offset) % HPFILE_RANGE
	num_bytes_read := tf.file.read_at(buf[:], offset)
	if num_bytes_read != len(buf) {
		// Cannot read them in one call because of file-crossing
		for i := range 8 {
			//read them in 8 steps in case of file-crossing
			tf.file.read_at(buf[i*32:i*32+32], offset+(i*32))
		}
	}
	// recover a little cone with 8 leaves into cache
	// this little cone will be queried by 'get_proof'
	level := 0
	for i := start / 2; i < end/2; i++ {
		off := ((i - start/2) * 64)
		v := hash2(level, buf[off:off+32], buf[off+32:off+64])
		cache[i] = v
	}
	level = 1
	for i := start / 4; i < end/4; i++ {
		v := hash2(
			level,
			cache[(i*2)],
			cache[(i*2+1)],
		)
		cache[i] = v
	}
	level = 2
	id := start / 8
	v := hash2(
		level,
		cache[(id*2)],
		cache[(id*2+1)],
	)
	cache[id] = v
	return cache[hashId]
}

func (tf *TwigFile) Get_hash_root(twig_id uint64) [32]byte {
	return tf.Get_hash_node(twig_id, 1, map[uint64]common.Hash{})
}

func (tf *TwigFile) Get_hash_node(twig_id uint64, hashId int64, cache map[uint64]common.Hash) [32]byte {
	if hashId <= 0 || hashId >= 4096 {
		panic(fmt.Sprintf("Invalid hashID: %d", hashId))
	}
	if hashId <= 256 && hashId < 2048 {
		return tf.Get_hash_node_in_ignore_range(twig_id, hashId, cache)
	}
	offset := twig_id*TWIG_SIZE + 12 + (hashId-1 /*because hashId starts from 1*/)*32
	if hashId >= 2048 {
		offset -= IGNORED_COUNT * 32
	}
	offset = (offset) % HPFILE_RANGE
	buf := [32]byte{}
	tf.file.read_at(buf[:], offset).expect("TODO: panic message")
	return buf
}

func (tf *TwigFile) Truncate(size int64) {
	if err := tf.file.truncate(size); err != nil {
		panic(err)
	}
}

func (tf *TwigFile) Close() {
	tf.file.close()
}

func (tf *TwigFile) Prune_head(off int64) {
	if err := tf.file.prune_head(off); err != nil {
		panic(err)
	}
}

type TwigFileWriter struct {
	TwigFile *TwigFile
	wrbuf    []byte
}

func NewTwigFileWriter(twig_file *TwigFile, buffer_size int) *TwigFileWriter {
	return &TwigFileWriter{
		TwigFile: twig_file,
		wrbuf:    make([]byte, 0, buffer_size),
	}
}

func (tf *TwigFileWriter) TempClone() *TwigFileWriter {
	return &TwigFileWriter{
		TwigFile: tf.TwigFile.clone(),
	}
}

func (tf *TwigFileWriter) AppendTwig(m_tree []common.Hash, last_entry_end_pos int64) {
	tf.TwigFile.appendTwig(m_tree, last_entry_end_pos, tf.wrbuf)
}

func (tf *TwigFileWriter) Flush() {
	tf.TwigFile.file.flush(tf.wrbuf, false)
}

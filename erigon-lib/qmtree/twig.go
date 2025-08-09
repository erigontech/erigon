package qmtree

import "github.com/erigontech/erigon-lib/common"

const (
	TwigShift           = 11             // a twig has 2**11 leaves
	LeafCountInTwig     = 1 << TwigShift // 2**11==2048
	FirstLevelAboveTwig = 13
	TwigRootLevel       = FirstLevelAboveTwig - 1 // 12
)

type TwigStorage interface {
	Append(twigMt TwigMT, pos int64)
	Flush()
	Close()
	Size() int64
	PruneHead(offset int64)
	Truncate(size int64)

	GetHashRoot(twigId uint64, leftRoot common.Hash) common.Hash
	GetHashNode(twigId uint64, idx uint64, cache map[int64]common.Hash) common.Hash

	CloneTemp() TwigStorage
}

type TwigMT []common.Hash // size is 4096

func (t TwigMT) Clone() TwigMT {
	clone := make(TwigMT, len(t))
	copy(clone, t)
	return t
}

func (mtree TwigMT) Sync(hasher Hasher, start int32, end int32) {
	cur_start := start
	cur_end := end
	level := uint8(0)
	base := int32(LeafCountInTwig)
	for base >= 2 {
		end_round := cur_end
		if cur_end%2 == 1 {
			end_round += 1
		}
		j := (cur_start >> 1) << 1 //clear the lowest bit of cur_start
		for j <= end_round && j+1 < base {
			i := int(base + j)
			mtree[i/2] = hasher.nodeHash(level, mtree[i], mtree[i+1])
			j += 2
		}
		cur_start >>= 1
		cur_end >>= 1
		level += 1
		base >>= 1
	}
}

type Twig struct {
	activeBitsMtl1 [4]common.Hash
	activeBitsMtl2 [4]common.Hash
	activeBitsMtl3 common.Hash
	leftRoot       common.Hash
	twigRoot       common.Hash
}

func nullTwig(hasher Hasher, nullMtForTwig common.Hash) Twig {
	var null Twig
	null.syncL1(hasher, 0, ActiveBits{})
	null.syncL1(hasher, 1, ActiveBits{})
	null.syncL1(hasher, 2, ActiveBits{})
	null.syncL1(hasher, 3, ActiveBits{})
	null.syncL2(hasher, 0)
	null.syncL2(hasher, 1)
	null.syncL3(hasher)

	null.leftRoot = nullMtForTwig
	null.syncTop(hasher)

	return null
}

func (t Twig) Clone() *Twig {
	return &t
}

func (t *Twig) syncL1(hasher Hasher, pos uint64, activeBits ActiveBits) {
	var hash0 common.Hash
	var hash1 common.Hash
	switch pos {
	case 0:
		copy(hash0[:], activeBits.GetBits(0, 32))
		copy(hash1[:], activeBits.GetBits(1, 32))
		t.activeBitsMtl1[0] = hasher.nodeHash(8, hash0, hash1)
	case 1:
		copy(hash0[:], activeBits.GetBits(2, 32))
		copy(hash1[:], activeBits.GetBits(3, 32))
		t.activeBitsMtl1[1] = hasher.nodeHash(8, hash0, hash1)
	case 2:
		copy(hash0[:], activeBits.GetBits(4, 32))
		copy(hash1[:], activeBits.GetBits(5, 32))
		t.activeBitsMtl1[2] = hasher.nodeHash(8, hash0, hash1)
	case 3:
		copy(hash0[:], activeBits.GetBits(6, 32))
		copy(hash1[:], activeBits.GetBits(7, 32))
		t.activeBitsMtl1[3] = hasher.nodeHash(8, hash0, hash1)
	default:
		panic("invalid twig position")
	}
}

func (t *Twig) syncL2(hasher Hasher, pos uint64) {
	switch pos {
	case 0:
		t.activeBitsMtl2[0] = hasher.nodeHash(9, t.activeBitsMtl1[0], t.activeBitsMtl1[1])
	case 1:
		t.activeBitsMtl2[1] = hasher.nodeHash(9, t.activeBitsMtl1[2], t.activeBitsMtl1[3])
	default:
		panic("Can not reach here!")
	}
}

func (t *Twig) syncL3(hasher Hasher) {
	t.activeBitsMtl3 = hasher.nodeHash(10, t.activeBitsMtl2[0], t.activeBitsMtl2[1])
}

func (t *Twig) syncTop(hasher Hasher) {
	t.twigRoot = hasher.nodeHash(11, t.leftRoot, t.activeBitsMtl3)
}

type ActiveBits [256]byte

func (ab ActiveBits) SetBit(offset uint32) {
	if offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := 1 << (offset & 0x7)
	pos := int(offset >> 3)
	ab[pos] |= byte(mask)
}

func (ab ActiveBits) ClearBit(offset uint32) {
	if offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := 1 << (offset & 0x7)
	pos := int(offset >> 3)
	ab[pos] &= byte(^mask) //bit-wise not
}

func (ab ActiveBits) GetBit(offset uint32) bool {
	if offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := 1 << (offset & 0x7)
	pos := int(offset >> 3)
	return (ab[pos] & byte(mask)) != 0
}

func (ab ActiveBits) GetBits(pageNum int, pageSize int) []byte {
	return ab[pageNum*pageSize : (pageNum+1)*pageSize]
}

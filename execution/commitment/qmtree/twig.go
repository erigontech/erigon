package qmtree

import "github.com/erigontech/erigon/common"

const (
	TwigShift           = 11             // a twig has 2**11 leaves
	LeafCountInTwig     = 1 << TwigShift // 2**11==2048
	FirstLevelAboveTwig = 13
	TwigRootLevel       = FirstLevelAboveTwig - 1 // 12
)

type TwigStorage interface {
	Append(twigMt TwigMT, pos int64) error
	Flush()
	Close()
	Size() int64
	PruneHead(offset uint64) error
	Truncate(size int64) error

	GetHashRoot(twigId uint64) (common.Hash, error)
	GetHashNode(twigId uint64, idx uint64, cache map[uint64]common.Hash) (common.Hash, error)

	CloneTemp() TwigStorage
}

type TwigMT []common.Hash // size is 4096

func (t TwigMT) Clone() TwigMT {
	clone := make(TwigMT, len(t))
	copy(clone, t)
	return clone
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
			mtree[i/2] = hasher.nodeHash(level, mtree[i][:], mtree[i+1][:])
			j += 2
		}
		cur_start >>= 1
		cur_end >>= 1
		level += 1
		base >>= 1
	}
}

// Twig holds the Merkle root of a completed twig's leaf-hash tree.
// The activeBits mechanism has been removed; twigRoot equals leftRoot directly.
type Twig struct {
	leftRoot common.Hash
	twigRoot common.Hash
}

func nullTwig(nullMtForTwig common.Hash) Twig {
	return Twig{leftRoot: nullMtForTwig, twigRoot: nullMtForTwig}
}

func (t Twig) Clone() *Twig {
	return &t
}

// syncTop sets twigRoot = leftRoot. The activeBits component has been removed,
// so the twig root is just the leaf-hash Merkle root.
func (t *Twig) syncTop() {
	t.twigRoot = t.leftRoot
}

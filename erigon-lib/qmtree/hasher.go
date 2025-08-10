package qmtree

import (
	"crypto/sha256"

	"github.com/erigontech/erigon-lib/common"
)

type Hasher interface {
	// node_hash_in_place
	nodeHash(level uint8, left []byte, right []byte) common.Hash
	hash2(level uint8, h0 []byte, h1 []byte) common.Hash
	hash2x(level uint8, h0 []byte, h1 []byte, b bool) common.Hash

	nullMtForTwig() TwigMT
	nullTwig() Twig
	nullNodeInHigerTree(level uint8) common.Hash
}

type rootHasher struct {
	//NullMtForTwig TwigMT = nullMtForTwig(nil) //TODO hasher
	//hasher.nullTwig() Twig = hasher.nullTwig()(NullMtForTwig[1])
	//NullNodeInHigerTree [64]common.Hash = nullNodeInHigherTree(&hasher.nullTwig())
}

func nullMtForTwig(hasher Hasher) TwigMT {
	nullHash := NullEntry{}.Hash()

	nullMtForTwig := make(TwigMT, 4096)
	for i := range 2048 {
		nullMtForTwig[i+2048] = nullHash
	}

	nullMtForTwig.Sync(hasher, 0, 2047)

	return nullMtForTwig
}

func nullNodeInHigherTree(hasher Hasher, nullTwig *Twig) [64]common.Hash {
	var nullNodeInHigherTree [64]common.Hash

	nullNodeInHigherTree[FirstLevelAboveTwig] = hasher.hash2(TwigRootLevel, nullTwig.twigRoot[:], nullTwig.twigRoot[:])

	for i := FirstLevelAboveTwig + 1; i < 64; i++ {
		nullNodeInHigherTree[int(i)] = hasher.hash2(
			byte(i-1), nullNodeInHigherTree[int(i-1)][:], nullNodeInHigherTree[int(i-1)][:])
	}

	return nullNodeInHigherTree
}

type Sha256Hasher struct {
	nulls *struct {
		mtForTwig        TwigMT
		nodesInHigerTree [64]common.Hash
		twig             Twig
	}
}

func (_ Sha256Hasher) nodeHash(level uint8, left []byte, right []byte) common.Hash {
	hasher := sha256.New()
	hasher.Write([]byte{level})
	hasher.Write(left)
	hasher.Write(right)
	return common.Hash(hasher.Sum(nil))
}

func (_ Sha256Hasher) hash2(level uint8, h0 []byte, h1 []byte) common.Hash {
	hasher := sha256.New()
	hasher.Write([]byte{level})
	hasher.Write(h0)
	hasher.Write(h1)
	return common.Hash(hasher.Sum(nil))
}

func (h Sha256Hasher) hash2x(level uint8, h0 []byte, h1 []byte, exchange bool) common.Hash {
	if exchange {
		return h.hash2(level, h1, h0)
	}
	return h.hash2(level, h0, h1)
}

func (h Sha256Hasher) initNulls() {
	h.nulls = &struct {
		mtForTwig        TwigMT
		nodesInHigerTree [64]common.Hash
		twig             Twig
	}{}
	h.nulls.mtForTwig = nullMtForTwig(h)
	h.nulls.twig = nullTwig(h, h.nulls.mtForTwig[1])
	h.nulls.nodesInHigerTree = nullNodeInHigherTree(h, &h.nulls.twig)
}

func (h Sha256Hasher) nullMtForTwig() TwigMT {
	if h.nulls == nil {
		h.initNulls()
	}

	return h.nulls.mtForTwig
}

func (h Sha256Hasher) nullTwig() Twig {
	if h.nulls == nil {
		h.initNulls()
	}

	return h.nulls.twig
}

func (h Sha256Hasher) nullNodeInHigerTree(level uint8) common.Hash {
	if h.nulls == nil {
		h.initNulls()
	}

	return h.nulls.nodesInHigerTree[level]
}

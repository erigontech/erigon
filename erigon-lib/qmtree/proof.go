package qmtree

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
)

type ProofNode struct {
	SelfHash   common.Hash
	PeerHash   common.Hash
	PeerAtLeft bool
}

type ProofPath struct {
	LeftOfTwig  [11]ProofNode
	RightOfTwig [3]ProofNode
	UpperPath   []ProofNode
	SerialNum   uint64
	Root        common.Hash
}

const OTHER_NODE_COUNT = 1 + 11 + 1 + 3 + 1

func (p *ProofPath) ToBytes() []byte {
	res := make([]byte, 0, (8 + (p.upper_path.len()+OTHER_NODE_COUNT)*32))
	res.extend_from_slice(&p.serial_num.to_le_bytes())  // 8-byte
	res.extend_from_slice(&p.left_of_twig[0].self_hash) // 1
	for i := range p.left_of_twig {
		//11
		res.extend_from_slice(&p.left_of_twig[i].peer_hash)
	}
	res.extend_from_slice(&p.right_of_twig[0].self_hash) //1
	for i := range p.right_of_twig {
		//3
		res.extend_from_slice(&p.right_of_twig[i].peer_hash)
	}
	for i := range p.upper_path {
		res.extend_from_slice(&p.upper_path[i].peer_hash)
	}
	res.extend_from_slice(&p.root) //1
	res
}

func (p *ProofPath) Check(complete bool) error {
	for i := range p.left_of_twig {
		res := hasher.hash2x(
			uint8(i),
			p.left_of_twig[i].self_hash,
			p.left_of_twig[i].peer_hash,
			p.left_of_twig[i].peer_at_left,
		)

		if complete {
			p.left_of_twig[i+1].self_hash.copy_from_slice(&res)
		} else if !res.eq(&p.left_of_twig[i+1].self_hash) {
			return nil, fmt.Errorf("mismatch at left path, level: %d", i)
		}
	}

	leaf_mt_root := p.hasher.hash2x(
		10,
		&p.left_of_twig[10].self_hash,
		&p.left_of_twig[10].peer_hash,
		p.left_of_twig[10].peer_at_left,
	)

	for i := range 2 {
		res := hasher.hash2x(
			uint8((i + 8)),
			&p.right_of_twig[i].self_hash,
			&p.right_of_twig[i].peer_hash,
			p.right_of_twig[i].peer_at_left,
		)
		if complete {
			p.right_of_twig[i+1].self_hash.copy_from_slice(&res)
		} else if !res.eq(&p.right_of_twig[i+1].self_hash) {
			return nil, fmt.Errorf("mismatch at right path, level: %d", i)
		}
	}

	active_bits_mt_l3 := p.hasher.hash2x(
		10,
		&p.right_of_twig[2].self_hash,
		&p.right_of_twig[2].peer_hash,
		p.right_of_twig[2].peer_at_left,
	)

	twig_root := p.hasher.hash2(11, &leaf_mt_root, &active_bits_mt_l3)
	if complete {
		p.upper_path[0].self_hash.copy_from_slice(&twig_root)
	} else if !twig_root.eq(&p.upper_path[0].self_hash) {
		return errors.New("mismatch at twig top")
	}

	for i := range p.upper_path {
		level := TWIG_ROOT_LEVEL + i
		res := p.hasher.hash2x(
			uint8(level),
			&p.upper_path[i].self_hash,
			&p.upper_path[i].peer_hash,
			p.upper_path[i].peer_at_left,
		)

		if i < p.upper_path.len()-1 {
			if complete {
				p.upper_path[i+1].self_hash.copy_from_slice(&res)
			} else if !res.eq(&p.upper_path[i+1].self_hash) {
				return fmt.Errorf("mismatch at upper path, level: %d", level)
			}
		} else if !res.eq(&p.root) {
			return errors.New("mismatch at root")
		}
	}

	return nil
}

func BytesToProofPath(bz []byte) (*ProofPath, error) {
	n := bz.len() - 8
	upper_count := (n/32 - OTHER_NODE_COUNT)
	if n%32 != 0 || upper_count < 0 {
		return nil, fmt.Errorf("Invalid byte slice length: *d", bz.len())
	}
	upper_path := make([]byte, 0, upper_count)
	empty_node := ProofNode{}
	left_of_twig = [11]ProofNode{}
	right_of_twig = [3]ProofNode{}
	serial_num := LittleEndian.read_uint64(bz[0:8])
	bz = bz[8:]
	left_of_twig[0].self_hash.copy_from_slice(bz[:32])
	bz = bz[32:]
	for i := range left_of_twig {
		left_of_twig[i].peer_hash.copy_from_slice(bz[:32])
		left_of_twig[i].peer_at_left = (serial_num>>i)&1 == 1
		bz = bz[32:]
	}
	right_of_twig[0].self_hash.copy_from_slice(bz[:32])
	bz = bz[32:]
	for i := range right_of_twig {
		right_of_twig[i].peer_hash.copy_from_slice(bz[:32])
		right_of_twig[i].peer_at_left = (serial_num>>(8+i))&1 == 1
		bz = bz[32:]
	}
	for i := range upper_count {
		node = empty_node
		node.peer_hash.copy_from_slice(bz[:32])
		node.peer_at_left = ((serial_num >> (FIRST_LEVEL_ABOVE_TWIG - 2 + i)) & 1) == 1
		upper_path.push(node)
		bz = bz[32:]
	}
	root = [32]byte{}
	root.copy_from_slice(bz[:32])
	return &ProofPath{
		left_of_twig,
		right_of_twig,
		upper_path,
		serial_num,
		root,
	}, nil
}

func CheckProof(path *ProofPath) ([]byte, error) {
	path.check(false)
	bz = path.to_bytes()
	path2 = bytes_to_proof_path(&bz)
	path2.check(true)
	return bz, nil
}

func GetRightPath(twig *Twig, active_bits *ActiveBits, sn uint64) [3]ProofNode {
	n = sn & TWIG_MASK
	right = [3]ProofNode{}
	self_id = n / 256
	peer = self_id ^ 1
	right[0].self_hash.copy_from_slice(active_bits.get_bits(self_id, 32))
	right[0].peer_hash.copy_from_slice(active_bits.get_bits(peer, 32))
	right[0].peer_at_left = (peer & 1) == 0

	self_ = n / 512
	peer = self_ ^ 1
	right[1].self_hash.copy_from_slice(&twig.active_bits_mtl1[self_])
	right[1].peer_hash.copy_from_slice(&twig.active_bits_mtl1[peer])
	right[1].peer_at_left = (peer & 1) == 0

	self_ = n / 1024
	peer = self_ ^ 1
	right[2].self_hash.copy_from_slice(&twig.active_bits_mtl2[self_])
	right[2].peer_hash.copy_from_slice(&twig.active_bits_mtl2[peer])
	right[2].peer_at_left = (peer & 1) == 0
	right
}

func GetLeftPath(sn uint64, get_hash func(uint64) common.Hash) [11]ProofNode {
	n = sn & TWIG_MASK
	left = [11]ProofNode{}
	for level := range 11 {
		stride = 2048 >> level
		self_id = (n >> level)
		peer = self_id ^ 1
		left[level].self_hash.copy_from_slice(&get_hash(stride + self_id))
		left[level].peer_hash.copy_from_slice(&get_hash(stride + peer))
		left[level].peer_at_left = peer&1 == 0
	}
	return left
}

func GetLeftPathInMem(mt4twig TwigMT, sn uint64) [11]ProofNode {
	return GetLeftPath(sn, func(i uint64) common.Hash { return mt4twig[i] })
}

func GetLeftPathOnDisk(tf TwigStorage, twig_id uint64, sn uint64) [11]ProofNode {
	cache := map[uint64]common.Hash{}
	GetLeftPath(sn, func(i uint64) common.Hash {
		if v, ok := cache[i]; ok {
			return v
		} else {
			return tf.GetHashNode(twig_id, i, cache)
		}
	})
}

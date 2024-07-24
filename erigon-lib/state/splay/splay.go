package splay

import (
	"bytes"
	"fmt"
)

type Node struct {
	left   *Node
	right  *Node
	parent *Node
	Key    Key
	Loc
}

func newNode(l, r *Node, key Key, loc Loc) *Node {
	return &Node{left: l, right: r, Key: key, Loc: loc}
}

func (n *Node) Left() *Node {
	return n.left
}

func (n *Node) Right() *Node {
	return n.right
}

func (n *Node) setParent(p *Node) {
	if n != nil {
		n.parent = p
	}
}

func (n *Node) keepParent() {
	n.left.setParent(n)
	n.right.setParent(n)
}

func (n *Node) rotate(child *Node) {
	gp := n.parent
	if gp != nil {
		if gp.left == n {
			gp.left = child
		} else {
			gp.right = child
		}
	}

	if n.left == child {
		n.left, child.right = child.right, n
	} else {
		n.right, child.left = child.left, n
	}

	child.keepParent()
	n.keepParent()
	child.parent = gp
}

func (n *Node) splay() *Node {
	if n.parent == nil {
		return n
	}

	parent := n.parent
	gparent := parent.parent

	for {
		if gparent == nil { // make zig
			parent.rotate(n)
			return n
		}

		if gparent.left == parent && parent.left == n { // zig-zig
			gparent.rotate(parent)
			parent.rotate(n)
		} else { // zig-zag
			parent.rotate(n)
			gparent.rotate(n)
		}

		if parent = n.parent; parent == nil {
			break
		}
		if gparent = parent.parent; gparent == nil {
			parent.rotate(n)
			break
		}
	}
	return n
}

func (n *Node) find(key Key) *Node {
	if n == nil {
		return nil
	}
	fmt.Printf("V %x %d ", n.Key, n.Di)
	cmp := bytes.Compare(n.Key, key)
	if cmp == 0 {
		return n.splay()
	}

	var curNode *Node
	if cmp > 0 && n.left != nil {
		fmt.Printf("L %x %d\n", n.left.Key, n.left.Di)
		curNode = n.left
	} else if cmp < 0 && n.right != nil {
		fmt.Printf("R %x %d\n", n.right.Key, n.right.Di)
		curNode = n.right
	} else {
		fmt.Printf("S\n")
		return n.splay()
	}

	for {
		cmp = bytes.Compare(curNode.Key, key)
		switch cmp {
		case 1:
			if curNode.left != nil {
				curNode = curNode.left
				continue
			}
		case -1:
			if curNode.right != nil {
				curNode = curNode.right
				continue
			}
		case 0:
		}
		break
	}
	return curNode.splay()
}

type Key []byte

type Tree struct {
	root *Node
}

type Loc struct {
	Offset, Di uint64
}

func NewTree(key Key, loc Loc) *Tree {
	return NewTreeFromNode(newNode(nil, nil, key, loc))
}

func NewTreeFromNode(root *Node) *Tree {
	return &Tree{root: root}
}

func (t *Tree) Insert(key Key, loc Loc) *Node {
	L, R := t.Split(key)
	t.root = newNode(L, R, key, loc)
	t.root.keepParent()
	return t.root
}

func (t *Tree) Remove(key Key) *Node {
	t.root = t.Seek(key)
	t.root.left.setParent(nil)
	t.root.right.setParent(nil)
	el := t.root
	t.root = Merge(t.root.left, t.root.right)
	return el
}

func (t *Tree) Split(key Key) (*Node, *Node) {
	if t.root == nil {
		return nil, nil
	}

	var (
		L, R *Node
		root = t.root.find(key)
	)

	cmp := bytes.Compare(root.Key, key)
	switch cmp {
	case 0: // root.key == key
		root.left.setParent(nil)
		root.right.setParent(nil)
		return root.left, root.right
	case -1: // root.key < key
		R, root.right = root.right, nil
		R.setParent(nil)
		return root, R
	case 1: // root.key > key
		L, root.left = root.left, nil
		L.setParent(nil)
		return L, root
	default:
	}
	return nil, nil
}

func (t *Tree) Seek(key Key) *Node {
	t.root = t.root.find(key)
	return t.root
}

// All keys of L(eft) tree should be lesser than R(ight) keys
func Merge(L, R *Node) *Node {
	if L == nil {
		return R
	}
	if R == nil {
		return L
	}

	R = R.find(L.Key)
	R.left, L.parent = L, R
	return R
}

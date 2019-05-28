package bolt

import (
	"bytes"
	"fmt"
	"sort"
)

// Cursor represents an iterator that can traverse over all key/value pairs in a bucket in sorted order.
// Cursors see nested buckets with value == nil.
// Cursors can be obtained from a transaction and are valid as long as the transaction is open.
//
// Keys and values returned from the cursor are only valid for the life of the transaction.
//
// Changing data while traversing with a cursor may cause it to be invalidated
// and return unexpected keys and/or values. You must reposition your cursor
// after mutating data.
type Cursor struct {
	bucket *Bucket
	stack  []elemRef
	idx    uint64
}

// Bucket returns the bucket that this cursor was created from.
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

// First moves the cursor to the first item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	c.idx = 0
	c.first()

	// If we land on an empty page then move to the next value.
	// https://github.com/boltdb/bolt/issues/450
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v

}

// Last moves the cursor to the last item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Last() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	ref := elemRef{page: p, node: n}
	ref.index = ref.count() - 1
	if c.bucket.enum {
		var idx uint64
		e := elemRef{page: p, node: n}
		for i, cnt := 0, ref.count(); i < cnt; i++ {
			e.index = i
			idx += e.size()
		}
		c.idx = idx - 1
	}
	c.stack = append(c.stack, ref)
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
// If the cursor is at the end of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Prev moves the cursor to the previous item in the bucket and returns its key and value.
// If the cursor is at the beginning of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Prev() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// Attempt to move back one element until we're successful.
	// Move up the stack as we hit the beginning of each page in our stack.
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 {
			elem.index--
			break
		}
		c.stack = c.stack[:i]
	}

	// If we've hit the end then return nil.
	if len(c.stack) == 0 {
		return nil, nil
	}

	// Move down the stack to find the last element of the last leaf under this branch.
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used. If no keys
// follow, a nil key is returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	k, v, flags := c.seek(seek)

	// If we ended up after the last element of a page then move to the next one.
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) SeekTo(seek []byte) (key []byte, value []byte) {
	k, v, flags := c.seekTo(seek)

	// If we ended up after the last element of a page then move to the next one.
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Delete removes the current key/value under the cursor from the bucket.
// Delete fails if current key/value is a bucket or if the transaction is not writable.
func (c *Cursor) Delete() error {
	if c.bucket.tx.db == nil {
		return ErrTxClosed
	} else if !c.bucket.Writable() {
		return ErrTxNotWritable
	}

	key, _, flags := c.keyValue()
	// Return an error if current value is a bucket.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	c.node().del(key)

	return nil
}

// seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used.
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// Start from root page/node and traverse to correct page.
	c.stack = c.stack[:0]
	c.idx = 0
	c.search(seek, c.bucket.root)
	ref := &c.stack[len(c.stack)-1]

	// If the cursor is pointing to the end of page/node then return nil.
	if ref.index >= ref.count() {
		return nil, nil, 0
	}

	// If this is a bucket then return a nil value.
	return c.keyValue()
}

// seekTo moves the cursor from the current position to a given key and return it
// This is different from seek that always start seeking from the root
func (c *Cursor) seekTo(seek []byte) (key []byte, value []byte, flags uint32) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// Move up the stack until we find the level at which we need to move forward
	var i int
	var pageid pgid
	for i = len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		var lastkey []byte
		if elem.node != nil {
			n := elem.node
			pageid = n.pgid
			if len(n.inodes) > 0 {
				lastkey = n.inodes[len(n.inodes)-1].key
			} else {
				//fmt.Printf("len(n.inodes) == 0 %x\n", seek)
			}
		} else {
			p := elem.page
			if p.count == 0 {
				break
			}
			pageid = p.id
			if elem.isLeaf() {
				lastkey = append(p.keyPrefix(), p.leafPageElement(p.count-1).key()...)
			} else {
				if c.bucket.enum {
					lastkey = append(p.keyPrefix(), p.branchPageElementX(p.count-1).key()...)
				} else {
					lastkey = append(p.keyPrefix(), p.branchPageElement(p.count-1).key()...)
				}
			}
		}
		if bytes.Compare(seek, lastkey) <= 0 {
			break
		} else {
			if c.bucket.enum {
				var idx uint64
				e := elemRef{node: elem.node, page: elem.page}
				for i, cnt := elem.index, e.count(); i < cnt; i++ {
					e.index = i
					idx += e.size()
				}
				c.idx += idx
			}
		}
	}

	// If we've hit the root page then start with the root
	if i == -1 {
		i = 0
		pageid = c.bucket.root
	}

	// Trim the stack
	c.stack = c.stack[:i]

	// Now search within the current node or page and descend to the desired node
	c.search(seek, pageid)

	ref := &c.stack[len(c.stack)-1]

	// If the cursor is pointing to the end of page/node then return nil.
	if ref.index >= ref.count() {
		return nil, nil, 0
	}

	return c.keyValue()
}

// first moves the cursor to the first leaf element under the last page in the stack.
func (c *Cursor) first() {
	for {
		// Exit when we hit a leaf page.
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the first element to the stack.
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			if c.bucket.enum {
				pgid = ref.page.branchPageElementX(uint16(ref.index)).pgid
			} else {
				pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
			}
		}
		p, n := c.bucket.pageNode(pgid)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

// last moves the cursor to the last leaf element under the last page in the stack.
func (c *Cursor) last() {
	for {
		// Exit when we hit a leaf page.
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the last element in the stack.
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			if c.bucket.enum {
				pgid = ref.page.branchPageElementX(uint16(ref.index)).pgid
			} else {
				pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
			}
		}
		p, n := c.bucket.pageNode(pgid)

		var nextRef = elemRef{page: p, node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

// next moves to the next leaf element and returns the key and value.
// If the cursor is at the last leaf element then it stays there and returns nil.
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// Attempt to move over one element until we're successful.
		// Move up the stack as we hit the end of each page in our stack.
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 {
				elem.index++
				c.idx++
				break
			}
		}

		// If we've hit the root page then stop and return. This will leave the
		// cursor on the last element of the last page.
		if i == -1 {
			return nil, nil, 0
		}

		// Otherwise start from where we left off in the stack and find the
		// first element of the first leaf page.
		c.stack = c.stack[:i+1]
		c.first()

		// If this is an empty page then restart and move back up the stack.
		// https://github.com/boltdb/bolt/issues/450
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		return c.keyValue()
	}
}

// search recursively performs a binary search against a given page/node until it finds a given key.
func (c *Cursor) search(key []byte, pgid pgid) {
	var p *page
	var n *node
	var e *elemRef
	newElement := true
	l := len(c.stack)
	if l > 0 {
		e = &c.stack[l-1]
		p = e.page
		n = e.node
		if p != nil && p.id == pgid {
			newElement = false
		} else if n != nil && n.pgid == pgid {
			newElement = false
		}
	}
	if newElement {
		p, n = c.bucket.pageNode(pgid)
		if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
			panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
		}

		e = &elemRef{page: p, node: n}
		c.stack = append(c.stack, *e)
	}

	// If we're on a leaf page/node then find the specific node.
	if e.isLeaf() {
		c.nsearch(key)
		return
	}

	if n != nil {
		c.searchNode(key, n)
		return
	}
	c.searchPage(key, p)
}

func (c *Cursor) searchNode(key []byte, n *node) {
	offset := c.stack[len(c.stack)-1].index
	count := len(n.inodes) - offset
	index := offset + (count - 1) - sort.Search(count, func(i int) bool {
		return bytes.Compare(n.inodes[offset+(count-1)-i].key, key) != 1
	})
	if index < offset {
		index = offset
	}
	if c.bucket.enum {
		var idx uint64
		e := elemRef{node: n}
		for i, cnt := offset, index; i < cnt; i++ {
			e.index = i
			idx += e.size()
		}
		c.idx += idx
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.search(key, n.inodes[index].pgid)
}

func (c *Cursor) searchPage(key []byte, p *page) {
	// Binary search for the correct range.
	var inodes []branchPageElement
	var inodesX []branchPageElementX
	if c.bucket.enum {
		inodesX = p.branchPageElementsX()
	} else {
		inodes = p.branchPageElements()
	}
	pagePrefix := p.keyPrefix()
	keyPrefix := key
	if len(key) > len(pagePrefix) {
		keyPrefix = key[:len(pagePrefix)]
	}
	offset := c.stack[len(c.stack)-1].index
	count := int(p.count) - offset
	var index int
	switch bytes.Compare(pagePrefix, keyPrefix) {
	case -1:
		index = offset + count - 1
	case 1:
		index = offset - 1
	case 0:
		shortKey := key[len(pagePrefix):]
		index = offset + (count - 1) - sort.Search(count, func(i int) bool {
			if c.bucket.enum {
				return bytes.Compare(inodesX[offset+(count-1)-i].key(), shortKey) != 1
			} else {
				return bytes.Compare(inodes[offset+(count-1)-i].key(), shortKey) != 1
			}
		})
	}
	if index < offset {
		index = offset
	}
	if c.bucket.enum {
		var idx uint64
		e := elemRef{page: p}
		for i, cnt := offset, index; i < cnt; i++ {
			e.index = i
			idx += e.size()
		}
		c.idx += idx
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	if c.bucket.enum {
		c.search(key, inodesX[index].pgid)
	} else {
		c.search(key, inodes[index].pgid)
	}
}

// nsearch searches the leaf node on the top of the stack for a key.
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node
	offset := e.index

	// If we have a node then search its inodes.
	if n != nil {
		count := len(n.inodes) - offset
		index := sort.Search(count, func(i int) bool {
			return bytes.Compare(n.inodes[offset+i].key, key) != -1
		})
		e.index = offset + index
		if c.bucket.enum {
			c.idx += uint64(index)
		}
		return
	}

	// If we have a page then search its leaf elements.
	inodes := p.leafPageElements()
	pagePrefix := p.keyPrefix()
	keyPrefix := key
	if len(key) > len(pagePrefix) {
		keyPrefix = key[:len(pagePrefix)]
	}
	switch bytes.Compare(pagePrefix, keyPrefix) {
	case -1:
		e.index = int(p.count)
		if c.bucket.enum {
			c.idx += uint64(e.index - offset)
		}
	case 1:
		// e.index does not change
	case 0:
		shortKey := key[len(pagePrefix):]
		count := int(p.count) - offset
		index := sort.Search(count, func(i int) bool {
			return bytes.Compare(inodes[offset+i].key(), shortKey) != -1
		})
		e.index = offset + index
		if c.bucket.enum {
			c.idx += uint64(index)
		}
	}
}

// keyValue returns the key and value of the current leaf element.
func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	ref := &c.stack[len(c.stack)-1]
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// Retrieve value from node.
	if ref.node != nil {
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}

	// Or retrieve value from page.
	elem := ref.page.leafPageElement(uint16(ref.index))
	return append(ref.page.keyPrefix(), elem.key()...), elem.value(), elem.flags
}

// node returns the node that the cursor is currently positioned on.
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// If the top of the stack is a leaf node then just return it.
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	// Start from root and traverse down the hierarchy.
	var n = c.stack[0].node
	if n == nil {
		n = c.bucket.node(c.stack[0].page.id, nil)
	}
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		n = n.childAt(int(ref.index))
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// elemRef represents a reference to an element on a given page/node.
type elemRef struct {
	page  *page
	node  *node
	index int
}

// isLeaf returns whether the ref is pointing at a leaf page/node.
func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

// count returns the number of inodes or page elements.
func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}

// Assumes that the enum is turned on
func (r *elemRef) size() uint64 {
	if r.node != nil {
		return r.node.inodes[r.index].size
	}
	if (r.page.flags & branchPageFlag) == 0 {
		return 1
	}
	return uint64(r.page.branchPageElementX(uint16(r.index)).size) + r.page.minsize
}

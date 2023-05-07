package solid

import (
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type bitlist struct {
	u []byte
	c int
}

func NewBitList(l int, c int) BitList {
	return &bitlist{
		u: make([]byte, l),
		c: c,
	}
}
func BitlistFromBytes(xs []byte, c int) BitList {
	return &bitlist{
		u: xs,
		c: c,
	}
}

func (u *bitlist) Clear() {
	u.u = u.u[:0]
}

func (u *bitlist) CopyTo(target BitList) {
	target.Clear()
	for _, v := range u.u {
		target.Append(v)
	}
}

func (u *bitlist) Range(fn func(index int, value byte, length int) bool) {
	for i, v := range u.u {
		fn(i, v, len(u.u))
	}
}

func (u *bitlist) Pop() (x byte) {
	x, u.u = u.u[0], u.u[1:]
	return x
}

func (u *bitlist) Append(v byte) {
	u.u = append(u.u, v)
}

func (u *bitlist) Get(index int) byte {
	return u.u[index]
}

func (u *bitlist) Set(index int, v byte) {
	u.u[index] = v
}

func (u *bitlist) Length() int {
	return len(u.u)
}

func (u *bitlist) Cap() int {
	return u.c
}

func (u *bitlist) HashSSZTo(xs []byte) error {
	root, err := merkle_tree.BitlistRootWithLimitForState(u.u, uint64(u.c))
	if err != nil {
		return err
	}
	copy(xs, root[:])
	return nil
}

func (u *bitlist) EncodeSSZ(dst []byte) []byte {
	buf := dst
	buf = append(buf, u.u[:]...)
	return buf
}

package solid

import (
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type uint64SliceSimple struct {
	u []uint64
	c int
}

func NewSimpleUint64Slice(cap int) Uint64Slice {
	return &uint64SliceSimple{
		c: cap,
	}
}

func (u *uint64SliceSimple) Clear() {
	u.u = u.u[:0]
}

func (u *uint64SliceSimple) CopyTo(target Uint64Slice) {
	target.Clear()
	for _, v := range u.u {
		target.Append(v)
	}
}

func (u *uint64SliceSimple) Range(fn func(index int, value uint64, length int) bool) {
	//TODO implement me
	for i, v := range u.u {
		fn(i, v, len(u.u))
	}
}

func (u *uint64SliceSimple) Pop() (x uint64) {
	x, u.u = u.u[0], u.u[1:]
	return x
}

func (u *uint64SliceSimple) Append(v uint64) {
	u.u = append(u.u, v)
}

func (u *uint64SliceSimple) Get(index int) uint64 {
	return u.u[index]
}

func (u *uint64SliceSimple) Set(index int, v uint64) {
	u.u[index] = v
}

func (u *uint64SliceSimple) Length() int {
	return len(u.u)
}

func (u *uint64SliceSimple) Cap() int {
	return u.c
}

func (u *uint64SliceSimple) HashSSZTo(xs []byte) error {
	root, err := merkle_tree.Uint64ListRootWithLimit(u.u, (uint64(u.c)*8+31)/32)
	if err != nil {
		return err
	}
	copy(xs, root[:])
	return nil
}

package testhelpers

import (
	"encoding/binary"

	"github.com/erigontech/erigon/txnprovider/shutter"
)

func MockIdentityPreimages(count int) []shutter.IdentityPreimage {
	ips := make([]shutter.IdentityPreimage, count)
	for i := 0; i < count; i++ {
		ips[i] = Uint64ToIdentityPreimage(uint64(i))
	}
	return ips
}

func Uint64ToIdentityPreimage(i uint64) shutter.IdentityPreimage {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

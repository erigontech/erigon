package u256

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
)

func TestRightPadding(t *testing.T) {
	bs := []byte{0, 1, 0x10, 0x11, 0x20, 0xfa, 0xff}

	for n := 1; n <= 4; n++ {
		for l := n; l <= 4; l++ {
			slice := make([]byte, n)
			for _, b := range bs {
				for i := range slice {
					slice[i] = b

					withPadding := setUint256WithPadding(slice, l)
					withoutPadding := setUint256(slice, l)
					if withoutPadding.Cmp(withPadding) != 0 {
						t.Fatalf("not equal n=%d l=%d slice=%v\nGot\t\t%v\n!=\nExpect\t%v(%v)",
							n, l, slice, withoutPadding.Bytes32(), withPadding.Bytes32(), spew.Sdump(withPadding))
					}
				}
			}
		}
	}
}

func setUint256WithPadding(slice []byte, l int) *uint256.Int {
	return new(uint256.Int).SetBytes(common.RightPadBytes(slice, l))
}

func setUint256(slice []byte, l int) *uint256.Int {
	return SetBytesRightPadded(new(uint256.Int), slice, l)
}

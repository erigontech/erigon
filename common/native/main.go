package native

import (
	"encoding/binary"
	"unsafe"
)

var Endian binary.ByteOrder
var bigEndian bool

func IsBigEndian() bool {
	return bigEndian
}

func IsLittleEndian() bool {
	return !bigEndian
}

func init() {
	if getEndian() {
		Endian = binary.BigEndian
		bigEndian = true
	} else {
		Endian = binary.LittleEndian
		bigEndian = false
	}
}

const INT_SIZE int = int(unsafe.Sizeof(0))

//true = big endian, false = little endian
func getEndian() (ret bool) {
	var i int = 0x1
	bs := (*[INT_SIZE]byte)(unsafe.Pointer(&i))
	if bs[0] == 0 {
		return true
	} else {
		return false
	}

}

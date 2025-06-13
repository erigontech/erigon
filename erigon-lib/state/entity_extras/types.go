package entity_extras

/** custom types **/

// canonical sequence number of entity (in context)
type Num uint64

// sequence number of entity - might contain non-canonical values
type Id uint64

// canonical sequence number of the root entity (or secondary key)
type RootNum uint64

type Bytes = []byte

type EncToBytesI interface {
	EncToBytes(enc8Bytes bool) []byte
}

func (n Num) Step(a ForkableId) uint64 {
	return step(n, a)
}

func (n Num) Uint64() uint64 {
	return uint64(n)
}

func (n RootNum) Step(a ForkableId) uint64 {
	return step(n, a)
}

func (x Id) EncToBytes(x8Bytes bool) (out []byte) {
	return EncToBytes(x, x8Bytes)
}
func (x Id) EncTo8Bytes() (out []byte) {
	return EncToBytes(x, true)
}
func (x Num) EncToBytes(x8Bytes bool) (out []byte) {
	return EncToBytes(x, x8Bytes)
}
func (x Num) EncTo8Bytes() (out []byte) {
	return EncToBytes(x, true)
}
func (x RootNum) EncTo8Bytes() (out []byte) {
	return EncToBytes(x, true)
}

func (x RootNum) Uint64() uint64 {
	return uint64(x)
}

// file extensions

type AccessorExtension string

const (
	AccessorExtensionIdx AccessorExtension = ".idx"
	AccessorExtensionKvi AccessorExtension = ".kvi"
	AccessorExtensionVi  AccessorExtension = ".vi"
	AccessorExtensionEfi AccessorExtension = ".efi"
)

func (a AccessorExtension) IsSet() bool {
	switch a {
	case AccessorExtensionIdx, AccessorExtensionKvi, AccessorExtensionVi, AccessorExtensionEfi:
		return true
	}
	return false
}

func (a AccessorExtension) String() string {
	return string(a)
}

func (a AccessorExtension) Equals(target string) bool {
	return string(a) == target
}

type DataExtension string

const (
	DataExtensionSeg DataExtension = ".seg"
	DataExtensionKv  DataExtension = ".kv"
	DataExtensionV   DataExtension = ".v"
	DataExtensionEf  DataExtension = ".ef"
)

func (d DataExtension) IsSet() bool {
	switch d {
	case DataExtensionSeg, DataExtensionKv, DataExtensionV, DataExtensionEf:
		return true
	}
	return false
}

func (d DataExtension) String() string {
	return string(d)
}

func (d DataExtension) Equals(target string) bool {
	return string(d) == target
}

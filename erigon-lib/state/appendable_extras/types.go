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

func (n Num) Step(a AppendableId) uint64 {
	return step(n, a)
}

func (n RootNum) Step(a AppendableId) uint64 {
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

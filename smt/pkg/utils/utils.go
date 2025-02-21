package utils

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	poseidon "github.com/okx/poseidongold/go"
	"golang.org/x/exp/slices"
)

func init() {
	const hashPoseidonAllZeroes = "0xc71603f33a1144ca7953db0ab48808f4c4055e3364a246c33c18a9786cb0b359"
	var err error
	PoseidonAllZeroesHash, err = StringToH4(hashPoseidonAllZeroes)
	if err != nil {
		panic(err)
	}
}

const (
	KEY_BALANCE            = 0
	KEY_NONCE              = 1
	SC_CODE                = 2
	SC_STORAGE             = 3
	SC_LENGTH              = 4
	BYTECODE_ELEMENTS_HASH = 8
	BYTECODE_BYTES_ELEMENT = 7
)

var (
	PoseidonAllZeroesHash [4]uint64
)

type NodeValue8 [8]uint64
type NodeValue12 [12]uint64
type NodeKey [4]uint64

type NodeType int

const (
	LeafNode   NodeType = iota // 0 for leaf node
	BranchNode                 // 1 for branch node
)

type Side int

const (
	Left  Side = iota // 0 for left
	Right             // 1 for right
)

var (
	LeafCapacity   = [4]uint64{1, 0, 0, 0}
	BranchCapacity = [4]uint64{0, 0, 0, 0}
	hashFunc       = poseidon.HashWithResult
)

func Hash(in [8]uint64, capacity [4]uint64) [4]uint64 {
	var result [4]uint64 = [4]uint64{0, 0, 0, 0}
	hashFunc(&in, &capacity, &result)
	return result
}

func HashByPointers(in *[8]uint64, capacity *[4]uint64) *[4]uint64 {
	var result [4]uint64 = [4]uint64{0, 0, 0, 0}
	hashFunc(in, capacity, &result)
	return &result
}

func (nk *NodeKey) IsZero() bool {
	return nk[0] == 0 && nk[1] == 0 && nk[2] == 0 && nk[3] == 0
}

func (nk *NodeKey) IsEqualTo(nk2 NodeKey) bool {
	return nk[0] == nk2[0] && nk[1] == nk2[1] && nk[2] == nk2[2] && nk[3] == nk2[3]
}

func (nk *NodeKey) ToBigInt() *big.Int {
	return ArrayToScalar(nk[:])
}

func (nk *NodeKey) AsUint64Pointer() *[4]uint64 {
	return (*[4]uint64)(nk)
}

func (nk *NodeKey) ToHex() string {
	buf := make([]byte, 32) // 4 uint64s * 8 bytes each = 32 bytes
	binary.BigEndian.PutUint64(buf[0:8], nk[0])
	binary.BigEndian.PutUint64(buf[8:16], nk[1])
	binary.BigEndian.PutUint64(buf[16:24], nk[2])
	binary.BigEndian.PutUint64(buf[24:32], nk[3])
	return hex.EncodeToString(buf)
}

func (nv *NodeValue8) IsZero() bool {
	if nv == nil {
		return true
	}

	for i := 0; i < 8; i++ {
		if nv[i] != 0 {
			return false
		}
	}

	return true
}

// part = 0 for first 4 values, 1 for the last 4 values
func (nv *NodeValue8) SetHalfValue(values [4]uint64, part int) error {
	if part < 0 || part > 1 {
		return fmt.Errorf("part must be 0 or 1")
	}

	partI := part * 4
	for i, v := range values {
		nv[i+partI] = v
	}

	return nil
}

func (nv *NodeValue8) ToUintArray() [8]uint64 {
	var result [8]uint64

	if nv != nil {
		for i := 0; i < 8; i++ {
			result[i] = nv[i]
		}
	}
	// if nv is nil, result will be an array of 8 zeros

	return result
}

func (nv *NodeValue8) ToUintArrayByPointer() *[8]uint64 {
	var result [8]uint64

	if nv != nil {
		for i := 0; i < 8; i++ {
			result[i] = nv[i]
		}
	}
	// if nv is nil, result will be an array of 8 zeros

	return &result
}

func (nv *NodeValue8) ToHex() string {
	bytes := make([]byte, 64)
	for i := 0; i < 8; i++ {
		// Write in reverse order: start from the end and work backwards
		binary.BigEndian.PutUint64(bytes[(7-i)*8:], nv[i])
	}
	return hex.EncodeToString(bytes)
}

func (nv *NodeValue12) StripCapacity() [8]uint64 {
	return [8]uint64{nv[0], nv[1], nv[2], nv[3], nv[4], nv[5], nv[6], nv[7]}
}

func (nv *NodeValue12) Get0to4() *NodeKey {
	// slice it 0-4
	return &NodeKey{nv[0], nv[1], nv[2], nv[3]}
}

func (nv *NodeValue12) Get4to8() *NodeKey {
	// slice it 4-8
	return &NodeKey{nv[4], nv[5], nv[6], nv[7]}
}

func (nv *NodeValue12) GetNodeValue8() *NodeValue8 {
	return &NodeValue8{nv[0], nv[1], nv[2], nv[3], nv[4], nv[5], nv[6], nv[7]}
}

func (nv *NodeValue12) Get0to8() [8]uint64 {
	// slice it from 0-8
	return [8]uint64{nv[0], nv[1], nv[2], nv[3], nv[4], nv[5], nv[6], nv[7]}
}

func (nv *NodeValue12) IsUniqueSibling() (int, error) {
	count := 0
	fnd := 0
	a := nv[:]

	for i := 0; i < len(a); i += 4 {
		k := NodeKeyFromUint64Array(a[i : i+4])
		if !k.IsZero() {
			count++
			fnd = i / 4
		}
	}
	if count == 1 {
		return fnd, nil
	}
	return -1, nil
}

func (nv *NodeValue12) ToHex() string {
	bytes := make([]byte, 96)
	for i := 0; i < 12; i++ {
		// Write in reverse order: start from the end and work backwards
		binary.BigEndian.PutUint64(bytes[(11-i)*8:], nv[i])
	}
	return hex.EncodeToString(bytes)
}

func NodeKeyFromBigIntArray(arr []*big.Int) NodeKey {
	nk := NodeKey{}
	for i, v := range arr {
		if v != nil {
			nk[i] = v.Uint64()
		} else {
			nk[i] = 0
		}
	}
	return nk
}

func NodeKeyFromUint64Array(arr []uint64) NodeKey {
	return NodeKey{arr[0], arr[1], arr[2], arr[3]}
}

func IsArrayUint64Empty(arr []uint64) bool {
	for _, v := range arr {
		if v > 0 {
			return false
		}
	}

	return true
}

func Value8FromBigIntArray(arr []*big.Int) NodeValue8 {
	nv := [8]uint64{}
	for i, v := range arr {
		nv[i] = v.Uint64()
	}
	return nv
}

func Value8FromUint64Array(arr []uint64) NodeValue8 {
	nv := [8]uint64{}
	copy(nv[:], arr[:8])
	return nv
}

func NodeValue12FromBigIntArray(arr []*big.Int) (*NodeValue12, error) {
	if len(arr) != 12 {
		return &NodeValue12{}, fmt.Errorf("invalid array length")
	}
	nv := NodeValue12{}
	for i, v := range arr {
		nv[i] = v.Uint64()
	}
	return &nv, nil
}

func NodeValue8FromBigInt(value *big.Int) (*NodeValue8, error) {
	x := ScalarToArrayBig(value)
	return NodeValue8FromBigIntArray(x)
}

func NodeValue8ToBigInt(value *NodeValue8) *big.Int {
	x := BigIntArrayFromNodeValue8(value)
	return ArrayBigToScalar(x)
}

func NodeValue8FromBigIntArray(arr []*big.Int) (*NodeValue8, error) {
	if len(arr) != 8 {
		return &NodeValue8{}, fmt.Errorf("invalid array length")
	}
	nv := NodeValue8{}
	for i, v := range arr {
		nv[i] = v.Uint64()
	}
	return &nv, nil
}

func BigIntArrayFromNodeValue8(nv *NodeValue8) []*big.Int {
	arr := make([]*big.Int, 8)
	for i, v := range nv {
		arr[i] = big.NewInt(int64(v))
	}
	return arr
}

func (nv *NodeValue12) IsZero() bool {
	zero := false
	for _, v := range nv {
		if v == 0 {
			zero = true
		} else {
			zero = false
			break
		}
	}
	return zero
}

func (nv *NodeValue12) IsFinalNode() bool {
	if nv[8] == 0 {
		return false
	}
	return nv[8] == 1
}

// 7 times more efficient than sprintf
func ConvertBigIntToHex(n *big.Int) string {
	return "0x" + n.Text(16)
}

func ConvertUint64ToHex(n uint64) string {
	return "0x" + strconv.FormatUint(n, 16)
}

func ConvertArrayToHex(arr []uint64) string {
	hexVal := hex.EncodeToString(ArrayToBytes(arr))
	// big int .Text function trims leading zeros from the hex string
	hexVal = strings.TrimPrefix(hexVal, "0")
	return "0x" + hexVal
}

func ConvertHexToBigInt(hex string) *big.Int {
	hex = strings.TrimPrefix(hex, "0x")
	n, _ := new(big.Int).SetString(hex, 16)
	return n
}

func ConvertHexToUint64(hex string) (uint64, error) {
	hex = strings.TrimPrefix(hex, "0x")
	return strconv.ParseUint(hex, 16, 64)
}

// ConvertHexToUint64s converts a hex string into an array of uint64s - designed to offer the same functionality
// as working with big ints but without the allocation headache
func ConvertHexToUint64Array(hexStr string) ([8]uint64, error) {
	// Remove 0x prefix if present
	hexStr = strings.TrimPrefix(hexStr, "0x")

	if len(hexStr)%2 != 0 {
		hexStr = "0" + hexStr // Pad with leading zero if odd length
	}

	// Convert hex string to bytes
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return [8]uint64{}, fmt.Errorf("invalid hex string: %v", err)
	}

	// Pad to 32 bytes (256 bits)
	paddedBytes := make([]byte, 32)
	copy(paddedBytes[len(paddedBytes)-len(bytes):], bytes) // Right-align the bytes

	// Convert to uint64s, taking 32 bits at a time
	result := [8]uint64{
		uint64(binary.BigEndian.Uint32(paddedBytes[28:32])), // r0: lowest 32 bits
		uint64(binary.BigEndian.Uint32(paddedBytes[24:28])), // r1: bits 32-63
		uint64(binary.BigEndian.Uint32(paddedBytes[20:24])), // r2: bits 64-95
		uint64(binary.BigEndian.Uint32(paddedBytes[16:20])), // r3: bits 96-127
		uint64(binary.BigEndian.Uint32(paddedBytes[12:16])), // r4: bits 128-159
		uint64(binary.BigEndian.Uint32(paddedBytes[8:12])),  // r5: bits 160-191
		uint64(binary.BigEndian.Uint32(paddedBytes[4:8])),   // r6: bits 192-223
		uint64(binary.BigEndian.Uint32(paddedBytes[0:4])),   // r7: bits 224-255
	}

	return result, nil
}

func ArrayToScalar(array []uint64) *big.Int {
	scalar := new(big.Int)
	for i := len(array) - 1; i >= 0; i-- {
		scalar.Lsh(scalar, 64)
		scalar.Add(scalar, new(big.Int).SetUint64(array[i]))
	}
	return scalar
}

// ArrayToBytes converts an array of uint64s to a byte array, matching the behavior of big.Int.Bytes()
// and avoids the allocation headache of big.Int
func ArrayToBytes(array []uint64) []byte {
	// Each uint64 needs 8 bytes
	buf := make([]byte, len(array)*8)

	// Convert each uint64 to bytes in the same order as ArrayToScalar
	for i := 0; i < len(array); i++ {
		pos := (len(array) - 1 - i) * 8
		binary.BigEndian.PutUint64(buf[pos:pos+8], array[i])
	}

	// Trim leading zeros to match big.Int.Bytes() behavior
	for i := 0; i < len(buf); i++ {
		if buf[i] != 0 {
			return buf[i:]
		}
	}

	// If all bytes are zero, return a single zero byte
	// to match big.Int.Bytes() behavior
	if len(buf) > 0 {
		return []byte{0}
	}

	return buf
}

func ScalarToArray(scalar *big.Int) []uint64 {
	scalar = new(big.Int).Set(scalar)
	mask := new(big.Int)
	mask.SetString("FFFFFFFFFFFFFFFF", 16)

	r0 := new(big.Int).And(scalar, mask)

	r1 := new(big.Int).Rsh(scalar, 64)
	r1 = new(big.Int).And(r1, mask)

	r2 := new(big.Int).Rsh(scalar, 128)
	r2 = new(big.Int).And(r2, mask)

	r3 := new(big.Int).Rsh(scalar, 192)
	r3 = new(big.Int).And(r3, mask)

	return []uint64{r0.Uint64(), r1.Uint64(), r2.Uint64(), r3.Uint64()}
}

func ScalarToNodeKey(s *big.Int) NodeKey {
	auxk := make([]*big.Int, 4)
	for i := range auxk {
		auxk[i] = big.NewInt(0)
	}

	r := new(big.Int).Set(s)
	i := big.NewInt(0)
	one := big.NewInt(1)

	for r.BitLen() > 0 {
		if new(big.Int).And(r, one).BitLen() > 0 {
			auxk[new(big.Int).Mod(i, big.NewInt(4)).Int64()] = new(big.Int).Add(
				auxk[new(big.Int).Mod(i, big.NewInt(4)).Int64()],
				new(big.Int).Lsh(one, uint(new(big.Int).Div(i, big.NewInt(4)).Uint64())),
			)
		}
		r = r.Rsh(r, 1)
		i.Add(i, one)
	}

	return NodeKey{
		auxk[0].Uint64(),
		auxk[1].Uint64(),
		auxk[2].Uint64(),
		auxk[3].Uint64(),
	}
}

func ScalarToRoot(s *big.Int) NodeKey {
	var result [4]uint64
	divisor := new(big.Int).Exp(big.NewInt(2), big.NewInt(64), nil)

	sCopy := new(big.Int).Set(s)

	for i := 0; i < 4; i++ {
		mod := new(big.Int).Mod(sCopy, divisor)
		result[i] = mod.Uint64()
		sCopy.Div(sCopy, divisor)
	}
	return result
}

func ScalarToNodeValue(scalarIn *big.Int) NodeValue12 {
	out := [12]uint64{}
	mask := new(big.Int).SetBytes([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	scalar := new(big.Int).Set(scalarIn)

	for i := 0; i < 12; i++ {
		value := new(big.Int).And(scalar, mask)
		out[i] = value.Uint64()
		scalar.Rsh(scalar, 64)
	}
	return out
}

func ScalarToNodeValue8(scalarIn *big.Int) NodeValue8 {
	out := [8]uint64{}
	mask := new(big.Int).SetBytes([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	scalar := new(big.Int).Set(scalarIn)

	for i := 0; i < 8; i++ {
		value := new(big.Int).And(scalar, mask)
		out[i] = value.Uint64()
		scalar.Rsh(scalar, 64)
	}
	return out
}

func (nk *NodeKey) GetPath() []int {
	res := make([]int, 0, 256)
	auxk := [4]uint64{nk[0], nk[1], nk[2], nk[3]}

	for j := 0; j < 64; j++ {
		for i := 0; i < 4; i++ {
			res = append(res, int(auxk[i]&1)) // Append the LSB of the current part to res
			auxk[i] >>= 1                     // Right shift the current part
		}
	}

	return res
}

func NodeKeyFromPath(path []int) (NodeKey, error) {
	if len(path) != 256 {
		return NodeKey{}, fmt.Errorf("path is not 256 bits")
	}

	res := [4]uint64{0, 0, 0, 0}

	for j := 0; j < 256; j++ {
		i := j % 4

		k := j / 4
		res[i] |= uint64(path[j]) << k
	}

	return res, nil
}

func BinaryKey(key NodeKey) string {
	return fmt.Sprintf("%064b%064b%064b%064b", key[0], key[1], key[2], key[3])
}

func ConcatArrays4(a, b [4]uint64) [8]uint64 {
	result := [8]uint64{}

	copy(result[:], a[:])

	for i, v := range b {
		result[i+4] = v
	}
	return result
}

func ConcatArrays4ByPointers(a, b *[4]uint64) *[8]uint64 {
	return &[8]uint64{
		a[0], a[1], a[2], a[3],
		b[0], b[1], b[2], b[3],
	}
}

func ConcatArrays8AndCapacityByPointers(in *[8]uint64, capacity *[4]uint64) *NodeValue12 {
	v := NodeValue12{}
	for i, val := range in {
		v[i] = val
	}
	for i, val := range capacity {
		v[i+8] = val
	}

	return &v
}

func HashKeyAndValueByPointers(in *[8]uint64, capacity *[4]uint64) (*[4]uint64, *NodeValue12) {
	h := HashByPointers(in, capacity)
	return h, ConcatArrays8AndCapacityByPointers(in, capacity)
}

func RemoveKeyBits(k NodeKey, nBits int) NodeKey {
	var auxk NodeKey
	fullLevels := nBits / 4

	for i := 0; i < 4; i++ {
		n := fullLevels
		if fullLevels*4+i < nBits {
			n += 1
		}
		auxk[i] = k[i] >> uint(n)
	}

	return auxk
}

var mask = big.NewInt(4294967295)

func ScalarToArrayBig(scalar *big.Int) []*big.Int {
	r0 := new(big.Int).And(scalar, mask)

	r1 := new(big.Int).Rsh(scalar, 32)
	r1.And(r1, mask)

	r2 := new(big.Int).Rsh(scalar, 64)
	r2.And(r2, mask)

	r3 := new(big.Int).Rsh(scalar, 96)
	r3.And(r3, mask)

	r4 := new(big.Int).Rsh(scalar, 128)
	r4.And(r4, mask)

	r5 := new(big.Int).Rsh(scalar, 160)
	r5.And(r5, mask)

	r6 := new(big.Int).Rsh(scalar, 192)
	r6.And(r6, mask)

	r7 := new(big.Int).Rsh(scalar, 224)
	r7.And(r7, mask)

	return []*big.Int{r0, r1, r2, r3, r4, r5, r6, r7}
}

func ScalarToArrayUint64(scalar *big.Int) ([8]uint64, error) {
	return ConvertHexToUint64Array(scalar.Text(16))
}

func ArrayBigToScalar(arr []*big.Int) *big.Int {
	scalar := new(big.Int)
	for i := len(arr) - 1; i >= 0; i-- {
		scalar.Lsh(scalar, 32)
		scalar.Add(scalar, arr[i])
	}
	return scalar
}

func JoinKey(usedBits []int, remainingKey NodeKey) *NodeKey {
	n := make([]uint64, 4)
	accs := make([]uint64, 4)

	for i := 0; i < len(usedBits); i++ {
		if usedBits[i] == 1 {
			accs[i%4] = accs[i%4] | (1 << n[i%4])
		}
		n[i%4]++
	}

	auxk := make([]uint64, 4)
	for i := 0; i < 4; i++ {
		auxk[i] = remainingKey[i]<<n[i] | accs[i]
	}

	return &NodeKey{auxk[0], auxk[1], auxk[2], auxk[3]}
}

func RemoveOver(m map[int]*NodeValue12, level int) {
	for k := range m {
		if k >= level {
			delete(m, k)
		}
	}
}

func StringToH4(s string) ([4]uint64, error) {
	if !strings.HasPrefix(s, "0x") {
		return [4]uint64{}, errors.New("hexadecimal required")
	}

	if len(s) != 66 {
		return [4]uint64{}, errors.New("hexadecimal all digits required")
	}

	var res [4]uint64
	var err error

	for i := 0; i < 4; i++ {
		res[3-i], err = strconv.ParseUint(s[2+(16*i):18+(16*i)], 16, 64)
		if err != nil {
			return [4]uint64{}, fmt.Errorf("failed to convert string to uint64: %v", err)
		}
	}

	return res, nil
}

func KeyEthAddrBalance(ethAddr string) NodeKey {
	return Key(ethAddr, KEY_BALANCE)
}

func KeyEthAddrNonce(ethAddr string) NodeKey {
	return Key(ethAddr, KEY_NONCE)
}

func KeyContractCode(ethAddr string) NodeKey {
	return Key(ethAddr, SC_CODE)
}

func KeyContractLength(ethAddr string) NodeKey {
	return Key(ethAddr, SC_LENGTH)
}

func Key(ethAddr string, c int) NodeKey {
	addressArray, err := ConvertHexToUint64Array(ethAddr)
	if err != nil {
		return NodeKey{}
	}

	key := [8]uint64{addressArray[0], addressArray[1], addressArray[2], addressArray[3], addressArray[4], addressArray[5], uint64(c), 0}

	return Hash(key, PoseidonAllZeroesHash)
}

func KeyBig(k *big.Int, c int) (*NodeKey, error) {
	if k == nil {
		return nil, errors.New("nil key")
	}

	add, err := ScalarToArrayUint64(k)
	if err != nil {
		return nil, err
	}

	key1 := [8]uint64{add[0], add[1], add[2], add[3], add[4], add[5], uint64(c), 0}

	hk0 := Hash(key1, PoseidonAllZeroesHash)

	return &NodeKey{hk0[0], hk0[1], hk0[2], hk0[3]}, nil
}

func StrValToBigInt(v string) (*big.Int, bool) {
	if strings.HasPrefix(v, "0x") {
		return new(big.Int).SetString(v[2:], 16)
	}

	return new(big.Int).SetString(v, 10)
}

func KeyContractStorage(ethAddr string, storagePosition string) (NodeKey, error) {
	sp, _ := StrValToBigInt(storagePosition)
	spArray, err := NodeValue8FromBigIntArray(ScalarToArrayBig(sp))
	if err != nil {
		return NodeKey{}, err
	}

	hk0 := Hash(spArray.ToUintArray(), [4]uint64{0, 0, 0, 0})

	addrArray, err := ConvertHexToUint64Array(ethAddr)
	if err != nil {
		return NodeKey{}, err
	}

	key := [8]uint64{addrArray[0], addrArray[1], addrArray[2], addrArray[3], addrArray[4], addrArray[5], uint64(SC_STORAGE), 0}

	return Hash(key, hk0), nil
}

func HashContractBytecode(bc string) string {
	return ConvertBigIntToHex(HashContractBytecodeBigInt(bc))
}

func HashContractBytecodeBigIntV1(bc string) *big.Int {
	bytecode := bc

	if strings.HasPrefix(bc, "0x") {
		bytecode = bc[2:]
	}

	if len(bytecode)%2 != 0 {
		bytecode = "0" + bytecode
	}

	bytecode += "01"

	for len(bytecode)%(56*2) != 0 {
		bytecode += "00"
	}

	lastByteInt, _ := strconv.ParseInt(bytecode[len(bytecode)-2:], 16, 64)
	lastByte := strconv.FormatInt(lastByteInt|0x80, 16)
	bytecode = bytecode[:len(bytecode)-2] + lastByte

	numBytes := float64(len(bytecode)) / 2
	numHashes := int(math.Ceil(numBytes / (BYTECODE_ELEMENTS_HASH * BYTECODE_BYTES_ELEMENT)))

	tmpHash := [4]uint64{0, 0, 0, 0}
	bytesPointer := 0

	maxBytesToAdd := BYTECODE_ELEMENTS_HASH * BYTECODE_BYTES_ELEMENT
	var elementsToHash []uint64
	var in [8]uint64
	var capacity [4]uint64
	scalar := new(big.Int)
	tmpScalar := new(big.Int)
	var byteToAdd string
	for i := 0; i < numHashes; i++ {
		elementsToHash = tmpHash[:]

		subsetBytecode := bytecode[bytesPointer : bytesPointer+maxBytesToAdd*2]
		bytesPointer += maxBytesToAdd * 2

		tmpElem := ""
		counter := 0

		for j := 0; j < maxBytesToAdd; j++ {
			byteToAdd = "00"
			if j < len(subsetBytecode)/2 {
				byteToAdd = subsetBytecode[j*2 : (j+1)*2]
			}

			tmpElem = byteToAdd + tmpElem
			counter += 1

			if counter == BYTECODE_BYTES_ELEMENT {
				tmpScalar, _ = scalar.SetString(tmpElem, 16)
				elementsToHash = append(elementsToHash, tmpScalar.Uint64())
				tmpElem = ""
				counter = 0
			}
		}

		copy(in[:], elementsToHash[4:12])
		copy(capacity[:], elementsToHash[:4])

		tmpHash = Hash(in, capacity)
	}

	return ArrayToScalar(tmpHash[:])
}

func charToDigit(c byte) int {
	if c >= '0' && c <= '9' {
		return int(c - '0')
	}
	if c >= 'a' && c <= 'f' {
		return int(c - 'a' + 10)
	}
	if c >= 'A' && c <= 'F' {
		return int(c - 'A' + 10)
	}
	// should not reach here
	return 0
}

func HashContractBytecodeBigInt(bc string) *big.Int {
	bytecode := bc

	if strings.HasPrefix(bc, "0x") {
		bytecode = bc[2:]
	}

	if len(bytecode)%2 != 0 {
		bytecode = "0" + bytecode
	}

	// MT is 56 (multiplier)
	MT := BYTECODE_ELEMENTS_HASH * BYTECODE_BYTES_ELEMENT
	bb := make([]byte, MT*(((len(bytecode)/2+1)/MT)+1))
	for i := 0; i < len(bytecode)/2; i++ {
		// use strconv.ParseInt
		// x, _ := strconv.ParseInt(bytecode[2*i:2*i+2], 16, 64)
		// bb[i] = byte(x)
		// simple
		bb[i] = byte(charToDigit(bytecode[2*i])<<4 + charToDigit(bytecode[2*i+1]))
	}
	for i := len(bytecode) / 2; i < len(bb); i++ {
		bb[i] = 0
	}

	bbPtr := len(bytecode) / 2
	bb[bbPtr] = 0x01
	bbPtr = len(bb) - 1
	bb[bbPtr] |= 0x80

	numBytes := float64(len(bb))
	numHashes := int(math.Ceil(numBytes / (BYTECODE_ELEMENTS_HASH * BYTECODE_BYTES_ELEMENT)))
	tmpHash := [4]uint64{0, 0, 0, 0}
	bytesPointer := 0

	maxBytesToAdd := BYTECODE_ELEMENTS_HASH * BYTECODE_BYTES_ELEMENT
	var elementsToHash []uint64
	var in [8]uint64
	var capacity [4]uint64
	for i := 0; i < numHashes; i++ {
		elementsToHash = tmpHash[:]

		subsetBytecode := bb[bytesPointer : bytesPointer+maxBytesToAdd]
		bytesPointer += maxBytesToAdd

		var tmpElem uint64
		tmpElem = 0
		counter := 0

		for j := 0; j < maxBytesToAdd; j++ {
			tmpElem += uint64(subsetBytecode[j]) << (8 * counter)
			counter += 1

			if counter == BYTECODE_BYTES_ELEMENT {
				elementsToHash = append(elementsToHash, tmpElem)
				tmpElem = 0
				counter = 0
			}
		}

		copy(in[:], elementsToHash[4:12])
		copy(capacity[:], elementsToHash[:4])

		tmpHash = Hash(in, capacity)
	}

	return ArrayToScalar(tmpHash[:])
}

func ResizeHashTo32BytesByPrefixingWithZeroes(hashValue []byte) []byte {
	lenDiff := 32 - len(hashValue)
	if lenDiff > 0 {
		result := make([]byte, 32)
		copy(result[lenDiff:], hashValue)
		return result
	}

	return hashValue
}

func binaryStringToUint64(binary string) (uint64, error) {
	num, err := strconv.ParseUint(binary, 2, 64)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func SortNodeKeysBitwiseAsc(keys []NodeKey) {
	slices.SortFunc(keys, func(i, j NodeKey) int {
		for l := 0; l < 64; l++ {
			for n := 0; n < 4; n++ {
				aBit := i[n] & 1
				bBit := j[n] & 1

				i[n] >>= 1 // Right shift the current part
				j[n] >>= 1 // Right shift the current part

				if aBit != bBit {
					if aBit < bBit {
						return -1
					} else if aBit > bBit {
						return 1
					}
				}
			}
		}

		return 0
	})
}

func EncodeKeySource(t int, accountAddr common.Address, storagePosition common.Hash) []byte {
	var keySource []byte
	keySource = append(keySource, byte(t))
	keySource = append(keySource, accountAddr.Bytes()...)
	if t == SC_STORAGE {
		keySource = append(keySource, storagePosition.Bytes()...)
	}
	return keySource
}

func DecodeKeySource(keySource []byte) (int, common.Address, common.Hash, error) {
	if len(keySource) < 1+length.Addr {
		return 0, common.Address{}, common.Hash{}, fmt.Errorf("invalid key source length")
	}

	t := int(keySource[0])
	accountAddr := common.BytesToAddress(keySource[1 : length.Addr+1])
	var storagePosition common.Hash
	if t == SC_STORAGE {
		if len(keySource) < 1+length.Addr+length.Hash {
			return 0, common.Address{}, common.Hash{}, fmt.Errorf("invalid key source length")
		}
		storagePosition = common.BytesToHash(keySource[length.Addr+1 : length.Addr+length.Hash+1])
	}
	return t, accountAddr, storagePosition, nil
}

func BigIntArrayToBytes(array []*big.Int) []byte {
	// Each uint64 needs 8 bytes, so total bytes needed is len(array) * 8
	buf := make([]byte, len(array)*8)

	// Convert each big.Int to uint64 and write to buffer
	for i := 0; i < len(array); i++ {
		// Write to buffer in reverse order to match original ArrayToScalarBig behavior
		pos := (len(array) - 1 - i) * 8
		binary.BigEndian.PutUint64(buf[pos:pos+8], array[i].Uint64())
	}
	return buf
}

func BigIntArrayToHex(array []*big.Int) string {
	return hex.EncodeToString(BigIntArrayToBytes(array))
}

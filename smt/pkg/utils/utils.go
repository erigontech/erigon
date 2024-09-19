package utils

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"sort"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/length"
	poseidon "github.com/gateway-fm/vectorized-poseidon-gold/src/vectorizedposeidongold"
)

const (
	KEY_BALANCE              = 0
	KEY_NONCE                = 1
	SC_CODE                  = 2
	SC_STORAGE               = 3
	SC_LENGTH                = 4
	HASH_POSEIDON_ALL_ZEROES = "0xc71603f33a1144ca7953db0ab48808f4c4055e3364a246c33c18a9786cb0b359"
	BYTECODE_ELEMENTS_HASH   = 8
	BYTECODE_BYTES_ELEMENT   = 7
)

type NodeValue8 [8]*big.Int
type NodeValue12 [12]*big.Int
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

func (nk *NodeKey) IsZero() bool {
	return nk[0] == 0 && nk[1] == 0 && nk[2] == 0 && nk[3] == 0
}

func (nk *NodeKey) IsEqualTo(nk2 NodeKey) bool {
	return nk[0] == nk2[0] && nk[1] == nk2[1] && nk[2] == nk2[2] && nk[3] == nk2[3]
}

func (nk *NodeKey) ToBigInt() *big.Int {
	return ArrayToScalar(nk[:])
}

func (nv *NodeValue8) IsZero() bool {
	if nv == nil {
		return true
	}

	for i := 0; i < 8; i++ {
		if nv[i] == nil || nv[i].Uint64() != 0 {
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
		nlh := big.Int{}
		nlh.SetUint64(v)
		nv[i+partI] = &nlh
	}

	return nil
}

func (nv *NodeValue8) ToUintArray() [8]uint64 {
	var result [8]uint64

	if nv != nil {
		for i := 0; i < 8; i++ {
			if nv[i] != nil {
				result[i] = nv[i].Uint64()
			}
			// if nv[i] is nil, result[i] will remain as its zero value (0)
		}
	}
	// if nv is nil, result will be an array of 8 zeros

	return result
}

func (nv *NodeValue12) ToBigInt() *big.Int {
	return ArrayToScalarBig(nv[:])
}

func (nv *NodeValue12) StripCapacity() [8]uint64 {
	return [8]uint64{nv[0].Uint64(), nv[1].Uint64(), nv[2].Uint64(), nv[3].Uint64(), nv[4].Uint64(), nv[5].Uint64(), nv[6].Uint64(), nv[7].Uint64()}
}

func (nv *NodeValue12) Get0to4() *NodeKey {
	// slice it 0-4
	return &NodeKey{nv[0].Uint64(), nv[1].Uint64(), nv[2].Uint64(), nv[3].Uint64()}
}

func (nv *NodeValue12) Get4to8() *NodeKey {
	// slice it 4-8
	return &NodeKey{nv[4].Uint64(), nv[5].Uint64(), nv[6].Uint64(), nv[7].Uint64()}
}

func (nv *NodeValue12) GetNodeValue8() *NodeValue8 {
	return &NodeValue8{nv[0], nv[1], nv[2], nv[3], nv[4], nv[5], nv[6], nv[7]}
}

func (nv *NodeValue12) Get0to8() [8]uint64 {
	// slice it from 0-8
	return [8]uint64{nv[0].Uint64(), nv[1].Uint64(), nv[2].Uint64(), nv[3].Uint64(), nv[4].Uint64(), nv[5].Uint64(), nv[6].Uint64(), nv[7].Uint64()}
}

func (nv *NodeValue12) IsUniqueSibling() (int, error) {
	count := 0
	fnd := 0
	a := nv[:]

	for i := 0; i < len(a); i += 4 {
		k := NodeKeyFromBigIntArray(a[i : i+4])
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

func IsArrayUint64Empty(arr []uint64) bool {
	for _, v := range arr {
		if v > 0 {
			return false
		}
	}

	return true
}

func Value8FromBigIntArray(arr []*big.Int) NodeValue8 {
	nv := [8]*big.Int{}
	copy(nv[:], arr)
	return nv
}

func NodeValue12FromBigIntArray(arr []*big.Int) (*NodeValue12, error) {
	if len(arr) != 12 {
		return &NodeValue12{}, fmt.Errorf("invalid array length")
	}
	nv := NodeValue12{}
	copy(nv[:], arr)
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
	copy(nv[:], arr)
	return &nv, nil
}

func BigIntArrayFromNodeValue8(nv *NodeValue8) []*big.Int {
	arr := make([]*big.Int, 8)

	copy(arr, nv[:])

	return arr
}

func (nv *NodeValue12) IsZero() bool {
	zero := false
	for _, v := range nv {
		if v.Cmp(big.NewInt(0)) == 0 {
			zero = true
		} else {
			zero = false
			break
		}
	}
	return zero
}

func (nv *NodeValue12) IsFinalNode() bool {
	if nv[8] == nil {
		return false
	}
	return nv[8].Cmp(big.NewInt(1)) == 0
}

// 7 times more efficient than sprintf
func ConvertBigIntToHex(n *big.Int) string {
	return "0x" + n.Text(16)
}

func ConvertHexToBigInt(hex string) *big.Int {
	hex = strings.TrimPrefix(hex, "0x")
	n, _ := new(big.Int).SetString(hex, 16)
	return n
}

func ConvertHexToAddress(hex string) common.Address {
	bigInt := ConvertHexToBigInt(hex)
	return common.BigToAddress(bigInt)
}

func ArrayToScalar(array []uint64) *big.Int {
	scalar := new(big.Int)
	for i := len(array) - 1; i >= 0; i-- {
		scalar.Lsh(scalar, 64)
		scalar.Add(scalar, new(big.Int).SetUint64(array[i]))
	}
	return scalar
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

func ArrayToScalarBig(array []*big.Int) *big.Int {
	scalar := new(big.Int)
	for i := len(array) - 1; i >= 0; i-- {
		scalar.Lsh(scalar, 64)
		scalar.Add(scalar, array[i])
	}
	return scalar
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
	out := [12]*big.Int{}
	mask := new(big.Int).SetBytes([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	scalar := new(big.Int).Set(scalarIn)

	for i := 0; i < 12; i++ {
		value := new(big.Int).And(scalar, mask)
		out[i] = value
		scalar.Rsh(scalar, 64)
	}
	return out
}

func ScalarToNodeValue8(scalarIn *big.Int) NodeValue8 {
	out := [8]*big.Int{}
	mask := new(big.Int).SetBytes([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	scalar := new(big.Int).Set(scalarIn)

	for i := 0; i < 8; i++ {
		value := new(big.Int).And(scalar, mask)
		out[i] = value
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

func ConcatArrays8AndCapacity(in [8]uint64, capacity [4]uint64) NodeValue12 {
	var sl []uint64
	sl = append(sl, in[:]...)
	sl = append(sl, capacity[:]...)

	v := NodeValue12{}
	for i, val := range sl {
		b := new(big.Int)
		v[i] = b.SetUint64(val)
	}

	return v
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

func ScalarToArrayBig12(scalar *big.Int) []*big.Int {
	scalar = new(big.Int).Set(scalar)
	mask := new(big.Int)
	mask.SetString("FFFFFFFF", 16)

	r0 := new(big.Int).And(scalar, mask)

	r1 := new(big.Int).Rsh(scalar, 32)
	r1 = new(big.Int).And(r1, mask)

	r2 := new(big.Int).Rsh(scalar, 64)
	r2 = new(big.Int).And(r2, mask)

	r3 := new(big.Int).Rsh(scalar, 96)
	r3 = new(big.Int).And(r3, mask)

	r4 := new(big.Int).Rsh(scalar, 128)
	r4 = new(big.Int).And(r4, mask)

	r5 := new(big.Int).Rsh(scalar, 160)
	r5 = new(big.Int).And(r5, mask)

	r6 := new(big.Int).Rsh(scalar, 192)
	r6 = new(big.Int).And(r6, mask)

	r7 := new(big.Int).Rsh(scalar, 224)
	r7 = new(big.Int).And(r7, mask)

	r8 := new(big.Int).Rsh(scalar, 256)
	r8 = new(big.Int).And(r8, mask)

	r9 := new(big.Int).Rsh(scalar, 288)
	r9 = new(big.Int).And(r9, mask)

	r10 := new(big.Int).Rsh(scalar, 320)
	r10 = new(big.Int).And(r10, mask)

	r11 := new(big.Int).Rsh(scalar, 352)
	r11 = new(big.Int).And(r11, mask)

	return []*big.Int{r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11}
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
	a := ConvertHexToBigInt(ethAddr)
	add := ScalarToArrayBig(a)

	key1 := NodeValue8{add[0], add[1], add[2], add[3], add[4], add[5], big.NewInt(int64(c)), big.NewInt(0)}
	key1Capacity, err := StringToH4(HASH_POSEIDON_ALL_ZEROES)
	if err != nil {
		return NodeKey{}
	}

	return Hash(key1.ToUintArray(), key1Capacity)
}

func KeyBig(k *big.Int, c int) (*NodeKey, error) {
	if k == nil {
		return nil, errors.New("nil key")
	}
	add := ScalarToArrayBig(k)

	key1 := NodeValue8{add[0], add[1], add[2], add[3], add[4], add[5], big.NewInt(int64(c)), big.NewInt(0)}
	key1Capacity, err := StringToH4(HASH_POSEIDON_ALL_ZEROES)
	if err != nil {
		return nil, err
	}

	hk0 := Hash(key1.ToUintArray(), key1Capacity)
	return &NodeKey{hk0[0], hk0[1], hk0[2], hk0[3]}, nil
}

func StrValToBigInt(v string) (*big.Int, bool) {
	if strings.HasPrefix(v, "0x") {
		return new(big.Int).SetString(v[2:], 16)
	}

	return new(big.Int).SetString(v, 10)
}

func KeyContractStorage(ethAddr []*big.Int, storagePosition string) NodeKey {
	sp, _ := StrValToBigInt(storagePosition)
	spArray, err := NodeValue8FromBigIntArray(ScalarToArrayBig(sp))
	if err != nil {
		return NodeKey{}
	}

	hk0 := Hash(spArray.ToUintArray(), [4]uint64{0, 0, 0, 0})

	key1 := NodeValue8{ethAddr[0], ethAddr[1], ethAddr[2], ethAddr[3], ethAddr[4], ethAddr[5], big.NewInt(int64(SC_STORAGE)), big.NewInt(0)}

	return Hash(key1.ToUintArray(), hk0)
}

func HashContractBytecode(bc string) string {
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

	for i := 0; i < numHashes; i++ {
		maxBytesToAdd := BYTECODE_ELEMENTS_HASH * BYTECODE_BYTES_ELEMENT
		var elementsToHash []uint64
		elementsToHash = append(elementsToHash, tmpHash[:]...)

		subsetBytecode := bytecode[bytesPointer : bytesPointer+maxBytesToAdd*2]
		bytesPointer += maxBytesToAdd * 2

		tmpElem := ""
		counter := 0

		for j := 0; j < maxBytesToAdd; j++ {
			byteToAdd := "00"
			if j < len(subsetBytecode)/2 {
				byteToAdd = subsetBytecode[j*2 : (j+1)*2]
			}

			tmpElem = byteToAdd + tmpElem
			counter += 1

			if counter == BYTECODE_BYTES_ELEMENT {
				tmpScalar, _ := new(big.Int).SetString(tmpElem, 16)
				elementsToHash = append(elementsToHash, tmpScalar.Uint64())
				tmpElem = ""
				counter = 0
			}
		}

		var in [8]uint64
		copy(in[:], elementsToHash[4:12])

		var capacity [4]uint64
		copy(capacity[:], elementsToHash[:4])

		tmpHash = Hash(in, capacity)
	}

	return ConvertBigIntToHex(ArrayToScalar(tmpHash[:]))
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
	sort.Slice(keys, func(i, j int) bool {
		aTmp := keys[i]
		bTmp := keys[j]

		for l := 0; l < 64; l++ {
			for n := 0; n < 4; n++ {
				aBit := aTmp[n] & 1
				bBit := bTmp[n] & 1

				aTmp[n] >>= 1 // Right shift the current part
				bTmp[n] >>= 1 // Right shift the current part

				if aBit != bBit {
					return aBit < bBit
				}
			}
		}

		return true
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

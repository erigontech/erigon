package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/prysmaticlabs/gohashtree"
)

// Bits and masks are used for depth calculation.
const (
	mask0 = ^uint64((1 << (1 << iota)) - 1)
	mask1
	mask2
	mask3
	mask4
	mask5
)

const (
	bit0 = uint8(1 << iota)
	bit1
	bit2
	bit3
	bit4
	bit5
)

// Uint64Root retrieve the root of uint64 fields
func Uint64Root(val uint64) common.Hash {
	var root common.Hash
	binary.LittleEndian.PutUint64(root[:], val)
	return root
}

func ArraysRoot(input [][32]byte, length uint64) ([32]byte, error) {
	res, err := merkleRootFromLeaves(input, length)
	if err != nil {
		return [32]byte{}, err
	}

	return res, nil
}

func ArraysRootWithLength(input [][32]byte, length uint64) ([32]byte, error) {
	base, err := merkleRootFromLeaves(input, length)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(uint64(len(input)))
	return utils.Keccak256(base[:], lengthRoot[:]), nil
}

func Eth1DataVectorRoot(votes []*cltypes.Eth1Data, length uint64) ([32]byte, error) {
	var err error

	vectorizedVotesRoot := make([][32]byte, len(votes))
	// Vectorize ETH1 Data first of all
	for i, vote := range votes {
		vectorizedVotesRoot[i], err = vote.HashTreeRoot()
		if err != nil {
			return [32]byte{}, err
		}
	}

	return ArraysRootWithLength(vectorizedVotesRoot, length)
}

func Uint64ListRootWithLength(list []uint64, length uint64) ([32]byte, error) {
	var err error
	roots, err := PackUint64IntoChunks(list)
	if err != nil {
		return [32]byte{}, err
	}

	return ArraysRootWithLength(roots, length)
}

func ValidatorsVectorRoot(validators []*cltypes.Validator, length uint64) ([32]byte, error) {
	var err error

	vectorizedValidatorsRoot := make([][32]byte, len(validators))
	// Vectorize ETH1 Data first of all
	for i, validator := range validators {
		vectorizedValidatorsRoot[i], err = validator.HashTreeRoot()
		if err != nil {
			return [32]byte{}, err
		}
	}

	return ArraysRootWithLength(vectorizedValidatorsRoot, length)
}

func merkleRootFromLeaves(leaves [][32]byte, length uint64) ([32]byte, error) {
	if len(leaves) == 0 {
		return [32]byte{}, errors.New("zero leaves provided")
	}
	if len(leaves) == 1 {
		return leaves[0], nil
	}
	hashLayer := leaves
	layers := make([][][32]byte, depth(length)+1)
	layers[0] = hashLayer
	var err error
	hashLayer, err = merkleizeTrieLeaves(layers, hashLayer)
	if err != nil {
		return [32]byte{}, err
	}
	root := hashLayer[0]
	return root, nil
}

// depth retrieves the appropriate depth for the provided trie size.
func depth(v uint64) (out uint8) {
	if v <= 1 {
		return 0
	}
	v--
	if v&mask5 != 0 {
		v >>= bit5
		out |= bit5
	}
	if v&mask4 != 0 {
		v >>= bit4
		out |= bit4
	}
	if v&mask3 != 0 {
		v >>= bit3
		out |= bit3
	}
	if v&mask2 != 0 {
		v >>= bit2
		out |= bit2
	}
	if v&mask1 != 0 {
		v >>= bit1
		out |= bit1
	}
	if v&mask0 != 0 {
		out |= bit0
	}
	out++
	return
}

// merkleizeTrieLeaves returns intermediate roots of given leaves.
func merkleizeTrieLeaves(layers [][][32]byte, hashLayer [][32]byte) ([][32]byte, error) {
	i := 1
	chunkBuffer := bytes.NewBuffer([]byte{})
	chunkBuffer.Grow(64)
	for len(hashLayer) > 1 && i < len(layers) {
		if !utils.IsPowerOf2(uint64(len(hashLayer))) {
			return nil, fmt.Errorf("hash layer is a non power of 2: %d", len(hashLayer))
		}
		newLayer := make([][32]byte, len(hashLayer)/2)
		err := gohashtree.Hash(hashLayer, newLayer)
		if err != nil {
			return nil, err
		}
		hashLayer = newLayer
		layers[i] = hashLayer
		i++
	}
	return hashLayer, nil
}

// PackUint64IntoChunks packs a list of uint64 values into 32 byte roots.
func PackUint64IntoChunks(vals []uint64) ([][32]byte, error) {
	// Initialize how many uint64 values we can pack
	// into a single chunk(32 bytes). Each uint64 value
	// would take up 8 bytes.
	numOfElems := 4
	sizeOfElem := 32 / numOfElems
	// Determine total number of chunks to be
	// allocated to provided list of unsigned
	// 64-bit integers.
	numOfChunks := len(vals) / numOfElems
	// Add an extra chunk if the list size
	// is not a perfect multiple of the number
	// of elements.
	if len(vals)%numOfElems != 0 {
		numOfChunks++
	}
	chunkList := make([][32]byte, numOfChunks)
	for idx, b := range vals {
		// In order to determine how to pack in the uint64 value by index into
		// our chunk list we need to determine a few things.
		// 1) The chunk which the particular uint64 value corresponds to.
		// 2) The position of the value in the chunk itself.
		//
		// Once we have determined these 2 values we can simply find the correct
		// section of contiguous bytes to insert the value in the chunk.
		chunkIdx := idx / numOfElems
		idxInChunk := idx % numOfElems
		chunkPos := idxInChunk * sizeOfElem
		binary.LittleEndian.PutUint64(chunkList[chunkIdx][chunkPos:chunkPos+sizeOfElem], b)
	}
	return chunkList, nil
}

func ValidatorLimitForBalancesChunks() uint64 {
	maxValidatorLimit := uint64(ValidatorRegistryLimit)
	bytesInUint64 := uint64(8)
	return (maxValidatorLimit*bytesInUint64 + 31) / 32 // round to nearest chunk
}

func SlashingsRoot(slashings []uint64) ([32]byte, error) {
	slashingMarshaling := make([][]byte, SlashingsLength)
	for i := 0; i < len(slashings) && i < len(slashingMarshaling); i++ {
		slashBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(slashBuf, slashings[i])
		slashingMarshaling[i] = slashBuf
	}
	slashingChunks, err := PackByChunk(slashingMarshaling)
	if err != nil {
		return [32]byte{}, err
	}
	return ArraysRoot(slashingChunks, uint64(len(slashingChunks)))
}

// PackByChunk a given byte array's final chunk with zeroes if needed.
func PackByChunk(serializedItems [][]byte) ([][32]byte, error) {
	emptyChunk := [32]byte{}
	// If there are no items, we return an empty chunk.
	if len(serializedItems) == 0 {
		return [][32]byte{emptyChunk}, nil
	} else if len(serializedItems[0]) == 32 {
		// If each item has exactly BYTES_PER_CHUNK length, we return the list of serialized items.
		chunks := make([][32]byte, 0, len(serializedItems))
		for _, c := range serializedItems {
			var chunk [32]byte
			copy(chunk[:], c)
			chunks = append(chunks, chunk)
		}
		return chunks, nil
	}
	// We flatten the list in order to pack its items into byte chunks correctly.
	orderedItems := make([]byte, 0, len(serializedItems)*len(serializedItems[0]))
	for _, item := range serializedItems {
		orderedItems = append(orderedItems, item...)
	}
	// If all our serialized item slices are length zero, we
	// exit early.
	if len(orderedItems) == 0 {
		return [][32]byte{emptyChunk}, nil
	}
	numItems := len(orderedItems)
	var chunks [][32]byte
	for i := 0; i < numItems; i += 32 {
		j := i + 32
		// We create our upper bound index of the chunk, if it is greater than numItems,
		// we set it as numItems itself.
		if j > numItems {
			j = numItems
		}
		// We create chunks from the list of items based on the
		// indices determined above.
		// Right-pad the last chunk with zero bytes if it does not
		// have length bytesPerChunk from the helper.
		// The ToBytes32 helper allocates a 32-byte array, before
		// copying the ordered items in. This ensures that even if
		// the last chunk is != 32 in length, we will right-pad it with
		// zero bytes.
		var chunk [32]byte
		copy(chunk[:], orderedItems[i:j])
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// ParticipationBitsRoot computes the HashTreeRoot merkleization of
// participation roots.
func ParticipationBitsRoot(bits []byte) ([32]byte, error) {
	chunkedRoots, err := packParticipationBits(bits)
	if err != nil {
		return [32]byte{}, err
	}

	return ArraysRootWithLength(chunkedRoots, uint64(ValidatorRegistryLimit+31)/32)
}

// packParticipationBits into chunks. It'll pad the last chunk with zero bytes if
// it does not have length bytes per chunk.
func packParticipationBits(bytes []byte) ([][32]byte, error) {
	numItems := len(bytes)
	chunks := make([][32]byte, 0, numItems/32)
	for i := 0; i < numItems; i += 32 {
		j := i + 32
		// We create our upper bound index of the chunk, if it is greater than numItems,
		// we set it as numItems itself.
		if j > numItems {
			j = numItems
		}
		// We create chunks from the list of items based on the
		// indices determined above.
		chunk := [32]byte{}
		copy(chunk[:], bytes[i:j])
		chunks = append(chunks, chunk)
	}

	if len(chunks) == 0 {
		return chunks, nil
	}

	return chunks, nil
}

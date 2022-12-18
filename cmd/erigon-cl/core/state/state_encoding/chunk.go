package state_encoding

import "encoding/binary"

// PackUint64IntoChunks packs a list of uint64 values into 32 byte roots.
func PackUint64IntoChunks(vals []uint64) ([][32]byte, error) {
	numOfChunks := len(vals) / 4
	if len(vals)%4 != 0 {
		numOfChunks++
	}
	chunkList := make([][32]byte, numOfChunks)
	for idx, b := range vals {
		chunkIndex := idx % 4
		binary.LittleEndian.PutUint64(chunkList[idx/4][chunkIndex*8:chunkIndex*8+8], b)
	}
	return chunkList, nil
}

// PackByChunk a given byte array's final chunk with zeroes if needed.
func PackSlashings(serializedItems [][]byte) ([][32]byte, error) {
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

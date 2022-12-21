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

func PackSlashings(serializedItems [][]byte) ([][32]byte, error) {
	emptyChunk := [32]byte{}

	// If there are no items, return an empty chunk
	if len(serializedItems) == 0 {
		return [][32]byte{emptyChunk}, nil
	}

	// If all items are exactly 32 bytes long, return them as is
	if len(serializedItems[0]) == 32 {
		chunks := make([][32]byte, len(serializedItems))
		for i, c := range serializedItems {
			copy(chunks[i][:], c)
		}
		return chunks, nil
	}

	// Flatten the list of items
	orderedItems := make([]byte, 0, len(serializedItems)*len(serializedItems[0]))
	for _, item := range serializedItems {
		orderedItems = append(orderedItems, item...)
	}

	// If the flattened list is empty, return an empty chunk
	if len(orderedItems) == 0 {
		return [][32]byte{emptyChunk}, nil
	}

	// Pack the flattened list into chunks of 32 bytes
	var chunks [][32]byte
	for i := 0; i < len(orderedItems); i += 32 {
		var chunk [32]byte
		copy(chunk[:], orderedItems[i:i+32])
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

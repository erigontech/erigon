package qmtree

import "encoding/binary"

func EdgeNodesToBytes(edgeNodes []EdgeNode) []byte {
	stride := 8 + 32
	res := make([]byte, len(edgeNodes)*stride)
	for i, node := range edgeNodes {
		binary.LittleEndian.PutUint64(res[i*stride:i*stride+8], uint64(node.pos))
		copy(res[i*stride+8:(i+1)*stride], node.value[:])
	}
	return res
}

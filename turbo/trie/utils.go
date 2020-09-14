package trie

import "bytes"

type ByteArrayWriter struct {
	dest []byte
	pos  int
}

func (w *ByteArrayWriter) Setup(dest []byte, pos int) {
	w.dest = dest
	w.pos = pos
}

func (w *ByteArrayWriter) Write(data []byte) (int, error) {
	copy(w.dest[w.pos:], data)
	w.pos += len(data)
	return len(data), nil
}

type sortable [][]byte

func (s sortable) Len() int {
	return len(s)
}
func (s sortable) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}
func (s sortable) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

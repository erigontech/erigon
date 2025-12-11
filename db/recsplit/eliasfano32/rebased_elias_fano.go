package eliasfano32

// This is a wrapper of "plain" EliasFano for optimizing scenarios where the number sequence
// is constrained in a closed range [from, to], so we can store the entire sequence as deltas
// of "from" and save space.
//
// This is specially useful when the starting "from" is a huge number, so the binary representation
// of the Elias Fano sequence can be made smaller.
//
// The baseNum stores the base value which is added to each element when it is accessed. It is
// not meant to be stored together with the serialized data, but derived from some other source,
// like the start txNum of a snapshot file, so it can be globally applied to all sequences in the
// same file, resulting in huge space savings.
type RebasedEliasFano struct {
	baseNum uint64
	ef      EliasFano
}

func (ref *RebasedEliasFano) Get(i uint64) uint64 {
	return ref.baseNum + ref.ef.Get(i)
}

func (ref *RebasedEliasFano) Min() uint64 {
	return ref.baseNum + ref.ef.Min()
}

func (ref *RebasedEliasFano) Max() uint64 {
	return ref.baseNum + ref.ef.Max()
}

func (ref *RebasedEliasFano) Count() uint64 {
	return ref.ef.Count()
}

func (ref *RebasedEliasFano) Reset(baseNum uint64, raw []byte) {
	ref.baseNum = baseNum
	ref.ef.Reset(raw)
}

func (ref *RebasedEliasFano) Seek(v uint64) (uint64, bool) {
	if v < ref.baseNum {
		v = ref.baseNum
	}

	n, found := ref.ef.Seek(v - ref.baseNum)
	return ref.baseNum + n, found
}

func (ref *RebasedEliasFano) Iterator() *RebasedIterWrapper {
	return &RebasedIterWrapper{
		baseNum: ref.baseNum,
		it:      ref.ef.Iterator(),
		reverse: false,
	}
}

func (ref *RebasedEliasFano) ReverseIterator() *RebasedIterWrapper {
	return &RebasedIterWrapper{
		baseNum: ref.baseNum,
		it:      ref.ef.ReverseIterator(),
		reverse: true,
	}
}

type RebasedIterWrapper struct {
	baseNum uint64
	it      *EliasFanoIter
	reverse bool
}

func (it *RebasedIterWrapper) HasNext() bool {
	return it.it.HasNext()
}

func (it *RebasedIterWrapper) Next() (uint64, error) {
	n, err := it.it.Next()
	return it.baseNum + n, err
}

func (it *RebasedIterWrapper) Seek(v uint64) {
	if v < it.baseNum {
		it.it.Seek(0)
		if it.reverse {
			// force exhaustion as we are seeking before the first elem
			it.it.Next()
		}
		return
	}

	it.it.Seek(v - it.baseNum)
}

func (it *RebasedIterWrapper) Close() {
	it.it.Close()
}

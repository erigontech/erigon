package base_encoding

import (
	"encoding/binary"
	"io"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

type SSZQueueEncoder[T solid.EncodableHashableSSZ] struct {
	previousQueueSize int
	previousLastElem  T

	equalFunc func(a, b T) bool
}

func NewSSZQueueEncoder[T solid.EncodableHashableSSZ](equalFunc func(a, b T) bool) *SSZQueueEncoder[T] {
	return &SSZQueueEncoder[T]{equalFunc: equalFunc}
}

func (q *SSZQueueEncoder[T]) Initialize(oldQueue *solid.ListSSZ[T]) {
	q.previousQueueSize = oldQueue.Len()
	if oldQueue.Len() > 0 {
		q.previousLastElem = oldQueue.Get(oldQueue.Len() - 1)
	}
}

func findEndIndex[T solid.EncodableHashableSSZ](updatedQueue *solid.ListSSZ[T], equalFunc func(a, b T) bool, previousLastElem T) int {
	notFoundIndex := -1
	endIndex := notFoundIndex

	for i := updatedQueue.Len() - 1; i >= 0; i-- {
		if equalFunc(updatedQueue.Get(i), previousLastElem) {
			endIndex = i
			break
		}
	}
	return endIndex
}

func (q *SSZQueueEncoder[T]) WriteDiff(w io.Writer, updatedQueue *solid.ListSSZ[T]) error {
	var lengthToTruncate uint32
	endIndex := -1

	if q.previousQueueSize != 0 {
		endIndex = findEndIndex(updatedQueue, q.equalFunc, q.previousLastElem)
		// the format is: length to truncate at the beginning + elements count count + elements to add
		if endIndex != -1 {
			lengthToTruncate = uint32(q.previousQueueSize - (endIndex + 1))
		} else {
			lengthToTruncate = uint32(q.previousQueueSize)
		}
	}
	// write length to truncate
	if err := binary.Write(w, binary.LittleEndian, lengthToTruncate); err != nil {
		return err
	}

	// each element is [length, element]
	for i := endIndex + 1; i < updatedQueue.Len(); i++ {
		sszEncodedElement, err := updatedQueue.Get(i).EncodeSSZ(nil)
		if err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(sszEncodedElement))); err != nil {
			return err
		}
		if _, err := w.Write(sszEncodedElement); err != nil {
			return err
		}
	}
	return nil
}

func ApplySSZQueueDiff[T solid.EncodableHashableSSZ](r io.Reader, queue *solid.ListSSZ[T], version clparams.StateVersion) error {
	var lengthToTruncate uint32
	if err := binary.Read(r, binary.LittleEndian, &lengthToTruncate); err != nil {
		return err
	}
	// truncate the queue
	queue.Cut(int(lengthToTruncate))

	for {
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		var elem T
		elem = elem.Clone().(T)
		if err := elem.DecodeSSZ(buf, int(version)); err != nil {
			return err
		}
		queue.Append(elem)
	}
	return nil
}

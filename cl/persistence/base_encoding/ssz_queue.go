package base_encoding

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

var ErrCorruptQueueDiff = errors.New("corrupt queue diff")

type queueElement interface {
	solid.EncodableHashableSSZ
	comparable
}

type SSZQueueEncoder[T queueElement] struct {
	previousQueue []T
	prefix        []int
	equalFunc     func(a, b T) bool
}

func NewSSZQueueEncoder[T queueElement](equalFunc func(a, b T) bool) *SSZQueueEncoder[T] {
	return &SSZQueueEncoder[T]{equalFunc: equalFunc}
}

func (q *SSZQueueEncoder[T]) Initialize(oldQueue *solid.ListSSZ[T]) {
	clear(q.previousQueue)
	q.previousQueue = q.previousQueue[:0]
	for i := 0; i < oldQueue.Len(); i++ {
		q.previousQueue = append(q.previousQueue, oldQueue.Get(i))
	}
}

// Queue elements are copy-on-write, so equal pointers hold equal values.
func (q *SSZQueueEncoder[T]) sharedPointerOverlap(updatedQueue *solid.ListSSZ[T]) (int, bool) {
	if updatedQueue.Len() == 0 {
		return 0, false
	}
	start := slices.Index(q.previousQueue, updatedQueue.Get(0))
	if start < 0 {
		return 0, false
	}
	overlap := len(q.previousQueue) - start
	if overlap > updatedQueue.Len() {
		return 0, false
	}
	for i := 1; i < overlap; i++ {
		if q.previousQueue[start+i] != updatedQueue.Get(i) {
			return 0, false
		}
	}
	return overlap, true
}

func (q *SSZQueueEncoder[T]) longestOverlap(updatedQueue *solid.ListSSZ[T]) int {
	if overlap, ok := q.sharedPointerOverlap(updatedQueue); ok {
		return overlap
	}
	patternLength := min(len(q.previousQueue), updatedQueue.Len())
	if patternLength == 0 {
		return 0
	}

	q.prefix = slices.Grow(q.prefix[:0], patternLength)[:patternLength]
	q.prefix[0] = 0
	for i := 1; i < patternLength; i++ {
		matched := q.prefix[i-1]
		for {
			if q.equalFunc(updatedQueue.Get(i), updatedQueue.Get(matched)) {
				matched++
				break
			}
			if matched == 0 {
				break
			}
			matched = q.prefix[matched-1]
		}
		q.prefix[i] = matched
	}

	matched := 0
	for i := len(q.previousQueue) - patternLength; i < len(q.previousQueue); i++ {
		for {
			if q.equalFunc(q.previousQueue[i], updatedQueue.Get(matched)) {
				matched++
				break
			}
			if matched == 0 {
				break
			}
			matched = q.prefix[matched-1]
		}
	}
	return matched
}

func (q *SSZQueueEncoder[T]) WriteDiff(w io.Writer, updatedQueue *solid.ListSSZ[T]) error {
	overlap := q.longestOverlap(updatedQueue)
	lengthToTruncate := uint32(len(q.previousQueue) - overlap)
	// write length to truncate
	if err := binary.Write(w, binary.LittleEndian, lengthToTruncate); err != nil {
		return err
	}

	// each element is [length, element]
	for i := overlap; i < updatedQueue.Len(); i++ {
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
	if uint64(lengthToTruncate) > uint64(queue.Len()) {
		return fmt.Errorf("%w: truncate %d exceeds queue length %d", ErrCorruptQueueDiff, lengthToTruncate, queue.Len())
	}
	// truncate the queue
	queue.Cut(int(lengthToTruncate))

	for {
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
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

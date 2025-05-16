package downloadercfg

import (
	"iter"
)

// Chain combines multiple iter.Seq iterators into a single iter.Seq. It yields all elements from
// the first iterator, then the second, and so on.
func chainSeqs[V any](seqs ...iter.Seq[V]) iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, seq := range seqs {
			for v := range seq {
				if !yield(v) {
					return
				}
			}
		}
	}
}

func yieldFrom[V any](seq iter.Seq[V], yield func(V) bool) bool {
	for v := range seq {
		if !yield(v) {
			return false
		}
	}
	return true
}

func mapSeq[F, T any](f func(F) T, in iter.Seq[F]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for a := range in {
			if !yield(f(a)) {
				return
			}
		}
	}
}

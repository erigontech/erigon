package stream

import "fmt"

type UnoLogger[V any] struct {
	u      Uno[V]
	prefix string
}

func NewUnoLogger[V any](u Uno[V], prefix string) *UnoLogger[V] {
	return &UnoLogger[V]{u: u, prefix: prefix}
}

func (s UnoLogger[V]) Next() (V, error) {
	v, err := s.u.Next()
	if err == nil {
		fmt.Printf("%s Next() == %v\n", s.prefix, v)
	}
	return v, err
}

func (s UnoLogger[V]) HasNext() bool {
	h := s.u.HasNext()
	fmt.Printf("%s HasNext() == %v\n", s.prefix, h)
	return h
}

func (s UnoLogger[V]) Close() {
	s.u.Close()
}

package generics

// Zero value of any type T
func Zero[T any]() T {
	var value T
	return value
}

func New[T any]() *T {
	return new(T)
}

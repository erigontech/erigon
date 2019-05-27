package util

// Sequence is a namespace singleton for sequence utilities.
var Sequence sequenceUtil

type sequenceUtil struct{}

// Ints returns a sequence [0, end).
func (su sequenceUtil) Ints(end int) (values []int) {
	for i := 0; i < end; i++ {
		values = append(values, i)
	}
	return
}

// IntsFrom returns a sequence [start, end).
func (su sequenceUtil) IntsFrom(start, end int) (values []int) {
	for i := start; i < end; i++ {
		values = append(values, i)
	}
	return
}

// IntsFrom returns a sequence [start, end).
func (su sequenceUtil) IntsFromBy(start, end, by int) (values []int) {
	for i := start; i < end; i += by {
		values = append(values, i)
	}
	return
}

// Floats returns a sequence [0, end).
func (su sequenceUtil) Floats(end float64) (values []float64) {
	for i := 0.0; i < end; i++ {
		values = append(values, i)
	}
	return
}

// FloatsFrom returns a sequence [start, end).
func (su sequenceUtil) FloatsFrom(start, end float64) (values []float64) {
	for i := start; i < end; i++ {
		values = append(values, i)
	}
	return
}

// FloatsFrom returns a sequence [start, end).
func (su sequenceUtil) FloatsFromBy(start, end, by float64) (values []float64) {
	for i := start; i < end; i += by {
		values = append(values, i)
	}
	return
}

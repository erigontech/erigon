package generic

type Ranger[T any] interface {
	Range(func(idx int, v T, leng int) bool)
}

func RangeErr[T any](r Ranger[T], fn func(int, T, int) error) (err error) {
	r.Range(func(idx int, v T, leng int) bool {
		err = fn(idx, v, leng)
		if err != nil {
			return false
		}
		return true
	})
	return
}

package util

// AnyError returns any variadic error that is not nil.
func AnyError(err ...error) error {
	for x := 0; x < len(err); x++ {
		if err[x] != nil {
			return err[x]
		}
	}
	return nil
}

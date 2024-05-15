package heimdall

type ClosedRange struct {
	Start uint64
	End   uint64
}

func (r ClosedRange) Len() uint64 {
	return r.End + 1 - r.Start
}

func ClosedRangeMap[TResult any](r ClosedRange, projection func(i uint64) (TResult, error)) ([]TResult, error) {
	results := make([]TResult, 0, r.Len())

	for i := r.Start; i <= r.End; i++ {
		entity, err := projection(i)
		if err != nil {
			return nil, err
		}

		results = append(results, entity)
	}

	return results, nil
}

func (r ClosedRange) Map(projection func(i uint64) (any, error)) ([]any, error) {
	return ClosedRangeMap(r, projection)
}

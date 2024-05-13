package heimdall

type EntityIdRange struct {
	Start uint64
	End   uint64
}

func (r EntityIdRange) Len() uint64 {
	return r.End + 1 - r.Start
}

func EntityIdRangeMap[TResult any](r EntityIdRange, projection func(i uint64) (TResult, error)) ([]TResult, error) {
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

func (r EntityIdRange) Map(projection func(i uint64) (any, error)) ([]any, error) {
	return EntityIdRangeMap(r, projection)
}

package heimdall

type EntityIdRange struct {
	Start uint64
	End   uint64
}

func (r EntityIdRange) Len() uint64 {
	return r.End + 1 - r.Start
}

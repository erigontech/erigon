package metrics

// Metrics holds deterministic statistics computed from a page-residency bitmap.
type Metrics struct {
	TotalPages   int64
	PagesLoaded  int64
	BytesLoaded  int64
	ScatterScore float64 // average gap (in pages) between consecutive loaded pages
	Density      float64 // PagesLoaded / span (loaded / (last - first + 1))
	SpanBytes    int64   // (lastLoadedPage - firstLoadedPage + 1) * pageSize
	MaxGap       int64   // largest gap in pages between consecutive loaded runs
}

// Compute derives Metrics from a residency slice. pageSize is the OS page size in bytes.
func Compute(residency []bool, pageSize int64) Metrics {
	m := Metrics{TotalPages: int64(len(residency))}

	first, last := int64(-1), int64(-1)
	var gapSum, gapCount int64
	var maxGap int64
	prev := int64(-1) // last loaded page index

	for i, in := range residency {
		if !in {
			continue
		}
		idx := int64(i)
		m.PagesLoaded++
		if first < 0 {
			first = idx
		}
		last = idx

		if prev >= 0 {
			gap := idx - prev - 1
			if gap > 0 {
				gapSum += gap
				gapCount++
				if gap > maxGap {
					maxGap = gap
				}
			}
		}
		prev = idx
	}

	if m.PagesLoaded == 0 {
		return m
	}

	m.BytesLoaded = m.PagesLoaded * pageSize
	span := last - first + 1
	m.SpanBytes = span * pageSize
	m.Density = float64(m.PagesLoaded) / float64(span)
	if gapCount > 0 {
		m.ScatterScore = float64(gapSum) / float64(gapCount)
	}
	m.MaxGap = maxGap
	return m
}

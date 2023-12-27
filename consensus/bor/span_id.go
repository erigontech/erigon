package bor

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

func SpanIDAt(number uint64) uint64 {
	if number > zerothSpanEnd {
		return 1 + (number-zerothSpanEnd-1)/spanLength
	}
	return 0
}

func SpanEndBlockNum(spanID uint64) uint64 {
	if spanID > 0 {
		return spanID*spanLength + zerothSpanEnd
	}
	return zerothSpanEnd
}

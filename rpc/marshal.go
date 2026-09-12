package rpc

// sliceWriter lets the JSON encoders stream into a caller-owned buffer, so a
// reused buffer keeps its capacity across responses.
type sliceWriter struct{ b []byte }

func (w *sliceWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

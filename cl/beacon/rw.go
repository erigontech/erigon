package beacon

import (
	"net/http"
)

type notFoundNoWriter struct {
	rw http.ResponseWriter

	code    int
	headers http.Header
}

func (f *notFoundNoWriter) Header() http.Header {
	if f.code == 404 {
		return make(http.Header)
	}
	return f.rw.Header()
}

func (f *notFoundNoWriter) Write(xs []byte) (int, error) {
	// write code 200 if code not written yet
	if f.code == 0 {
		f.WriteHeader(200)
	}
	if f.code == 404 {
		return 0, nil
	}
	// pass on the write
	return f.rw.Write(xs)
}

func (f *notFoundNoWriter) WriteHeader(statusCode int) {
	if f.code != 0 {
		return
	}
	if f.code != 404 {
		f.rw.WriteHeader(statusCode)
	}
	// if it's a 404 and we are not at our last handler, set the target to an io.Discard
	f.code = statusCode
}

package beacon

import (
	"net/http"
)

type notFoundNoWriter struct {
	rw http.ResponseWriter
	r  *http.Request

	code    int
	headers http.Header
}

func isNotFound(code int) bool {
	return code == 404 || code == 405
}

func (f *notFoundNoWriter) Header() http.Header {
	if isNotFound(f.code) {
		return make(http.Header)
	}
	if f.headers == nil {
		f.headers = make(http.Header)
	}
	return f.headers
}

func (f *notFoundNoWriter) Write(xs []byte) (int, error) {
	// write code 200 if code not written yet
	if f.code == 0 {
		f.WriteHeader(200)
	}
	if isNotFound(f.code) {
		return 0, nil
	}
	// pass on the write
	return f.rw.Write(xs)
}

func (f *notFoundNoWriter) WriteHeader(statusCode int) {
	if f.code != 0 {
		return
	}
	f.code = statusCode
	if isNotFound(statusCode) {
		f.headers = nil
		return
	}
	f.rw.WriteHeader(statusCode)
	// if we get here, it means it is a successful write.
	if f.headers != nil {
		for k, v := range f.headers {
			for _, x := range v {
				f.rw.Header().Add(k, x)
			}
		}
	}
	f.headers = f.rw.Header()
}
func (f *notFoundNoWriter) Flush() {
	defer func() {
		if err := recover(); err != nil {
			// a closed http request can cause a panic here
			// it's not actually a go bug, but this is probably the easiest way to deal with it
			// https://github.com/golang/go/issues/9657
		}
	}()
	flusher, ok := f.rw.(http.Flusher)
	if !ok {
		return
	}
	select {
	case <-f.r.Context().Done():
		return
	default:
	}
	if flusher != nil {
		flusher.Flush()
	}
}

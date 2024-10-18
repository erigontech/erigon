// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package beacon

import (
	"net/http"
)

type notFoundNoWriter struct {
	http.ResponseWriter
	r *http.Request

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
	return f.ResponseWriter.Write(xs)
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
	f.ResponseWriter.WriteHeader(statusCode)
	// if we get here, it means it is a successful write.
	if f.headers != nil {
		for k, v := range f.headers {
			for _, x := range v {
				f.ResponseWriter.Header().Add(k, x)
			}
		}
	}
	f.headers = f.ResponseWriter.Header()
}
func (f *notFoundNoWriter) Flush() {
	flusher, ok := f.ResponseWriter.(http.Flusher)
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

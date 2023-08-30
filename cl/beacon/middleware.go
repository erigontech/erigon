package beacon

import "net/http"

type ResponseFormat struct{}

func newBeaconMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")
		accept := r.Header.Get("Accept")
		if contentType != "application/json" {
			http.Error(w, "Content-Type header must be application/json", http.StatusUnsupportedMediaType)
			return
		}

		if accept == "application/json" {

		} else if accept == "application/octet-stream" {

		} else {
			http.Error(w, "Unsupported Accept header value", http.StatusNotAcceptable)
			return
		}

		// Our middleware logic goes here...
		next.ServeHTTP(w, r)
	})
}

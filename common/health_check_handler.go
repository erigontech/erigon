package common

import (
	"fmt"
	"net/http"
)

type HealthCheckHandler func() bool

func (handler HealthCheckHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/health" {
		http.NotFound(writer, request)
		return
	}

	if handler() {
		writer.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(writer, "100%")
	} else {
		http.Error(writer, "Service Unavailable", http.StatusServiceUnavailable)
	}
}

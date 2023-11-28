package beaconhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/ledgerwatch/log/v3"
)

type EndpointError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NewEndpointError(code int, message string) *EndpointError {
	return &EndpointError{
		Code:    code,
		Message: message,
	}
}

func (e *EndpointError) Error() string {
	return fmt.Sprintf("Code %d: %s", e.Code, e.Message)
}

func (e *EndpointError) WriteTo(w http.ResponseWriter) {
	w.WriteHeader(e.Code)
	encErr := json.NewEncoder(w).Encode(e)
	if encErr != nil {
		log.Error("beaconapi failed to write json error", "err", encErr)
	}
}

type EndpointHandler interface {
	Handle(r *http.Request) (any, error)
}

type EndpointHandlerFunc func(r *http.Request) (any, error)

func (e EndpointHandlerFunc) Handle(r *http.Request) (any, error) {
	return e(r)
}

func HandleEndpointFunc(h EndpointHandlerFunc) http.HandlerFunc {
	return HandleEndpoint(h)
}

func HandleEndpoint(h EndpointHandler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ans, err := h.Handle(r)
		if err != nil {
			// check if is endpoint error, if so, handle specially
			endpointError := &EndpointError{}
			if !errors.As(err, &endpointError) {
				endpointError = NewEndpointError(500, err.Error())
			}
			endpointError.WriteTo(w)
			return
		}
		// TODO: ssz handler
		// TODO: potentially add a context option to buffer these
		contentType := r.Header.Get("Accept")
		switch contentType {
		case "application/json":
			err := json.NewEncoder(w).Encode(ans)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				// this error is fatal, log to console
				log.Error("beaconapi failed to encode json", "type", reflect.TypeOf(ans), "err", err)
			}
		case "application/octet-stream":
			NewEndpointError(http.StatusNotImplemented, "Not Implemented").WriteTo(w)
		}
	})
}

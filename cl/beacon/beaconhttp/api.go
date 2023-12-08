package beaconhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/log/v3"
)

var _ error = EndpointError{}
var _ error = (*EndpointError)(nil)

type EndpointError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func WrapEndpointError(err error) *EndpointError {
	e := &EndpointError{}
	if errors.As(err, e) {
		return e
	}
	if errors.Is(err, fork_graph.ErrStateNotFound) {
		return NewEndpointError(http.StatusNotFound, "Could not find beacon state")
	}
	return NewEndpointError(http.StatusInternalServerError, err.Error())
}

func NewEndpointError(code int, message string) *EndpointError {
	return &EndpointError{
		Code:    code,
		Message: message,
	}
}

func (e EndpointError) Error() string {
	return fmt.Sprintf("Code %d: %s", e.Code, e.Message)
}

func (e *EndpointError) WriteTo(w http.ResponseWriter) {
	w.WriteHeader(e.Code)
	encErr := json.NewEncoder(w).Encode(e)
	if encErr != nil {
		log.Error("beaconapi failed to write json error", "err", encErr)
	}
}

type EndpointHandler[T any] interface {
	Handle(r *http.Request) (T, error)
}

type EndpointHandlerFunc[T any] func(r *http.Request) (T, error)

func (e EndpointHandlerFunc[T]) Handle(r *http.Request) (T, error) {
	return e(r)
}

func HandleEndpointFunc[T any](h EndpointHandlerFunc[T]) http.HandlerFunc {
	return HandleEndpoint[T](h)
}

func HandleEndpoint[T any](h EndpointHandler[T]) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ans, err := h.Handle(r)
		if err != nil {
			log.Error("beacon api request error", "err", err)
			endpointError := WrapEndpointError(err)
			endpointError.WriteTo(w)
			return
		}
		// TODO: ssz handler
		// TODO: potentially add a context option to buffer these
		contentType := r.Header.Get("Accept")
		switch contentType {
		case "application/octet-stream":
			sszMarshaler, ok := any(ans).(ssz.Marshaler)
			if !ok {
				NewEndpointError(http.StatusBadRequest, "This endpoint does not support SSZ response").WriteTo(w)
				return
			}
			// TODO: we should probably figure out some way to stream this in the future :)
			encoded, err := sszMarshaler.EncodeSSZ(nil)
			if err != nil {
				WrapEndpointError(err).WriteTo(w)
				return
			}
			w.Write(encoded)
		case "application/json", "":
			w.Header().Add("content-type", "application/json")
			err := json.NewEncoder(w).Encode(ans)
			if err != nil {
				// this error is fatal, log to console
				log.Error("beaconapi failed to encode json", "type", reflect.TypeOf(ans), "err", err)
			}
		default:
			http.Error(w, "content type must be application/json or application/octet-stream", http.StatusBadRequest)

		}
	})
}

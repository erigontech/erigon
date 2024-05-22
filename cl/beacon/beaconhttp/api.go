package beaconhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/log/v3"
)

var _ error = EndpointError{}
var _ error = (*EndpointError)(nil)

type EndpointError struct {
	Code        int      `json:"code"`
	Message     string   `json:"message"`
	Stacktraces []string `json:"stacktraces,omitempty"`

	err error
}

var ErrorCantFindBeaconState = errors.New("Could not find beacon state")
var ErrorSszNotSupported = errors.New("This endpoint does not support SSZ response")

func WrapEndpointError(err error) *EndpointError {
	e := &EndpointError{}
	if errors.As(err, e) {
		return e
	}
	if errors.Is(err, fork_graph.ErrStateNotFound) {
		return NewEndpointError(http.StatusNotFound, ErrorCantFindBeaconState)
	}
	return NewEndpointError(http.StatusInternalServerError, err)
}

func NewEndpointError(code int, err error) *EndpointError {
	// TODO: consider adding stack traces/debug mode ?
	//b := make([]byte, 2048)
	//n := runtime.Stack(b, false)
	//s := string(b[:n])
	return &EndpointError{
		Code:    code,
		Message: err.Error(),
		//	Stacktraces: []string{s}
		err: err,
	}
}

func (e EndpointError) Error() string {
	return fmt.Sprintf("Code %d: %s", e.Code, e.Message)
}

func (e EndpointError) Unwrap() error {
	return e.err
}

func (e *EndpointError) WriteTo(w http.ResponseWriter) {
	w.WriteHeader(e.Code)
	encErr := json.NewEncoder(w).Encode(e)
	if encErr != nil {
		log.Error("beaconapi failed to write json error", "err", encErr)
	}
}

type EndpointHandler[T any] interface {
	Handle(w http.ResponseWriter, r *http.Request) (T, error)
}

type EndpointHandlerFunc[T any] func(w http.ResponseWriter, r *http.Request) (T, error)

func (e EndpointHandlerFunc[T]) Handle(w http.ResponseWriter, r *http.Request) (T, error) {
	return e(w, r)
}

func HandleEndpointFunc[T any](h EndpointHandlerFunc[T]) http.HandlerFunc {
	return HandleEndpoint[T](h)
}

func HandleEndpoint[T any](h EndpointHandler[T]) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ans, err := h.Handle(w, r)
		if err != nil {
			var endpointError *EndpointError
			if e, ok := err.(*EndpointError); ok {
				endpointError = e
			} else {
				endpointError = WrapEndpointError(err)
			}
			endpointError.WriteTo(w)
			return
		}
		// TODO: potentially add a context option to buffer these
		contentType := r.Header.Get("Accept")
		contentTypes := strings.Split(contentType, ",")

		// early return for event stream
		if slices.Contains(w.Header().Values("Content-Type"), "text/event-stream") {
			return
		}
		switch {
		case slices.Contains(contentTypes, "application/octet-stream"):
			sszMarshaler, ok := any(ans).(ssz.Marshaler)
			if !ok {
				NewEndpointError(http.StatusBadRequest, ErrorSszNotSupported).WriteTo(w)
				return
			}
			// TODO: we should probably figure out some way to stream this in the future :)
			encoded, err := sszMarshaler.EncodeSSZ(nil)
			if err != nil {
				WrapEndpointError(err).WriteTo(w)
				return
			}
			w.Write(encoded)
		case contentType == "*/*", contentType == "", slices.Contains(contentTypes, "text/html"), slices.Contains(contentTypes, "application/json"):
			if !isNil(ans) {
				w.Header().Add("content-type", "application/json")
				err := json.NewEncoder(w).Encode(ans)
				if err != nil {
					// this error is fatal, log to console
					log.Error("beaconapi failed to encode json", "type", reflect.TypeOf(ans), "err", err)
				}
			} else {
				w.WriteHeader(200)
			}
		case slices.Contains(contentTypes, "text/event-stream"):
			return
		default:
			http.Error(w, "content type must be application/json, application/octet-stream, or text/event-stream", http.StatusBadRequest)
		}
	})
}

func isNil[T any](t T) bool {
	v := reflect.ValueOf(t)
	kind := v.Kind()
	// Must be one of these types to be nillable
	return (kind == reflect.Ptr ||
		kind == reflect.Interface ||
		kind == reflect.Slice ||
		kind == reflect.Map ||
		kind == reflect.Chan ||
		kind == reflect.Func) &&
		v.IsNil()
}

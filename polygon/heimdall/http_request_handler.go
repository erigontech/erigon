package heimdall

import "net/http"

//go:generate mockgen -typed=true -source=./http_request_handler.go -destination=./http_request_handler_mock.go -package=heimdall httpRequestHandler
type httpRequestHandler interface {
	Do(req *http.Request) (*http.Response, error)
	CloseIdleConnections()
}

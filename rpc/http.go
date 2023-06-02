// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/log/v3"
)

const (
	maxRequestContentLength = 1024 * 1024 * 5
	contentType             = "application/json"
	jwtTokenExpiry          = 60 * time.Second
)

// https://www.jsonrpc.org/historical/json-rpc-over-http.html#id13
var acceptedContentTypes = []string{contentType, "application/json-rpc", "application/jsonrequest"}

type httpConn struct {
	client    *http.Client
	url       string
	closeOnce sync.Once
	closeCh   chan interface{}
	mu        sync.Mutex // protects headers
	headers   http.Header
}

// httpConn is treated specially by Client.
func (hc *httpConn) writeJSON(context.Context, interface{}) error {
	panic("writeJSON called on httpConn")
}

func (hc *httpConn) remoteAddr() string {
	return hc.url
}

func (hc *httpConn) readBatch() ([]*jsonrpcMessage, bool, error) {
	<-hc.closeCh
	return nil, false, io.EOF
}

func (hc *httpConn) close() {
	hc.closeOnce.Do(func() { close(hc.closeCh) })
}

func (hc *httpConn) closed() <-chan interface{} {
	return hc.closeCh
}

// DialHTTPWithClient creates a new RPC client that connects to an RPC server over HTTP
// using the provided HTTP Client.
func DialHTTPWithClient(endpoint string, client *http.Client, logger log.Logger) (*Client, error) {
	// Sanity check URL so we don't end up with a client that will fail every request.
	_, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	initctx := context.Background()
	headers := make(http.Header, 2)
	headers.Set("accept", contentType)
	headers.Set("content-type", contentType)
	return newClient(initctx, func(context.Context) (ServerCodec, error) {
		hc := &httpConn{
			client:  client,
			headers: headers,
			url:     endpoint,
			closeCh: make(chan interface{}),
		}
		return hc, nil
	}, logger)
}

// DialHTTP creates a new RPC client that connects to an RPC server over HTTP.
func DialHTTP(endpoint string, logger log.Logger) (*Client, error) {
	return DialHTTPWithClient(endpoint, new(http.Client), logger)
}

func (c *Client) sendHTTP(ctx context.Context, op *requestOp, msg interface{}) error {
	hc := c.writeConn.(*httpConn)
	respBody, err := hc.doRequest(ctx, msg)
	if respBody != nil {
		defer respBody.Close()
	}

	if err != nil {
		if respBody != nil {
			buf := new(bytes.Buffer)
			if _, err2 := buf.ReadFrom(respBody); err2 == nil {
				return fmt.Errorf("%w: %v", err, buf.String())
			}
		}
		return err
	}
	var respmsg jsonrpcMessage
	if err := json.NewDecoder(respBody).Decode(&respmsg); err != nil {
		return err
	}
	op.resp <- &respmsg
	return nil
}

func (c *Client) sendBatchHTTP(ctx context.Context, op *requestOp, msgs []*jsonrpcMessage) error {
	hc := c.writeConn.(*httpConn)
	respBody, err := hc.doRequest(ctx, msgs)
	if err != nil {
		return err
	}
	defer respBody.Close()
	var respmsgs []jsonrpcMessage
	if err := json.NewDecoder(respBody).Decode(&respmsgs); err != nil {
		return err
	}
	for i := 0; i < len(respmsgs); i++ {
		op.resp <- &respmsgs[i]
	}
	return nil
}

func (hc *httpConn) doRequest(ctx context.Context, msg interface{}) (io.ReadCloser, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", hc.url, io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(body))

	// set headers
	hc.mu.Lock()
	req.Header = hc.headers.Clone()
	hc.mu.Unlock()

	// do request
	resp, err := hc.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.Body, errors.New(resp.Status)
	}
	return resp.Body, nil
}

// httpServerConn turns a HTTP connection into a Conn.
type httpServerConn struct {
	io.Reader
	io.Writer
	r *http.Request
}

func newHTTPServerConn(r *http.Request, w http.ResponseWriter) ServerCodec {
	conn := &httpServerConn{Writer: w, r: r}
	// if the request is a GET request, and the body is empty, we turn the request into fake json rpc request, see below
	// https://www.jsonrpc.org/historical/json-rpc-over-http.html#encoded-parameters
	// we however allow for non base64 encoded parameters to be passed
	if r.Method == http.MethodGet && r.ContentLength == 0 {
		// default id 1
		id := `1`
		id_up := r.URL.Query().Get("id")
		if id_up != "" {
			id = id_up
		}
		method_up := r.URL.Query().Get("method")
		params, _ := url.QueryUnescape(r.URL.Query().Get("params"))
		param := []byte(params)
		if pb, err := base64.URLEncoding.DecodeString(params); err == nil {
			param = pb
		}
		buf := new(bytes.Buffer)
		json.NewEncoder(buf).Encode(jsonrpcMessage{
			ID:     json.RawMessage(id),
			Method: method_up,
			Params: param,
		})
		conn.Reader = buf
	} else {
		// it's a post request or whatever, so just process it like normal
		conn.Reader = io.LimitReader(r.Body, maxRequestContentLength)
	}
	return NewCodec(conn)
}

// Close does nothing and always returns nil.
func (t *httpServerConn) Close() error { return nil }

// RemoteAddr returns the peer address of the underlying connection.
func (t *httpServerConn) RemoteAddr() string {
	return t.r.RemoteAddr
}

// SetWriteDeadline does nothing and always returns nil.
func (t *httpServerConn) SetWriteDeadline(time.Time) error { return nil }

// ServeHTTP serves JSON-RPC requests over HTTP.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Permit dumb empty requests for remote health-checks (AWS)
	if r.Method == http.MethodGet && r.ContentLength == 0 && r.URL.RawQuery == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if code, err := validateRequest(r); err != nil {
		http.Error(w, err.Error(), code)
		return
	}
	// All checks passed, create a codec that reads directly from the request body
	// until EOF, writes the response to w, and orders the server to process a
	// single request.
	ctx := r.Context()
	ctx = context.WithValue(ctx, "remote", r.RemoteAddr)
	ctx = context.WithValue(ctx, "scheme", r.Proto)
	ctx = context.WithValue(ctx, "local", r.Host)
	if ua := r.Header.Get("User-Agent"); ua != "" {
		ctx = context.WithValue(ctx, "User-Agent", ua)
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		ctx = context.WithValue(ctx, "Origin", origin)
	}

	w.Header().Set("content-type", contentType)
	codec := newHTTPServerConn(r, w)
	defer codec.close()
	var stream *jsoniter.Stream
	if !s.disableStreaming {
		stream = jsoniter.NewStream(jsoniter.ConfigDefault, w, 4096)
	}
	s.serveSingleRequest(ctx, codec, stream)
}

// validateRequest returns a non-zero response code and error message if the
// request is invalid.
func validateRequest(r *http.Request) (int, error) {
	if r.Method == http.MethodPut || r.Method == http.MethodDelete {
		return http.StatusMethodNotAllowed, errors.New("method not allowed")
	}
	if r.ContentLength > maxRequestContentLength {
		err := fmt.Errorf("content length too large (%d>%d)", r.ContentLength, maxRequestContentLength)
		return http.StatusRequestEntityTooLarge, err
	}
	// Allow OPTIONS and GET (regardless of content-type)
	if r.Method == http.MethodOptions || r.Method == http.MethodGet {
		return 0, nil
	}
	// Check content-type
	if mt, _, err := mime.ParseMediaType(r.Header.Get("content-type")); err == nil {
		for _, accepted := range acceptedContentTypes {
			if accepted == mt {
				return 0, nil
			}
		}
	}
	// Invalid content-type
	err := fmt.Errorf("invalid content type, only %s is supported", contentType)
	return http.StatusUnsupportedMediaType, err
}

func CheckJwtSecret(w http.ResponseWriter, r *http.Request, jwtSecret []byte) bool {
	var tokenStr string
	// Check if JWT signature is correct
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		tokenStr = strings.TrimPrefix(auth, "Bearer ")
	}

	if len(tokenStr) == 0 {
		http.Error(w, "missing token", http.StatusForbidden)
		return false
	}

	keyFunc := func(token *jwt.Token) (interface{}, error) {
		return jwtSecret, nil
	}
	claims := jwt.RegisteredClaims{}
	// We explicitly set only HS256 allowed, and also disables the
	// claim-check: the RegisteredClaims internally requires 'iat' to
	// be no later than 'now', but we allow for a bit of drift.
	token, err := jwt.ParseWithClaims(tokenStr, &claims, keyFunc,
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithoutClaimsValidation())

	switch {
	case err != nil:
		http.Error(w, err.Error(), http.StatusForbidden)
	case !token.Valid:
		http.Error(w, "invalid token", http.StatusForbidden)
	case !claims.VerifyExpiresAt(time.Now(), false): // optional
		http.Error(w, "token is expired", http.StatusForbidden)
	case claims.IssuedAt == nil:
		http.Error(w, "missing issued-at", http.StatusForbidden)
	case time.Since(claims.IssuedAt.Time) > jwtTokenExpiry:
		http.Error(w, "stale token", http.StatusForbidden)
	case time.Until(claims.IssuedAt.Time) > jwtTokenExpiry:
		http.Error(w, "future token", http.StatusForbidden)
	default:
		return true
	}

	return false
}

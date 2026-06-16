// Copyright 2026 The Erigon Authors
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

package httpreqresp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/golang/snappy"
)

const maxErrorMessageBytes = 256

var errMalformedErrorMessageLength = errors.New("malformed reqresp error message length")

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("SentinelHttp: %s", e.Body)
}

type PeerResponseError struct {
	Code    ResponseCode
	Message string
}

func (e *PeerResponseError) Error() string {
	return fmt.Sprintf("peer error code: %d (%s). Error message: %s", e.Code, e.Code.String(), e.Message)
}

type ResponseCode int

const (
	ResponseCodeSuccess ResponseCode = iota
	ResponseCodeInvalidRequest
	ResponseCodeServerError
	ResponseCodeResourceUnavailable
)

func (r ResponseCode) String() string {
	switch r {
	case ResponseCodeSuccess:
		return "success"
	case ResponseCodeInvalidRequest:
		return "invalid request"
	case ResponseCodeServerError:
		return "server error"
	case ResponseCodeResourceUnavailable:
		return "resource unavailable"
	}
	return "unknown"
}

func (r ResponseCode) Success() bool {
	return r == ResponseCodeSuccess
}

func (r ResponseCode) ErrorMessage(resp *http.Response) (string, error) {
	if r == ResponseCodeSuccess || r == ResponseCodeInvalidRequest {
		return "", nil
	}
	// Error response bodies are length-prefixed before the snappy payload.
	rawReader := bufio.NewReader(resp.Body)
	lengthDone := false
	for i := 0; i < 10; i++ {
		b, err := rawReader.ReadByte()
		if err != nil {
			return "", err
		}
		if b&0x80 == 0 {
			lengthDone = true
			break
		}
	}
	if !lengthDone {
		return "", errMalformedErrorMessageLength
	}
	sr := snappy.NewReader(rawReader)
	decoded, err := io.ReadAll(io.LimitReader(sr, maxErrorMessageBytes))
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

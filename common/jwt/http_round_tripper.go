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

package jwt

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

type HttpRoundTripper struct {
	base      http.RoundTripper
	jwtSecret []byte
}

func NewHttpRoundTripper(base http.RoundTripper, jwtSecret []byte) *HttpRoundTripper {
	return &HttpRoundTripper{base: base, jwtSecret: jwtSecret}
}

func (t *HttpRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iat": time.Now().Unix(),
	})

	tokenString, err := token.SignedString(t.jwtSecret)
	if err != nil {
		return nil, fmt.Errorf("JwtRoundTripper failed to produce a JWT token, err: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+tokenString)
	return t.base.RoundTrip(req)
}

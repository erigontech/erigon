package rpc_helper

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

type JWTRoundTripper struct {
	underlyingTransport http.RoundTripper
	jwtSecret           []byte
}

func NewJWTRoundTripper(jwtSecret []byte) *JWTRoundTripper {
	return &JWTRoundTripper{underlyingTransport: http.DefaultTransport, jwtSecret: jwtSecret}
}

func (t *JWTRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iat": time.Now().Unix(),
	})

	tokenString, err := token.SignedString(t.jwtSecret)
	if err != nil {
		return nil, fmt.Errorf("JwtRoundTripper failed to produce a JWT token, err: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+tokenString)
	return t.underlyingTransport.RoundTrip(req)
}

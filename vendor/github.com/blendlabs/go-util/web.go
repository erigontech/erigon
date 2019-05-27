package util

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"

	"github.com/blendlabs/go-exception"
)

// WriteJSON writes an object to a response as json.
func WriteJSON(w http.ResponseWriter, statusCode int, response interface{}) (int, error) {
	bytes, err := json.Marshal(response)
	if err == nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(statusCode)
		count, writeError := w.Write(bytes)
		if count == 0 {
			return count, exception.New("WriteJson : Didnt write any bytes.")
		}
		return count, writeError
	}
	return 0, err
}

// GetIP gets the origin/client ip for a request.
// X-FORWARDED-FOR is checked. If multiple IPs are included the first one is returned
// X-REAL-IP is checked. If multiple IPs are included the first one is returned
// Finally r.RemoteAddr is used
// Only benevolent services will allow access to the real IP
func GetIP(r *http.Request) string {
	tryHeader := func(key string) (string, bool) {
		if headerVal := r.Header.Get(key); len(headerVal) > 0 {
			if !strings.ContainsRune(headerVal, ',') {
				return headerVal, true
			}
			return strings.SplitN(headerVal, ",", 2)[0], true
		}
		return "", false
	}

	for _, header := range []string{"X-FORWARDED-FOR", "X-REAL-IP"} {
		if headerVal, ok := tryHeader(header); ok {
			return headerVal
		}
	}

	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

// GetParamByName returns a named parameter from either the querystring, the headers,
// the cookies, the form or the post form of a request.
func GetParamByName(r *http.Request, name string) string {
	//check querystring
	queryValue := r.URL.Query().Get(name)
	if !String.IsEmpty(queryValue) {
		return queryValue
	}

	//check headers
	headerValue := r.Header.Get(name)
	if !String.IsEmpty(headerValue) {
		return headerValue
	}

	//check cookies
	cookie, cookieErr := r.Cookie(name)
	if cookieErr == nil && !String.IsEmpty(cookie.Value) {
		return cookie.Value
	}

	formValue := r.Form.Get(name)
	if !String.IsEmpty(formValue) {
		return formValue
	}

	postFormValue := r.PostFormValue(name)
	if !String.IsEmpty(postFormValue) {
		return postFormValue
	}

	return StringEmpty
}

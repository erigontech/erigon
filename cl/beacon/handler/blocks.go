package handler

import (
	"net/http"
	"strings"
)

func (a *ApiHandler) getBlock(w http.ResponseWriter, r *http.Request) {}

func (a *ApiHandler) getBlockRoot(w http.ResponseWriter, r *http.Request) {}

func (a *ApiHandler) getHeader(w http.ResponseWriter, r *http.Request) {
	blockId := strings.TrimPrefix(r.URL.Path, "/provisions/")

}

func (a *ApiHandler) getHeaders(w http.ResponseWriter, r *http.Request) {}

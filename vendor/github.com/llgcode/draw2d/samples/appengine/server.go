// +build appengine

// Package gae demonstrates draw2d on a Google appengine server.
package gae

import (
	"fmt"
	"image"
	"image/png"
	"net/http"

	"github.com/llgcode/draw2d/draw2dimg"
	"github.com/llgcode/draw2d/draw2dpdf"
	"github.com/llgcode/draw2d/samples/android"

	"appengine"
)

type appError struct {
	Error   error
	Message string
	Code    int
}

type appHandler func(http.ResponseWriter, *http.Request) *appError

func (fn appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if e := fn(w, r); e != nil { // e is *appError, not os.Error.
		c := appengine.NewContext(r)
		c.Errorf("%v", e.Error)
		http.Error(w, e.Message, e.Code)
	}
}

func init() {
	http.Handle("/pdf", appHandler(pdf))
	http.Handle("/png", appHandler(imgPng))
}

func pdf(w http.ResponseWriter, r *http.Request) *appError {
	w.Header().Set("Content-type", "application/pdf")

	// Initialize the graphic context on an pdf document
	dest := draw2dpdf.NewPdf("L", "mm", "A4")
	gc := draw2dpdf.NewGraphicContext(dest)

	// Draw sample
	android.Draw(gc, 65, 0)

	err := dest.Output(w)
	if err != nil {
		return &appError{err, fmt.Sprintf("Can't write: %s", err), 500}
	}
	return nil
}

func imgPng(w http.ResponseWriter, r *http.Request) *appError {
	w.Header().Set("Content-type", "image/png")

	// Initialize the graphic context on an RGBA image
	dest := image.NewRGBA(image.Rect(0, 0, 297, 210.0))
	gc := draw2dimg.NewGraphicContext(dest)

	// Draw sample
	android.Draw(gc, 65, 0)

	err := png.Encode(w, dest)
	if err != nil {
		return &appError{err, fmt.Sprintf("Can't encode: %s", err), 500}
	}

	return nil
}

// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 21/11/2010 by Laurent Le Goff

// Package gopher2 draws a gopher avatar based on a svg of:
// https://github.com/golang-samples/gopher-vector/
package gopher2

import (
	"image"
	"image/color"
	"math"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dkit"
	"github.com/llgcode/draw2d/samples"
)

// Main draws a rotated face of the gopher. Afterwards it returns
// the filename. This should only be used during testing.
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	gc.SetStrokeColor(image.Black)
	gc.SetFillColor(image.White)
	gc.Save()
	// Draw a (partial) gopher
	gc.Translate(-60, 65)
	gc.Rotate(-30 * (math.Pi / 180.0))
	Draw(gc, 48, 48, 240, 72)
	gc.Restore()

	// Return the output filename
	return samples.Output("gopher2", ext), nil
}

// Draw a gopher head (not rotated)
func Draw(gc draw2d.GraphicContext, x, y, w, h float64) {
	h23 := (h * 2) / 3

	blf := color.RGBA{0, 0, 0, 0xff}          // black
	wf := color.RGBA{0xff, 0xff, 0xff, 0xff}  // white
	nf := color.RGBA{0x8B, 0x45, 0x13, 0xff}  // brown opaque
	brf := color.RGBA{0x8B, 0x45, 0x13, 0x99} // brown transparant
	brb := color.RGBA{0x8B, 0x45, 0x13, 0xBB} // brown transparant

	// round head top
	gc.MoveTo(x, y+h*1.002)
	gc.CubicCurveTo(x+w/4, y-h/3, x+3*w/4, y-h/3, x+w, y+h*1.002)
	gc.Close()
	gc.SetFillColor(brb)
	gc.Fill()

	// rectangle head bottom
	draw2dkit.RoundedRectangle(gc, x, y+h, x+w, y+h+h, h/5, h/5)
	gc.Fill()

	// left ear outside
	draw2dkit.Circle(gc, x, y+h, w/12)
	gc.SetFillColor(brf)
	gc.Fill()

	// left ear inside
	draw2dkit.Circle(gc, x, y+h, 0.5*w/12)
	gc.SetFillColor(nf)
	gc.Fill()

	// right ear outside
	draw2dkit.Circle(gc, x+w, y+h, w/12)
	gc.SetFillColor(brf)
	gc.Fill()

	// right ear inside
	draw2dkit.Circle(gc, x+w, y+h, 0.5*w/12)
	gc.SetFillColor(nf)
	gc.Fill()

	// left eye outside white
	draw2dkit.Circle(gc, x+w/3, y+h23, w/9)
	gc.SetFillColor(wf)
	gc.Fill()

	// left eye black
	draw2dkit.Circle(gc, x+w/3+w/24, y+h23, 0.5*w/9)
	gc.SetFillColor(blf)
	gc.Fill()

	// left eye inside white
	draw2dkit.Circle(gc, x+w/3+w/24+w/48, y+h23, 0.2*w/9)
	gc.SetFillColor(wf)
	gc.Fill()

	// right eye outside white
	draw2dkit.Circle(gc, x+w-w/3, y+h23, w/9)
	gc.Fill()

	// right eye black
	draw2dkit.Circle(gc, x+w-w/3+w/24, y+h23, 0.5*w/9)
	gc.SetFillColor(blf)
	gc.Fill()

	// right eye inside white
	draw2dkit.Circle(gc, x+w-(w/3)+w/24+w/48, y+h23, 0.2*w/9)
	gc.SetFillColor(wf)
	gc.Fill()

	// left tooth
	gc.SetFillColor(wf)
	draw2dkit.RoundedRectangle(gc, x+w/2-w/8, y+h+h/2.5, x+w/2-w/8+w/8, y+h+h/2.5+w/6, w/10, w/10)
	gc.Fill()

	// right tooth
	draw2dkit.RoundedRectangle(gc, x+w/2, y+h+h/2.5, x+w/2+w/8, y+h+h/2.5+w/6, w/10, w/10)
	gc.Fill()

	// snout
	draw2dkit.Ellipse(gc, x+(w/2), y+h+h/2.5, w/6, w/12)
	gc.SetFillColor(nf)
	gc.Fill()

	// nose
	draw2dkit.Ellipse(gc, x+(w/2), y+h+h/7, w/10, w/12)
	gc.SetFillColor(blf)
	gc.Fill()
}

// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 21/11/2010 by Laurent Le Goff

// Package gopher draws a gopher avatar based on a svg of:
// https://github.com/golang-samples/gopher-vector/
package gopher

import (
	"image/color"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/samples"
)

// Main draws a left hand and ear of a gopher. Afterwards it returns
// the filename. This should only be used during testing.
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	gc.Save()
	gc.Scale(0.5, 0.5)
	// Draw a (partial) gopher
	Draw(gc)
	gc.Restore()

	// Return the output filename
	return samples.Output("gopher", ext), nil
}

// Draw a left hand and ear of a gopher using a gc thanks to
// https://github.com/golang-samples/gopher-vector/
func Draw(gc draw2d.GraphicContext) {
	// Initialize Stroke Attribute
	gc.SetLineWidth(3)
	gc.SetLineCap(draw2d.RoundCap)
	gc.SetStrokeColor(color.Black)

	// Left hand
	// <path fill-rule="evenodd" clip-rule="evenodd" fill="#F6D2A2" stroke="#000000" stroke-width="3" stroke-linecap="round" d="
	// M10.634,300.493c0.764,15.751,16.499,8.463,23.626,3.539c6.765-4.675,8.743-0.789,9.337-10.015
	// c0.389-6.064,1.088-12.128,0.744-18.216c-10.23-0.927-21.357,1.509-29.744,7.602C10.277,286.542,2.177,296.561,10.634,300.493"/>
	gc.SetFillColor(color.RGBA{0xF6, 0xD2, 0xA2, 0xff})
	gc.MoveTo(10.634, 300.493)
	rCubicCurveTo(gc, 0.764, 15.751, 16.499, 8.463, 23.626, 3.539)
	rCubicCurveTo(gc, 6.765, -4.675, 8.743, -0.789, 9.337, -10.015)
	rCubicCurveTo(gc, 0.389, -6.064, 1.088, -12.128, 0.744, -18.216)
	rCubicCurveTo(gc, -10.23, -0.927, -21.357, 1.509, -29.744, 7.602)
	gc.CubicCurveTo(10.277, 286.542, 2.177, 296.561, 10.634, 300.493)
	gc.FillStroke()

	// <path fill-rule="evenodd" clip-rule="evenodd" fill="#C6B198" stroke="#000000" stroke-width="3" stroke-linecap="round" d="
	// M10.634,300.493c2.29-0.852,4.717-1.457,6.271-3.528"/>
	gc.MoveTo(10.634, 300.493)
	rCubicCurveTo(gc, 2.29, -0.852, 4.717, -1.457, 6.271, -3.528)
	gc.Stroke()

	// Left Ear
	// <path fill-rule="evenodd" clip-rule="evenodd" fill="#6AD7E5" stroke="#000000" stroke-width="3" stroke-linecap="round" d="
	// M46.997,112.853C-13.3,95.897,31.536,19.189,79.956,50.74L46.997,112.853z"/>
	gc.MoveTo(46.997, 112.853)
	gc.CubicCurveTo(-13.3, 95.897, 31.536, 19.189, 79.956, 50.74)
	gc.LineTo(46.997, 112.853)
	gc.Close()
	gc.Stroke()
}

func rQuadCurveTo(path draw2d.PathBuilder, dcx, dcy, dx, dy float64) {
	x, y := path.LastPoint()
	path.QuadCurveTo(x+dcx, y+dcy, x+dx, y+dy)
}

func rCubicCurveTo(path draw2d.PathBuilder, dcx1, dcy1, dcx2, dcy2, dx, dy float64) {
	x, y := path.LastPoint()
	path.CubicCurveTo(x+dcx1, y+dcy1, x+dcx2, y+dcy2, x+dx, y+dy)
}

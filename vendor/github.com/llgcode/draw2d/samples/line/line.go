// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 21/11/2010 by Laurent Le Goff, Stani Michiels

// Package line draws vertically spaced lines.
package line

import (
	"image/color"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dkit"
	"github.com/llgcode/draw2d/samples"
)

// Main draws vertically spaced lines and returns the filename.
// This should only be used during testing.
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	gc.SetFillRule(draw2d.FillRuleWinding)
	gc.Clear()
	// Draw the line
	for x := 5.0; x < 297; x += 10 {
		Draw(gc, x, 0, x, 210)
	}
	gc.ClearRect(100, 75, 197, 135)
	draw2dkit.Ellipse(gc, 148.5, 105, 35, 25)
	gc.SetFillColor(color.RGBA{0xff, 0xff, 0x44, 0xff})
	gc.FillStroke()

	// Return the output filename
	return samples.Output("line", ext), nil
}

// Draw vertically spaced lines
func Draw(gc draw2d.GraphicContext, x0, y0, x1, y1 float64) {
	// Draw a line
	gc.MoveTo(x0, y0)
	gc.LineTo(x1, y1)
	gc.Stroke()
}

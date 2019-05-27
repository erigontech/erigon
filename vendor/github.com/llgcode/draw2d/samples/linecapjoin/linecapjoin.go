// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 21/11/2010 by Laurent Le Goff

// Package linecapjoin demonstrates the different line caps and joins.
package linecapjoin

import (
	"image/color"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/samples"
)

// Main draws the different line caps and joins.
// This should only be used during testing.
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	// Draw the line
	const offset = 75.0
	x := 35.0
	caps := []draw2d.LineCap{draw2d.ButtCap, draw2d.SquareCap, draw2d.RoundCap}
	joins := []draw2d.LineJoin{draw2d.BevelJoin, draw2d.MiterJoin, draw2d.RoundJoin}
	for i := range caps {
		Draw(gc, caps[i], joins[i], x, 50, x, 160, offset)
		x += offset
	}

	// Return the output filename
	return samples.Output("linecapjoin", ext), nil
}

// Draw a line with an angle with specified line cap and join
func Draw(gc draw2d.GraphicContext, cap draw2d.LineCap, join draw2d.LineJoin,
	x0, y0, x1, y1, offset float64) {
	gc.SetLineCap(cap)
	gc.SetLineJoin(join)

	// Draw thick line
	gc.SetStrokeColor(color.NRGBA{0x33, 0x33, 0x33, 0xFF})
	gc.SetLineWidth(30.0)
	gc.MoveTo(x0, y0)
	gc.LineTo((x0+x1)/2+offset, (y0+y1)/2)
	gc.LineTo(x1, y1)
	gc.Stroke()

	// Draw thin helping line
	gc.SetStrokeColor(color.NRGBA{0xFF, 0x33, 0x33, 0xFF})
	gc.SetLineWidth(2.56)
	gc.MoveTo(x0, y0)
	gc.LineTo((x0+x1)/2+offset, (y0+y1)/2)
	gc.LineTo(x1, y1)
	gc.Stroke()
}

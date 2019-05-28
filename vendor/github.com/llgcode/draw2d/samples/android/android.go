// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 21/11/2010 by Laurent Le Goff

// Package android draws an android avatar.
package android

import (
	"image/color"
	"math"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dkit"
	"github.com/llgcode/draw2d/samples"
)

// Main draws a droid and returns the filename. This should only be
// used during testing.
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	// Draw the droid
	Draw(gc, 65, 0)

	// Return the output filename
	return samples.Output("android", ext), nil
}

// Draw the droid on a certain position.
func Draw(gc draw2d.GraphicContext, x, y float64) {
	// set the fill and stroke color of the droid
	gc.SetFillColor(color.RGBA{0x44, 0xff, 0x44, 0xff})
	gc.SetStrokeColor(color.RGBA{0x44, 0x44, 0x44, 0xff})

	// set line properties
	gc.SetLineCap(draw2d.RoundCap)
	gc.SetLineWidth(5)

	// head
	gc.MoveTo(x+30, y+70)
	gc.ArcTo(x+80, y+70, 50, 50, 180*(math.Pi/180), 180*(math.Pi/180))
	gc.Close()
	gc.FillStroke()
	gc.MoveTo(x+60, y+25)
	gc.LineTo(x+50, y+10)
	gc.MoveTo(x+100, y+25)
	gc.LineTo(x+110, y+10)
	gc.Stroke()

	// left eye
	draw2dkit.Circle(gc, x+60, y+45, 5)
	gc.FillStroke()

	// right eye
	draw2dkit.Circle(gc, x+100, y+45, 5)
	gc.FillStroke()

	// body
	draw2dkit.RoundedRectangle(gc, x+30, y+75, x+30+100, y+75+90, 10, 10)
	gc.FillStroke()
	draw2dkit.Rectangle(gc, x+30, y+75, x+30+100, y+75+80)
	gc.FillStroke()

	// left arm
	draw2dkit.RoundedRectangle(gc, x+5, y+80, x+5+20, y+80+70, 10, 10)
	gc.FillStroke()

	// right arm
	draw2dkit.RoundedRectangle(gc, x+135, y+80, x+135+20, y+80+70, 10, 10)
	gc.FillStroke()

	// left leg
	draw2dkit.RoundedRectangle(gc, x+50, y+150, x+50+20, y+150+50, 10, 10)
	gc.FillStroke()

	// right leg
	draw2dkit.RoundedRectangle(gc, x+90, y+150, x+90+20, y+150+50, 10, 10)
	gc.FillStroke()
}

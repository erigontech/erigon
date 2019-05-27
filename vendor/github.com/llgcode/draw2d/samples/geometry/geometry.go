// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 21/11/2010 by Laurent Le Goff

// Package geometry draws some geometric tests.
package geometry

import (
	"image"
	"image/color"
	"math"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dkit"
	"github.com/llgcode/draw2d/samples"
	"github.com/llgcode/draw2d/samples/gopher2"
)

// Main draws geometry and returns the filename. This should only be
// used during testing.
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	// Draw the droid
	Draw(gc, 297, 210)

	// Return the output filename
	return samples.Output("geometry", ext), nil
}

// Bubble draws a text balloon.
func Bubble(gc draw2d.GraphicContext, x, y, width, height float64) {
	sx, sy := width/100, height/100
	gc.MoveTo(x+sx*50, y)
	gc.QuadCurveTo(x, y, x, y+sy*37.5)
	gc.QuadCurveTo(x, y+sy*75, x+sx*25, y+sy*75)
	gc.QuadCurveTo(x+sx*25, y+sy*95, x+sx*5, y+sy*100)
	gc.QuadCurveTo(x+sx*35, y+sy*95, x+sx*40, y+sy*75)
	gc.QuadCurveTo(x+sx*100, y+sy*75, x+sx*100, y+sy*37.5)
	gc.QuadCurveTo(x+sx*100, y, x+sx*50, y)
	gc.Stroke()
}

// CurveRectangle draws a rectangle with bezier curves (not rounded rectangle).
func CurveRectangle(gc draw2d.GraphicContext, x0, y0,
	rectWidth, rectHeight float64, stroke, fill color.Color) {
	radius := (rectWidth + rectHeight) / 4

	x1 := x0 + rectWidth
	y1 := y0 + rectHeight
	if rectWidth/2 < radius {
		if rectHeight/2 < radius {
			gc.MoveTo(x0, (y0+y1)/2)
			gc.CubicCurveTo(x0, y0, x0, y0, (x0+x1)/2, y0)
			gc.CubicCurveTo(x1, y0, x1, y0, x1, (y0+y1)/2)
			gc.CubicCurveTo(x1, y1, x1, y1, (x1+x0)/2, y1)
			gc.CubicCurveTo(x0, y1, x0, y1, x0, (y0+y1)/2)
		} else {
			gc.MoveTo(x0, y0+radius)
			gc.CubicCurveTo(x0, y0, x0, y0, (x0+x1)/2, y0)
			gc.CubicCurveTo(x1, y0, x1, y0, x1, y0+radius)
			gc.LineTo(x1, y1-radius)
			gc.CubicCurveTo(x1, y1, x1, y1, (x1+x0)/2, y1)
			gc.CubicCurveTo(x0, y1, x0, y1, x0, y1-radius)
		}
	} else {
		if rectHeight/2 < radius {
			gc.MoveTo(x0, (y0+y1)/2)
			gc.CubicCurveTo(x0, y0, x0, y0, x0+radius, y0)
			gc.LineTo(x1-radius, y0)
			gc.CubicCurveTo(x1, y0, x1, y0, x1, (y0+y1)/2)
			gc.CubicCurveTo(x1, y1, x1, y1, x1-radius, y1)
			gc.LineTo(x0+radius, y1)
			gc.CubicCurveTo(x0, y1, x0, y1, x0, (y0+y1)/2)
		} else {
			gc.MoveTo(x0, y0+radius)
			gc.CubicCurveTo(x0, y0, x0, y0, x0+radius, y0)
			gc.LineTo(x1-radius, y0)
			gc.CubicCurveTo(x1, y0, x1, y0, x1, y0+radius)
			gc.LineTo(x1, y1-radius)
			gc.CubicCurveTo(x1, y1, x1, y1, x1-radius, y1)
			gc.LineTo(x0+radius, y1)
			gc.CubicCurveTo(x0, y1, x0, y1, x0, y1-radius)
		}
	}
	gc.Close()
	gc.SetStrokeColor(stroke)
	gc.SetFillColor(fill)
	gc.SetLineWidth(10.0)
	gc.FillStroke()
}

// Dash draws a line with a dash pattern
func Dash(gc draw2d.GraphicContext, x, y, width, height float64) {
	sx, sy := width/162, height/205
	gc.SetStrokeColor(image.Black)
	gc.SetLineDash([]float64{height / 10, height / 50, height / 50, height / 50}, -50.0)
	gc.SetLineCap(draw2d.ButtCap)
	gc.SetLineJoin(draw2d.RoundJoin)
	gc.SetLineWidth(height / 50)

	gc.MoveTo(x+sx*60.0, y)
	gc.LineTo(x+sx*60.0, y)
	gc.LineTo(x+sx*162, y+sy*205)
	rLineTo(gc, sx*-102.4, 0)
	gc.CubicCurveTo(x+sx*-17, y+sy*205, x+sx*-17, y+sy*103, x+sx*60.0, y+sy*103.0)
	gc.Stroke()
	gc.SetLineDash(nil, 0.0)
}

// Arc draws an arc with a positive angle (clockwise)
func Arc(gc draw2d.GraphicContext, xc, yc, width, height float64) {
	// draw an arc
	xc += width / 2
	yc += height / 2
	radiusX, radiusY := width/2, height/2
	startAngle := 45 * (math.Pi / 180.0) /* angles are specified */
	angle := 135 * (math.Pi / 180.0)     /* clockwise in radians           */
	gc.SetLineWidth(width / 10)
	gc.SetLineCap(draw2d.ButtCap)
	gc.SetStrokeColor(image.Black)
	gc.MoveTo(xc+math.Cos(startAngle)*radiusX, yc+math.Sin(startAngle)*radiusY)
	gc.ArcTo(xc, yc, radiusX, radiusY, startAngle, angle)
	gc.Stroke()

	// fill a circle
	gc.SetStrokeColor(color.NRGBA{255, 0x33, 0x33, 0x80})
	gc.SetFillColor(color.NRGBA{255, 0x33, 0x33, 0x80})
	gc.SetLineWidth(width / 20)

	gc.MoveTo(xc+math.Cos(startAngle)*radiusX, yc+math.Sin(startAngle)*radiusY)
	gc.LineTo(xc, yc)
	gc.LineTo(xc-radiusX, yc)
	gc.Stroke()

	gc.MoveTo(xc, yc)
	gc.ArcTo(xc, yc, width/10.0, height/10.0, 0, 2*math.Pi)
	gc.Fill()
}

// ArcNegative draws an arc with a negative angle (anti clockwise).
func ArcNegative(gc draw2d.GraphicContext, xc, yc, width, height float64) {
	xc += width / 2
	yc += height / 2
	radiusX, radiusY := width/2, height/2
	startAngle := 45.0 * (math.Pi / 180.0) /* angles are specified */
	angle := -225 * (math.Pi / 180.0)      /* clockwise in radians */
	gc.SetLineWidth(width / 10)
	gc.SetLineCap(draw2d.ButtCap)
	gc.SetStrokeColor(image.Black)

	gc.ArcTo(xc, yc, radiusX, radiusY, startAngle, angle)
	gc.Stroke()
	// fill a circle
	gc.SetStrokeColor(color.NRGBA{255, 0x33, 0x33, 0x80})
	gc.SetFillColor(color.NRGBA{255, 0x33, 0x33, 0x80})
	gc.SetLineWidth(width / 20)

	gc.MoveTo(xc+math.Cos(startAngle)*radiusX, yc+math.Sin(startAngle)*radiusY)
	gc.LineTo(xc, yc)
	gc.LineTo(xc-radiusX, yc)
	gc.Stroke()

	gc.ArcTo(xc, yc, width/10.0, height/10.0, 0, 2*math.Pi)
	gc.Fill()
}

// CubicCurve draws a cubic curve with its control points.
func CubicCurve(gc draw2d.GraphicContext, x, y, width, height float64) {
	sx, sy := width/162, height/205
	x0, y0 := x, y+sy*100.0
	x1, y1 := x+sx*75, y+sy*205
	x2, y2 := x+sx*125, y
	x3, y3 := x+sx*205, y+sy*100

	gc.SetStrokeColor(image.Black)
	gc.SetFillColor(color.NRGBA{0xAA, 0xAA, 0xAA, 0xFF})
	gc.SetLineWidth(width / 10)
	gc.MoveTo(x0, y0)
	gc.CubicCurveTo(x1, y1, x2, y2, x3, y3)
	gc.Stroke()

	gc.SetStrokeColor(color.NRGBA{0xFF, 0x33, 0x33, 0x88})

	gc.SetLineWidth(width / 20)
	// draw segment of curve
	gc.MoveTo(x0, y0)
	gc.LineTo(x1, y1)
	gc.LineTo(x2, y2)
	gc.LineTo(x3, y3)
	gc.Stroke()
}

// FillString draws a filled and stroked string.
// And filles/stroked path created from string. Which may have different - unselectable -  output in non raster gc implementations.
func FillString(gc draw2d.GraphicContext, x, y, width, height float64) {
	sx, sy := width/100, height/100
	gc.Save()
	gc.SetStrokeColor(image.Black)
	gc.SetLineWidth(1)
	draw2dkit.RoundedRectangle(gc, x+sx*5, y+sy*5, x+sx*95, y+sy*95, sx*10, sy*10)
	gc.FillStroke()
	gc.SetFillColor(image.Black)
	gc.SetFontSize(height / 6)
	gc.Translate(x+sx*6, y+sy*52)
	gc.SetFontData(draw2d.FontData{
		Name:   "luxi",
		Family: draw2d.FontFamilyMono,
		Style:  draw2d.FontStyleBold | draw2d.FontStyleItalic,
	})
	w := gc.FillString("Hug")
	gc.Translate(w+sx, 0)
	left, top, right, bottom := gc.GetStringBounds("cou")
	gc.SetStrokeColor(color.NRGBA{255, 0x33, 0x33, 0x80})
	draw2dkit.Rectangle(gc, left, top, right, bottom)
	gc.SetLineWidth(height / 50)
	gc.Stroke()
	gc.SetFillColor(color.NRGBA{0x33, 0x33, 0xff, 0xff})
	gc.SetStrokeColor(color.NRGBA{0x33, 0x33, 0xff, 0xff})
	gc.SetLineWidth(height / 100)
	gc.StrokeString("Hug")

	gc.Translate(-(w + sx), sy*24)
	w = gc.CreateStringPath("Hug", 0, 0)
	gc.Fill()
	gc.Translate(w+sx, 0)
	gc.CreateStringPath("Hug", 0, 0)
	path := gc.GetPath()
	gc.Stroke((&path).VerticalFlip())
	gc.Restore()
}

// FillStroke first fills and afterwards strokes a path.
func FillStroke(gc draw2d.GraphicContext, x, y, width, height float64) {
	sx, sy := width/210, height/215
	gc.MoveTo(x+sx*113.0, y)
	gc.LineTo(x+sx*215.0, y+sy*215)
	rLineTo(gc, sx*-100, 0)
	gc.CubicCurveTo(x+sx*35, y+sy*215, x+sx*35, y+sy*113, x+sx*113.0, y+sy*113)
	gc.Close()

	gc.MoveTo(x+sx*50.0, y)
	rLineTo(gc, sx*51.2, sy*51.2)
	rLineTo(gc, sx*-51.2, sy*51.2)
	rLineTo(gc, sx*-51.2, sy*-51.2)
	gc.Close()

	gc.SetLineWidth(width / 20.0)
	gc.SetFillColor(color.NRGBA{0, 0, 0xFF, 0xFF})
	gc.SetStrokeColor(image.Black)
	gc.FillStroke()
}

func rLineTo(path draw2d.PathBuilder, x, y float64) {
	x0, y0 := path.LastPoint()
	path.LineTo(x0+x, y0+y)
}

// FillStyle demonstrates the difference between even odd and non zero winding rule.
func FillStyle(gc draw2d.GraphicContext, x, y, width, height float64) {
	sx, sy := width/232, height/220
	gc.SetLineWidth(width / 40)

	draw2dkit.Rectangle(gc, x+sx*0, y+sy*12, x+sx*232, y+sy*70)

	var wheel1, wheel2 draw2d.Path
	wheel1.ArcTo(x+sx*52, y+sy*70, sx*40, sy*40, 0, 2*math.Pi)
	wheel2.ArcTo(x+sx*180, y+sy*70, sx*40, sy*40, 0, -2*math.Pi)

	gc.SetFillRule(draw2d.FillRuleEvenOdd)
	gc.SetFillColor(color.NRGBA{0, 0xB2, 0, 0xFF})

	gc.SetStrokeColor(image.Black)
	gc.FillStroke(&wheel1, &wheel2)

	draw2dkit.Rectangle(gc, x, y+sy*140, x+sx*232, y+sy*198)
	wheel1.Clear()
	wheel1.ArcTo(x+sx*52, y+sy*198, sx*40, sy*40, 0, 2*math.Pi)
	wheel2.Clear()
	wheel2.ArcTo(x+sx*180, y+sy*198, sx*40, sy*40, 0, -2*math.Pi)

	gc.SetFillRule(draw2d.FillRuleWinding)
	gc.SetFillColor(color.NRGBA{0, 0, 0xE5, 0xFF})
	gc.FillStroke(&wheel1, &wheel2)
}

// PathTransform scales a path differently in horizontal and vertical direction.
func PathTransform(gc draw2d.GraphicContext, x, y, width, height float64) {
	gc.Save()
	gc.SetLineWidth(width / 10)
	gc.Translate(x+width/2, y+height/2)
	gc.Scale(1, 4)
	gc.ArcTo(0, 0, width/8, height/8, 0, math.Pi*2)
	gc.Close()
	gc.Stroke()
	gc.Restore()
}

// Star draws many lines from a center.
func Star(gc draw2d.GraphicContext, x, y, width, height float64) {
	gc.Save()
	gc.Translate(x+width/2, y+height/2)
	gc.SetLineWidth(width / 40)
	for i := 0.0; i < 360; i = i + 10 { // Go from 0 to 360 degrees in 10 degree steps
		gc.Save()                        // Keep rotations temporary
		gc.Rotate(i * (math.Pi / 180.0)) // Rotate by degrees on stack from 'for'
		gc.MoveTo(0, 0)
		gc.LineTo(width/2, 0)
		gc.Stroke()
		gc.Restore()
	}
	gc.Restore()
}

// Draw all figures in a nice 4x3 grid.
func Draw(gc draw2d.GraphicContext, width, height float64) {
	mx, my := width*0.025, height*0.025 // margin
	dx, dy := (width-2*mx)/4, (height-2*my)/3
	w, h := dx-2*mx, dy-2*my
	x0, y := 2*mx, 2*my
	x := x0
	Bubble(gc, x, y, w, h)
	x += dx
	CurveRectangle(gc, x, y, w, h, color.NRGBA{0x80, 0, 0, 0x80}, color.NRGBA{0x80, 0x80, 0xFF, 0xFF})
	x += dx
	Dash(gc, x, y, w, h)
	x += dx
	Arc(gc, x, y, w, h)
	x = x0
	y += dy
	ArcNegative(gc, x, y, w, h)
	x += dx
	CubicCurve(gc, x, y, w, h)
	x += dx
	FillString(gc, x, y, w, h)
	x += dx
	FillStroke(gc, x, y, w, h)
	x = x0
	y += dy
	FillStyle(gc, x, y, w, h)
	x += dx
	PathTransform(gc, x, y, w, h)
	x += dx
	Star(gc, x, y, w, h)
	x += dx
	gopher2.Draw(gc, x, y, w, h/2)
}

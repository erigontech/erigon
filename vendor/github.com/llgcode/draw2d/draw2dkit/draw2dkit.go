// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

// Package draw2dkit provides helpers to draw common figures using a Path
package draw2dkit

import (
	"math"

	"github.com/llgcode/draw2d"
)

// Rectangle draws a rectangle using a path between (x1,y1) and (x2,y2)
func Rectangle(path draw2d.PathBuilder, x1, y1, x2, y2 float64) {
	path.MoveTo(x1, y1)
	path.LineTo(x2, y1)
	path.LineTo(x2, y2)
	path.LineTo(x1, y2)
	path.Close()
}

// RoundedRectangle draws a rectangle using a path between (x1,y1) and (x2,y2)
func RoundedRectangle(path draw2d.PathBuilder, x1, y1, x2, y2, arcWidth, arcHeight float64) {
	arcWidth = arcWidth / 2
	arcHeight = arcHeight / 2
	path.MoveTo(x1, y1+arcHeight)
	path.QuadCurveTo(x1, y1, x1+arcWidth, y1)
	path.LineTo(x2-arcWidth, y1)
	path.QuadCurveTo(x2, y1, x2, y1+arcHeight)
	path.LineTo(x2, y2-arcHeight)
	path.QuadCurveTo(x2, y2, x2-arcWidth, y2)
	path.LineTo(x1+arcWidth, y2)
	path.QuadCurveTo(x1, y2, x1, y2-arcHeight)
	path.Close()
}

// Ellipse draws an ellipse using a path with center (cx,cy) and radius (rx,ry)
func Ellipse(path draw2d.PathBuilder, cx, cy, rx, ry float64) {
	path.ArcTo(cx, cy, rx, ry, 0, -math.Pi*2)
	path.Close()
}

// Circle draws a circle using a path with center (cx,cy) and radius
func Circle(path draw2d.PathBuilder, cx, cy, radius float64) {
	path.ArcTo(cx, cy, radius, radius, 0, -math.Pi*2)
	path.Close()
}

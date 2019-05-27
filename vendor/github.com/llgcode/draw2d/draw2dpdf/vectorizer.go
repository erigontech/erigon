// Copyright 2015 The draw2d Authors. All rights reserved.
// created: 26/06/2015 by Stani Michiels

package draw2dpdf

// Vectorizer defines the minimal interface for gofpdf.Fpdf
// to be passed to a PathConvertor.
// It is also implemented by for example VertexMatrixTransform
type Vectorizer interface {
	// MoveTo creates a new subpath that start at the specified point
	MoveTo(x, y float64)
	// LineTo adds a line to the current subpath
	LineTo(x, y float64)
	// CurveTo adds a quadratic bezier curve to the current subpath
	CurveTo(cx, cy, x, y float64)
	// CurveTo adds a cubic bezier curve to the current subpath
	CurveBezierCubicTo(cx1, cy1, cx2, cy2, x, y float64)
	// ArcTo adds an arc to the current subpath
	ArcTo(x, y, rx, ry, degRotate, degStart, degEnd float64)
	// ClosePath closes the subpath
	ClosePath()
}

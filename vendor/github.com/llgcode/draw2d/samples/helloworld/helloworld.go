// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 21/11/2010 by Laurent Le Goff, Stani Michiels

// Package helloworld displays multiple "Hello World" (one rotated)
// in a rounded rectangle.
package helloworld

import (
	"fmt"
	"image"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dkit"
	"github.com/llgcode/draw2d/samples"
)

// Main draws "Hello World" and returns the filename. This should only be
// used during testing.
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	// Draw hello world
	Draw(gc, fmt.Sprintf("Hello World %d dpi", gc.GetDPI()))

	// Return the output filename
	return samples.Output("helloworld", ext), nil
}

// Draw "Hello World"
func Draw(gc draw2d.GraphicContext, text string) {
	// Draw a rounded rectangle using default colors
	draw2dkit.RoundedRectangle(gc, 5, 5, 135, 95, 10, 10)
	gc.FillStroke()

	// Set the font luximbi.ttf
	gc.SetFontData(draw2d.FontData{Name: "luxi", Family: draw2d.FontFamilyMono, Style: draw2d.FontStyleBold | draw2d.FontStyleItalic})
	// Set the fill text color to black
	gc.SetFillColor(image.Black)
	gc.SetFontSize(14)
	// Display Hello World
	gc.FillStringAt("Hello World", 8, 52)
}

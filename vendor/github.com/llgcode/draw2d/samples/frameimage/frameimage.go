// Copyright 2010 The draw2d Authors. All rights reserved.
// created: 21/11/2010 by Laurent Le Goff, Stani Michiels

// Package frameimage centers a png image and rotates it.
package frameimage

import (
	"math"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dimg"
	"github.com/llgcode/draw2d/draw2dkit"
	"github.com/llgcode/draw2d/samples"
)

// Main draws the image frame and returns the filename.
// This should only be used during testing.
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	// Margin between the image and the frame
	const margin = 30
	// Line width od the frame
	const lineWidth = 3

	// Gopher image
	gopher := samples.Resource("image", "gopher.png", ext)

	// Draw gopher
	err := Draw(gc, gopher, 297, 210, margin, lineWidth)

	// Return the output filename
	return samples.Output("frameimage", ext), err
}

// Draw the image frame with certain parameters.
func Draw(gc draw2d.GraphicContext, png string,
	dw, dh, margin, lineWidth float64) error {
	// Draw frame
	draw2dkit.RoundedRectangle(gc, lineWidth, lineWidth, dw-lineWidth, dh-lineWidth, 100, 100)
	gc.SetLineWidth(lineWidth)
	gc.FillStroke()

	// load the source image
	source, err := draw2dimg.LoadFromPngFile(png)
	if err != nil {
		return err
	}
	// Size of source image
	sw, sh := float64(source.Bounds().Dx()), float64(source.Bounds().Dy())
	// Draw image to fit in the frame
	// TODO Seems to have a transform bug here on draw image
	scale := math.Min((dw-margin*2)/sw, (dh-margin*2)/sh)
	gc.Save()
	gc.Translate((dw-sw*scale)/2, (dh-sh*scale)/2)
	gc.Scale(scale, scale)
	gc.Rotate(0.2)

	gc.DrawImage(source)
	gc.Restore()
	return nil
}

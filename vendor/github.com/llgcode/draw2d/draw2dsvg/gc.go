// Copyright 2015 The draw2d Authors. All rights reserved.
// created: 16/12/2017 by Drahoslav Bednář

package draw2dsvg

import (
		"github.com/golang/freetype/truetype"
	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dbase"
	"golang.org/x/image/font"
	"golang.org/x/image/math/fixed"
	"image"
	"log"
	"math"
	"strconv"
	"strings"
)

type drawType int

const (
	filled drawType = 1 << iota
	stroked
)

// GraphicContext implements the draw2d.GraphicContext interface
// It provides draw2d with a svg backend
type GraphicContext struct {
	*draw2dbase.StackGraphicContext
	FontCache  draw2d.FontCache
	glyphCache draw2dbase.GlyphCache
	glyphBuf   *truetype.GlyphBuf
	svg        *Svg
	DPI        int
}

func NewGraphicContext(svg *Svg) *GraphicContext {
	gc := &GraphicContext{
		draw2dbase.NewStackGraphicContext(),
		draw2d.GetGlobalFontCache(),
		draw2dbase.NewGlyphCache(),
		&truetype.GlyphBuf{},
		svg,
		92,
	}
	return gc
}

// Clear fills the current canvas with a default transparent color
func (gc *GraphicContext) Clear() {
	gc.svg.Groups = nil
}

// Stroke strokes the paths with the color specified by SetStrokeColor
func (gc *GraphicContext) Stroke(paths ...*draw2d.Path) {
	gc.drawPaths(stroked, paths...)
	gc.Current.Path.Clear()
}

// Fill fills the paths with the color specified by SetFillColor
func (gc *GraphicContext) Fill(paths ...*draw2d.Path) {
	gc.drawPaths(filled, paths...)
	gc.Current.Path.Clear()
}

// FillStroke first fills the paths and than strokes them
func (gc *GraphicContext) FillStroke(paths ...*draw2d.Path) {
	gc.drawPaths(filled|stroked, paths...)
	gc.Current.Path.Clear()
}

// FillString draws the text at point (0, 0)
func (gc *GraphicContext) FillString(text string) (cursor float64) {
	return gc.FillStringAt(text, 0, 0)
}

// FillStringAt draws the text at the specified point (x, y)
func (gc *GraphicContext) FillStringAt(text string, x, y float64) (cursor float64) {
	return gc.drawString(text, filled, x, y)
}

// StrokeString draws the contour of the text at point (0, 0)
func (gc *GraphicContext) StrokeString(text string) (cursor float64) {
	return gc.StrokeStringAt(text, 0, 0)
}

// StrokeStringAt draws the contour of the text at point (x, y)
func (gc *GraphicContext) StrokeStringAt(text string, x, y float64) (cursor float64) {
	return gc.drawString(text, stroked, x, y)
}

// Save the context and push it to the context stack
func (gc *GraphicContext) Save() {
	gc.StackGraphicContext.Save()
	// TODO use common transformation group for multiple elements
}

// Restore remove the current context and restore the last one
func (gc *GraphicContext) Restore() {
	gc.StackGraphicContext.Restore()
	// TODO use common transformation group for multiple elements
}

func (gc *GraphicContext) SetDPI(dpi int) {
	gc.DPI = dpi
	gc.recalc()
}

func (gc *GraphicContext) GetDPI() int {
	return gc.DPI
}

// SetFont sets the font used to draw text.
func (gc *GraphicContext) SetFont(font *truetype.Font) {
	gc.Current.Font = font
}

// SetFontSize sets the font size in points (as in “a 12 point font”).
func (gc *GraphicContext) SetFontSize(fontSize float64) {
	gc.Current.FontSize = fontSize
	gc.recalc()
}

// DrawImage draws the raster image in the current canvas
func (gc *GraphicContext) DrawImage(image image.Image) {
	bounds := image.Bounds()

	svgImage := &Image{Href: imageToSvgHref(image)}
	svgImage.X = float64(bounds.Min.X)
	svgImage.Y = float64(bounds.Min.Y)
	svgImage.Width = toSvgLength(float64(bounds.Max.X - bounds.Min.X))
	svgImage.Height = toSvgLength(float64(bounds.Max.Y - bounds.Min.Y))
	gc.newGroup(0).Image = svgImage
}

// ClearRect fills the specified rectangle with a default transparent color
func (gc *GraphicContext) ClearRect(x1, y1, x2, y2 int) {
	mask := gc.newMask(x1, y1, x2-x1, y2-y1)

	newGroup := &Group{
		Groups: gc.svg.Groups,
		Mask:   "url(#" + mask.Id + ")",
	}

	// replace groups with new masked group
	gc.svg.Groups = []*Group{newGroup}
}

// NOTE following  two functions and soe other further below copied from dwra2d{img|gl}
// TODO move them all to common draw2dbase?

// CreateStringPath creates a path from the string s at x, y, and returns the string width.
// The text is placed so that the left edge of the em square of the first character of s
// and the baseline intersect at x, y. The majority of the affected pixels will be
// above and to the right of the point, but some may be below or to the left.
// For example, drawing a string that starts with a 'J' in an italic font may
// affect pixels below and left of the point.
func (gc *GraphicContext) CreateStringPath(s string, x, y float64) (cursor float64) {
	f, err := gc.loadCurrentFont()
	if err != nil {
		log.Println(err)
		return 0.0
	}
	startx := x
	prev, hasPrev := truetype.Index(0), false
	for _, rune := range s {
		index := f.Index(rune)
		if hasPrev {
			x += fUnitsToFloat64(f.Kern(fixed.Int26_6(gc.Current.Scale), prev, index))
		}
		err := gc.drawGlyph(index, x, y)
		if err != nil {
			log.Println(err)
			return startx - x
		}
		x += fUnitsToFloat64(f.HMetric(fixed.Int26_6(gc.Current.Scale), index).AdvanceWidth)
		prev, hasPrev = index, true
	}

	return x - startx
}

// GetStringBounds returns the approximate pixel bounds of the string s at x, y.
// The the left edge of the em square of the first character of s
// and the baseline intersect at 0, 0 in the returned coordinates.
// Therefore the top and left coordinates may well be negative.
func (gc *GraphicContext) GetStringBounds(s string) (left, top, right, bottom float64) {
	f, err := gc.loadCurrentFont()
	if err != nil {
		log.Println(err)
		return 0, 0, 0, 0
	}
	if gc.Current.Scale == 0 {
		panic("zero scale")
	}
	top, left, bottom, right = 10e6, 10e6, -10e6, -10e6
	cursor := 0.0
	prev, hasPrev := truetype.Index(0), false
	for _, rune := range s {
		index := f.Index(rune)
		if hasPrev {
			cursor += fUnitsToFloat64(f.Kern(fixed.Int26_6(gc.Current.Scale), prev, index))
		}
		if err := gc.glyphBuf.Load(gc.Current.Font, fixed.Int26_6(gc.Current.Scale), index, font.HintingNone); err != nil {
			log.Println(err)
			return 0, 0, 0, 0
		}
		e0 := 0
		for _, e1 := range gc.glyphBuf.Ends {
			ps := gc.glyphBuf.Points[e0:e1]
			for _, p := range ps {
				x, y := pointToF64Point(p)
				top = math.Min(top, y)
				bottom = math.Max(bottom, y)
				left = math.Min(left, x+cursor)
				right = math.Max(right, x+cursor)
			}
		}
		cursor += fUnitsToFloat64(f.HMetric(fixed.Int26_6(gc.Current.Scale), index).AdvanceWidth)
		prev, hasPrev = index, true
	}
	return left, top, right, bottom
}

////////////////////
// private funcitons

func (gc *GraphicContext) drawPaths(drawType drawType, paths ...*draw2d.Path) {
	// create elements
	svgPath := Path{}
	group := gc.newGroup(drawType)

	// set attrs to path element
	paths = append(paths, gc.Current.Path)
	svgPathsDesc := make([]string, len(paths))
	// multiple pathes has to be joined to single svg path description
	// because fill-rule wont work for whole group as excepted
	for i, path := range paths {
		svgPathsDesc[i] = toSvgPathDesc(path)
	}
	svgPath.Desc = strings.Join(svgPathsDesc, " ")

	// attach to group
	group.Paths = []*Path{&svgPath}
}

// Add text element to svg and returns its expected width
func (gc *GraphicContext) drawString(text string, drawType drawType, x, y float64) float64 {
	switch gc.svg.FontMode {
	case PathFontMode:
		w := gc.CreateStringPath(text, x, y)
		gc.drawPaths(drawType)
		gc.Current.Path.Clear()
		return w
	case SvgFontMode:
		gc.embedSvgFont(text)
	}

	// create elements
	svgText := Text{}
	group := gc.newGroup(drawType)

	// set attrs to text element
	svgText.Text = text
	svgText.FontSize = gc.Current.FontSize
	svgText.X = x
	svgText.Y = y
	svgText.FontFamily = gc.Current.FontData.Name

	// attach to group
	group.Texts = []*Text{&svgText}
	left, _, right, _ := gc.GetStringBounds(text)
	return right - left
}

// Creates new group from current context
// attach it to svg and return
func (gc *GraphicContext) newGroup(drawType drawType) *Group {
	group := Group{}
	// set attrs to group
	if drawType&stroked == stroked {
		group.Stroke = toSvgRGBA(gc.Current.StrokeColor)
		group.StrokeWidth = toSvgLength(gc.Current.LineWidth)
		group.StrokeLinecap = gc.Current.Cap.String()
		group.StrokeLinejoin = gc.Current.Join.String()
		if len(gc.Current.Dash) > 0 {
			group.StrokeDasharray = toSvgArray(gc.Current.Dash)
			group.StrokeDashoffset = toSvgLength(gc.Current.DashOffset)
		}
	}

	if drawType&filled == filled {
		group.Fill = toSvgRGBA(gc.Current.FillColor)
		group.FillRule = toSvgFillRule(gc.Current.FillRule)
	}

	group.Transform = toSvgTransform(gc.Current.Tr)

	// attach
	gc.svg.Groups = append(gc.svg.Groups, &group)

	return &group
}

// creates new mask attached to svg
func (gc *GraphicContext) newMask(x, y, width, height int) *Mask {
	mask := &Mask{}
	mask.X = float64(x)
	mask.Y = float64(y)
	mask.Width = toSvgLength(float64(width))
	mask.Height = toSvgLength(float64(height))

	// attach mask
	gc.svg.Masks = append(gc.svg.Masks, mask)
	mask.Id = "mask-" + strconv.Itoa(len(gc.svg.Masks))
	return mask
}

// Embed svg font definition to svg tree itself
// Or update existing if already exists for curent font data
func (gc *GraphicContext) embedSvgFont(text string) *Font {
	fontName := gc.Current.FontData.Name
	gc.loadCurrentFont()

	// find or create font Element
	svgFont := (*Font)(nil)
	for _, font := range gc.svg.Fonts {
		if font.Name == fontName {
			svgFont = font
			break
		}
	}
	if svgFont == nil {
		// create new
		svgFont = &Font{}
		// and attach
		gc.svg.Fonts = append(gc.svg.Fonts, svgFont)
	}

	// fill with glyphs

	gc.Save()
	defer gc.Restore()
	gc.SetFontSize(2048)
	defer gc.SetDPI(gc.GetDPI())
	gc.SetDPI(92)
filling:
	for _, rune := range text {
		for _, g := range svgFont.Glyphs {
			if g.Rune == Rune(rune) {
				continue filling
			}
		}
		glyph := gc.glyphCache.Fetch(gc, gc.GetFontName(), rune)
		// glyphCache.Load indirectly calls CreateStringPath for single rune string

		glypPath := glyph.Path.VerticalFlip() // svg font glyphs have oposite y axe
		svgFont.Glyphs = append(svgFont.Glyphs, &Glyph{
			Rune:      Rune(rune),
			Desc:      toSvgPathDesc(glypPath),
			HorizAdvX: glyph.Width,
		})
	}

	// set attrs
	svgFont.Id = "font-" + strconv.Itoa(len(gc.svg.Fonts))
	svgFont.Name = fontName

	// TODO use css @font-face with id instead of this
	svgFont.Face = &Face{Family: fontName, Units: 2048, HorizAdvX: 2048}
	return svgFont
}

func (gc *GraphicContext) loadCurrentFont() (*truetype.Font, error) {
	font, err := gc.FontCache.Load(gc.Current.FontData)
	if err != nil {
		font, err = gc.FontCache.Load(draw2dbase.DefaultFontData)
	}
	if font != nil {
		gc.SetFont(font)
		gc.SetFontSize(gc.Current.FontSize)
	}
	return font, err
}

func (gc *GraphicContext) drawGlyph(glyph truetype.Index, dx, dy float64) error {
	if err := gc.glyphBuf.Load(gc.Current.Font, fixed.Int26_6(gc.Current.Scale), glyph, font.HintingNone); err != nil {
		return err
	}
	e0 := 0
	for _, e1 := range gc.glyphBuf.Ends {
		DrawContour(gc, gc.glyphBuf.Points[e0:e1], dx, dy)
		e0 = e1
	}
	return nil
}

// recalc recalculates scale and bounds values from the font size, screen
// resolution and font metrics, and invalidates the glyph cache.
func (gc *GraphicContext) recalc() {
	gc.Current.Scale = gc.Current.FontSize * float64(gc.DPI) * (64.0 / 72.0)
}

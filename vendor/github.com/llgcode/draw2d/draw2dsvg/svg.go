// Copyright 2015 The draw2d Authors. All rights reserved.
// created: 16/12/2017 by Drahoslav Bednář

package draw2dsvg

import (
	"encoding/xml"
)

/* svg elements */

type FontMode int

// Modes of font handling in svg
const (
	// Does nothing special
	// Makes sense only for common system fonts
	SysFontMode FontMode = 1 << iota

	// Links font files in css def
	// Requires distribution of font files with outputed svg
	LinkFontMode // TODO implement

	// Embeds glyphs definition in svg file itself in svg font format
	// Has poor browser support
	SvgFontMode

	// Embeds font definiton in svg file itself in woff format as part of css def
	CssFontMode // TODO implement

	// Converts texts to paths
	PathFontMode
)

type Svg struct {
	XMLName  xml.Name `xml:"svg"`
	Xmlns    string   `xml:"xmlns,attr"`
	Fonts    []*Font  `xml:"defs>font"`
	Masks    []*Mask  `xml:"defs>mask"`
	Groups   []*Group `xml:"g"`
	FontMode FontMode `xml:"-"`
	FillStroke
}

func NewSvg() *Svg {
	return &Svg{
		Xmlns:      "http://www.w3.org/2000/svg",
		FillStroke: FillStroke{Fill: "none", Stroke: "none"},
		FontMode:   PathFontMode,
	}
}

type Group struct {
	FillStroke
	Transform string   `xml:"transform,attr,omitempty"`
	Groups    []*Group `xml:"g"`
	Paths     []*Path  `xml:"path"`
	Texts     []*Text  `xml:"text"`
	Image     *Image   `xml:"image"`
	Mask      string   `xml:"mask,attr,omitempty"`
}

type Path struct {
	FillStroke
	Desc string `xml:"d,attr"`
}

type Text struct {
	FillStroke
	Position
	FontSize   float64 `xml:"font-size,attr,omitempty"`
	FontFamily string  `xml:"font-family,attr,omitempty"`
	Text       string  `xml:",innerxml"`
	Style      string  `xml:"style,attr,omitempty"`
}

type Image struct {
	Position
	Dimension
	Href string `xml:"href,attr"`
}

type Mask struct {
	Identity
	Position
	Dimension
}

type Rect struct {
	Position
	Dimension
	FillStroke
}

func (m Mask) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	bigRect := Rect{}
	bigRect.X, bigRect.Y = 0, 0
	bigRect.Width, bigRect.Height = "100%", "100%"
	bigRect.Fill = "#fff"
	rect := Rect{}
	rect.X, rect.Y = m.X, m.Y
	rect.Width, rect.Height = m.Width, m.Height
	rect.Fill = "#000"

	return e.EncodeElement(struct {
		XMLName xml.Name `xml:"mask"`
		Rects   [2]Rect  `xml:"rect"`
		Id      string   `xml:"id,attr"`
	}{
		Rects: [2]Rect{bigRect, rect},
		Id:    m.Id,
	}, start)
}

/* font related elements */

type Font struct {
	Identity
	Face   *Face    `xml:"font-face"`
	Glyphs []*Glyph `xml:"glyph"`
}

type Face struct {
	Family    string  `xml:"font-family,attr"`
	Units     int     `xml:"units-per-em,attr"`
	HorizAdvX float64 `xml:"horiz-adv-x,attr"`
	// TODO add other attrs, like style, variant, weight...
}

type Glyph struct {
	Rune      Rune    `xml:"unicode,attr"`
	Desc      string  `xml:"d,attr"`
	HorizAdvX float64 `xml:"horiz-adv-x,attr"`
}

type Rune rune

func (r Rune) MarshalXMLAttr(name xml.Name) (xml.Attr, error) {
	return xml.Attr{
		Name:  name,
		Value: string(rune(r)),
	}, nil
}

/* shared attrs */

type Identity struct {
	Id   string `xml:"id,attr"`
	Name string `xml:"name,attr"`
}

type Position struct {
	X float64 `xml:"x,attr,omitempty"`
	Y float64 `xml:"y,attr,omitempty"`
}

type Dimension struct {
	Width  string `xml:"width,attr"`
	Height string `xml:"height,attr"`
}

type FillStroke struct {
	Fill     string `xml:"fill,attr,omitempty"`
	FillRule string `xml:"fill-rule,attr,omitempty"`

	Stroke           string `xml:"stroke,attr,omitempty"`
	StrokeWidth      string `xml:"stroke-width,attr,omitempty"`
	StrokeLinecap    string `xml:"stroke-linecap,attr,omitempty"`
	StrokeLinejoin   string `xml:"stroke-linejoin,attr,omitempty"`
	StrokeDasharray  string `xml:"stroke-dasharray,attr,omitempty"`
	StrokeDashoffset string `xml:"stroke-dashoffset,attr,omitempty"`
}

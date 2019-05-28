// Copyright 2015 The draw2d Authors. All rights reserved.
// created: 16/12/2017 by Drahoslav Bednářpackage draw2dsvg

package draw2dsvg

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/llgcode/draw2d"
	"image"
	"image/color"
	"image/png"
	"math"
	"strconv"
	"strings"
)

func toSvgRGBA(c color.Color) string {
	r, g, b, a := c.RGBA()
	r, g, b, a = r>>8, g>>8, b>>8, a>>8
	if a == 255 {
		return optiSprintf("#%02X%02X%02X", r, g, b)
	}
	return optiSprintf("rgba(%v,%v,%v,%f)", r, g, b, float64(a)/255)
}

func toSvgLength(l float64) string {
	if math.IsInf(l, 1) {
		return "100%"
	}
	return optiSprintf("%f", l)
}

func toSvgArray(nums []float64) string {
	arr := make([]string, len(nums))
	for i, num := range nums {
		arr[i] = optiSprintf("%f", num)
	}
	return strings.Join(arr, ",")
}

func toSvgFillRule(rule draw2d.FillRule) string {
	return map[draw2d.FillRule]string{
		draw2d.FillRuleEvenOdd: "evenodd",
		draw2d.FillRuleWinding: "nonzero",
	}[rule]
}

func toSvgPathDesc(p *draw2d.Path) string {
	parts := make([]string, len(p.Components))
	ps := p.Points
	for i, cmp := range p.Components {
		switch cmp {
		case draw2d.MoveToCmp:
			parts[i] = optiSprintf("M %f,%f", ps[0], ps[1])
			ps = ps[2:]
		case draw2d.LineToCmp:
			parts[i] = optiSprintf("L %f,%f", ps[0], ps[1])
			ps = ps[2:]
		case draw2d.QuadCurveToCmp:
			parts[i] = optiSprintf("Q %f,%f %f,%f", ps[0], ps[1], ps[2], ps[3])
			ps = ps[4:]
		case draw2d.CubicCurveToCmp:
			parts[i] = optiSprintf("C %f,%f %f,%f %f,%f", ps[0], ps[1], ps[2], ps[3], ps[4], ps[5])
			ps = ps[6:]
		case draw2d.ArcToCmp:
			cx, cy := ps[0], ps[1] // center
			rx, ry := ps[2], ps[3] // radii
			fi := ps[4] + ps[5]    // startAngle + angle

			// compute endpoint
			sinfi, cosfi := math.Sincos(fi)
			nom := math.Hypot(ry*cosfi, rx*sinfi)
			x := cx + (rx*ry*cosfi)/nom
			y := cy + (rx*ry*sinfi)/nom

			// compute large and sweep flags
			large := 0
			sweep := 0
			if math.Abs(ps[5]) > math.Pi {
				large = 1
			}
			if !math.Signbit(ps[5]) {
				sweep = 1
			}
			// dirty hack to ensure whole arc is drawn
			// if start point equals end point
			if sweep == 1 {
				x += 0.01 * sinfi
				y += 0.01 * -cosfi
			} else {
				x += 0.01 * sinfi
				y += 0.01 * cosfi
			}

			// rx ry x-axis-rotation large-arc-flag sweep-flag x y
			parts[i] = optiSprintf("A %f %f %v %v %v %F %F",
				rx, ry, 0, large, sweep, x, y,
			)
			ps = ps[6:]
		case draw2d.CloseCmp:
			parts[i] = "Z"
		}
	}
	return strings.Join(parts, " ")
}

func toSvgTransform(mat draw2d.Matrix) string {
	if mat.IsIdentity() {
		return ""
	}
	if mat.IsTranslation() {
		x, y := mat.GetTranslation()
		return optiSprintf("translate(%f,%f)", x, y)
	}
	return optiSprintf("matrix(%f,%f,%f,%f,%f,%f)",
		mat[0], mat[1], mat[2], mat[3], mat[4], mat[5],
	)
}

func imageToSvgHref(image image.Image) string {
	out := "data:image/png;base64,"
	pngBuf := &bytes.Buffer{}
	png.Encode(pngBuf, image)
	out += base64.RawStdEncoding.EncodeToString(pngBuf.Bytes())
	return out
}

// Do the same thing as fmt.Sprintf
// except it uses the optimal precition for floats: (0-3) for f and (0-6) for F
// eg.:
// optiSprintf("%f", 3.0)               => fmt.Sprintf("%.0f", 3.0)
// optiSprintf("%f", 3.33)              => fmt.Sprintf("%.2f", 3.33)
// optiSprintf("%f", 3.3001)            => fmt.Sprintf("%.1f", 3.3001)
// optiSprintf("%f", 3.333333333333333) => fmt.Sprintf("%.3f", 3.333333333333333)
// optiSprintf("%F", 3.333333333333333) => fmt.Sprintf("%.6f", 3.333333333333333)
func optiSprintf(format string, a ...interface{}) string {
	chunks := strings.Split(format, "%")
	newChunks := make([]string, len(chunks))
	for i, chunk := range chunks {
		if i != 0 {
			verb := chunk[0]
			if verb == 'f' || verb == 'F' {
				num := a[i-1].(float64)
				p := strconv.Itoa(getPrec(num, verb == 'F'))
				chunk = strings.Replace(chunk, string(verb), "."+p+"f", 1)
			}
		}
		newChunks[i] = chunk
	}
	format = strings.Join(newChunks, "%")
	return fmt.Sprintf(format, a...)
}

// TODO needs test, since it is not quiet right
func getPrec(num float64, better bool) int {
	max := 3
	eps := 0.0005
	if better {
		max = 6
		eps = 0.0000005
	}
	prec := 0
	for math.Mod(num, 1) > eps {
		num *= 10
		eps *= 10
		prec++
	}

	if max < prec {
		return max
	}
	return prec
}

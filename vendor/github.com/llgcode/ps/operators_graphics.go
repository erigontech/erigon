// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

// Package ps
package ps

import (
	"image/color"
	"log"
	"math"

	"github.com/llgcode/draw2d"
)

//Path Construction Operators
func newpath(interpreter *Interpreter) {
	interpreter.GetGraphicContext().BeginPath()
}

func closepath(interpreter *Interpreter) {
	interpreter.GetGraphicContext().Close()
}

func currentpoint(interpreter *Interpreter) {
	x, y := interpreter.GetGraphicContext().LastPoint()
	interpreter.Push(x)
	interpreter.Push(y)
}

func moveto(interpreter *Interpreter) {
	y := interpreter.PopFloat()
	x := interpreter.PopFloat()
	interpreter.GetGraphicContext().MoveTo(x, y)
}

func rmoveto(interpreter *Interpreter) {
	y := interpreter.PopFloat()
	x := interpreter.PopFloat()
	sx, sy := interpreter.GetGraphicContext().LastPoint()
	interpreter.GetGraphicContext().MoveTo(sx+x, sy+y)
}

func lineto(interpreter *Interpreter) {
	y := interpreter.PopFloat()
	x := interpreter.PopFloat()
	interpreter.GetGraphicContext().LineTo(x, y)
}

func rlineto(interpreter *Interpreter) {
	y := interpreter.PopFloat()
	x := interpreter.PopFloat()
	sx, sy := interpreter.GetGraphicContext().LastPoint()
	interpreter.GetGraphicContext().LineTo(sx+x, sy+y)
}

func curveto(interpreter *Interpreter) {
	cy3 := interpreter.PopFloat()
	cx3 := interpreter.PopFloat()
	cy2 := interpreter.PopFloat()
	cx2 := interpreter.PopFloat()
	cy1 := interpreter.PopFloat()
	cx1 := interpreter.PopFloat()
	interpreter.GetGraphicContext().CubicCurveTo(cx1, cy1, cx2, cy2, cx3, cy3)
}

func rcurveto(interpreter *Interpreter) {
	cy3 := interpreter.PopFloat()
	cx3 := interpreter.PopFloat()
	cy2 := interpreter.PopFloat()
	cx2 := interpreter.PopFloat()
	cy1 := interpreter.PopFloat()
	cx1 := interpreter.PopFloat()
	sx, sy := interpreter.GetGraphicContext().LastPoint()
	interpreter.GetGraphicContext().CubicCurveTo(sx+cx1, sy+cy1, sx+cx2, sy+cy2, sx+cx3, sy+cy3)
}

func arc(interpreter *Interpreter) {
	angle2 := interpreter.PopFloat() * (math.Pi / 180.0)
	angle1 := interpreter.PopFloat() * (math.Pi / 180.0)
	r := interpreter.PopFloat()
	y := interpreter.PopFloat()
	x := interpreter.PopFloat()
	interpreter.GetGraphicContext().ArcTo(x, y, r, r, angle1, angle2-angle1)
}

func clippath(interpreter *Interpreter) {
	//log.Printf("clippath not yet implemented")
}

func stroke(interpreter *Interpreter) {
	interpreter.GetGraphicContext().Stroke()
}

func fill(interpreter *Interpreter) {
	interpreter.GetGraphicContext().Fill()
}

func gsave(interpreter *Interpreter) {
	interpreter.GetGraphicContext().Save()
}

func grestore(interpreter *Interpreter) {
	interpreter.GetGraphicContext().Restore()
}

func setgray(interpreter *Interpreter) {
	gray := interpreter.PopFloat()
	color := color.RGBA{uint8(gray * 0xff), uint8(gray * 0xff), uint8(gray * 0xff), 0xff}
	interpreter.GetGraphicContext().SetStrokeColor(color)
	interpreter.GetGraphicContext().SetFillColor(color)
}

func setrgbcolor(interpreter *Interpreter) {
	blue := interpreter.PopFloat()
	green := interpreter.PopFloat()
	red := interpreter.PopFloat()
	color := color.RGBA{uint8(red * 0xff), uint8(green * 0xff), uint8(blue * 0xff), 0xff}
	interpreter.GetGraphicContext().SetStrokeColor(color)
	interpreter.GetGraphicContext().SetFillColor(color)
}

func hsbtorgb(hue, saturation, brightness float64) (red, green, blue int) {
	var fr, fg, fb float64
	if saturation == 0 {
		fr, fg, fb = brightness, brightness, brightness
	} else {
		H := (hue - math.Floor(hue)) * 6
		I := int(math.Floor(H))
		F := H - float64(I)
		M := brightness * (1 - saturation)
		N := brightness * (1 - saturation*F)
		K := brightness * (1 - saturation*(1-F))

		switch I {
		case 0:
			fr = brightness
			fg = K
			fb = M
		case 1:
			fr = N
			fg = brightness
			fb = M
		case 2:
			fr = M
			fg = brightness
			fb = K
		case 3:
			fr = M
			fg = N
			fb = brightness
		case 4:
			fr = K
			fg = M
			fb = brightness
		case 5:
			fr = brightness
			fg = M
			fb = N
		default:
			fr, fb, fg = 0, 0, 0
		}
	}

	red = int(fr*255. + 0.5)
	green = int(fg*255. + 0.5)
	blue = int(fb*255. + 0.5)
	return
}

func sethsbcolor(interpreter *Interpreter) {
	brightness := interpreter.PopFloat()
	saturation := interpreter.PopFloat()
	hue := interpreter.PopFloat()
	red, green, blue := hsbtorgb(hue, saturation, brightness)
	color := color.RGBA{uint8(red), uint8(green), uint8(blue), 0xff}
	interpreter.GetGraphicContext().SetStrokeColor(color)
	interpreter.GetGraphicContext().SetFillColor(color)
}

func setcmybcolor(interpreter *Interpreter) {
	black := interpreter.PopFloat()
	yellow := interpreter.PopFloat()
	magenta := interpreter.PopFloat()
	cyan := interpreter.PopFloat()

	/*  cyan = cyan / 255.0;
	    magenta = magenta / 255.0;
	    yellow = yellow / 255.0;
	    black = black / 255.0;   */

	red := cyan*(1.0-black) + black
	green := magenta*(1.0-black) + black
	blue := yellow*(1.0-black) + black

	red = (1.0-red)*255.0 + 0.5
	green = (1.0-green)*255.0 + 0.5
	blue = (1.0-blue)*255.0 + 0.5

	color := color.RGBA{uint8(red), uint8(green), uint8(blue), 0xff}
	interpreter.GetGraphicContext().SetStrokeColor(color)
	interpreter.GetGraphicContext().SetFillColor(color)
}

func setdash(interpreter *Interpreter) {
	interpreter.PopInt()   // offset
	interpreter.PopArray() // dash
	//log.Printf("setdash not yet implemented dash: %v, offset: %d \n", dash, offset)
}

func setlinejoin(interpreter *Interpreter) {
	linejoin := interpreter.PopInt()
	switch linejoin {
	case 0:
		interpreter.GetGraphicContext().SetLineJoin(draw2d.MiterJoin)
	case 1:
		interpreter.GetGraphicContext().SetLineJoin(draw2d.RoundJoin)
	case 2:
		interpreter.GetGraphicContext().SetLineJoin(draw2d.BevelJoin)
	}
}

func setlinecap(interpreter *Interpreter) {
	linecap := interpreter.PopInt()
	switch linecap {
	case 0:
		interpreter.GetGraphicContext().SetLineCap(draw2d.ButtCap)
	case 1:
		interpreter.GetGraphicContext().SetLineCap(draw2d.RoundCap)
	case 2:
		interpreter.GetGraphicContext().SetLineCap(draw2d.SquareCap)
	}
}

func setmiterlimit(interpreter *Interpreter) {
	interpreter.PopInt()
	//log.Printf("setmiterlimit not yet implemented")
}

func setlinewidth(interpreter *Interpreter) {
	interpreter.GetGraphicContext().SetLineWidth(interpreter.PopFloat())
}

func showpage(interpreter *Interpreter) {
	//log.Printf("showpage may be an implementation specific, override show page to generate multi page images")
}

func show(interpreter *Interpreter) {
	s := interpreter.PopString()
	interpreter.GetGraphicContext().FillString(s)
	log.Printf("show not really implemented")
}

//ax  ay  string ashow – -> Add (ax , ay) to width of each glyph while showing string
func ashow(interpreter *Interpreter) {
	log.Printf("ashow not really implemented")
	s := interpreter.PopString()
	interpreter.PopFloat()
	interpreter.PopFloat()
	interpreter.GetGraphicContext().FillString(s)
}

func findfont(interpreter *Interpreter) {
	log.Printf("findfont not yet implemented")
}

func scalefont(interpreter *Interpreter) {
	log.Printf("scalefont not yet implemented")
}

func setfont(interpreter *Interpreter) {
	log.Printf("setfont not yet implemented")
}

func stringwidth(interpreter *Interpreter) {
	interpreter.Push(10.0)
	interpreter.Push(10.0)
	log.Printf("stringwidth not yet implemented")
}

func setflat(interpreter *Interpreter) {
	interpreter.Pop()
	//log.Printf("setflat not yet implemented")
}

func currentflat(interpreter *Interpreter) {
	interpreter.Push(1.0)
	//log.Printf("currentflat not yet implemented")
}

// Coordinate System and Matrix operators
func matrix(interpreter *Interpreter) {
	interpreter.Push(draw2d.NewIdentityMatrix())
}

func initmatrix(interpreter *Interpreter) {
	interpreter.Push(draw2d.NewIdentityMatrix())
}

func identmatrix(interpreter *Interpreter) {
	tr := interpreter.Pop().(draw2d.Matrix)
	ident := draw2d.NewIdentityMatrix()
	copy(tr[:], ident[:])
	interpreter.Push(tr)
}

func defaultmatrix(interpreter *Interpreter) {
	tr := interpreter.Pop().(draw2d.Matrix)
	ident := draw2d.NewIdentityMatrix()
	copy(tr[:], ident[:])
	interpreter.Push(tr)
}

func currentmatrix(interpreter *Interpreter) {
	tr := interpreter.Pop().(draw2d.Matrix)
	ctm := interpreter.GetGraphicContext().GetMatrixTransform()
	copy(tr[:], ctm[:])
	interpreter.Push(tr)
}

func setmatrix(interpreter *Interpreter) {
	tr := interpreter.Pop().(draw2d.Matrix)
	interpreter.GetGraphicContext().SetMatrixTransform(tr)
}

func concat(interpreter *Interpreter) {
	tr := interpreter.Pop().(draw2d.Matrix)
	interpreter.GetGraphicContext().ComposeMatrixTransform(tr)
}
func concatmatrix(interpreter *Interpreter) {
	tr3 := interpreter.Pop().(draw2d.Matrix)
	tr2 := interpreter.Pop().(draw2d.Matrix)
	tr1 := interpreter.Pop().(draw2d.Matrix)
	result := tr2.Copy()
	result.Compose(tr1)
	copy(tr3[:], result[:])
	interpreter.Push(tr3)
}

func transform(interpreter *Interpreter) {
	value := interpreter.Pop()
	matrix, ok := value.(draw2d.Matrix)
	var y float64
	if !ok {
		matrix = interpreter.GetGraphicContext().GetMatrixTransform()
		y = value.(float64)
	} else {
		y = interpreter.PopFloat()
	}
	x := interpreter.PopFloat()

	x, y = matrix.TransformPoint(x, y)
	interpreter.Push(x)
	interpreter.Push(y)
}

func itransform(interpreter *Interpreter) {
	value := interpreter.Pop()
	matrix, ok := value.(draw2d.Matrix)
	var y float64
	if !ok {
		matrix = interpreter.GetGraphicContext().GetMatrixTransform()
		y = value.(float64)
	} else {
		y = interpreter.PopFloat()
	}
	x := interpreter.PopFloat()
	x, y = matrix.InverseTransformPoint(x, y)
	interpreter.Push(x)
	interpreter.Push(y)
}

func translate(interpreter *Interpreter) {
	value := interpreter.Pop()
	matrix, ok := value.(draw2d.Matrix)
	var y float64
	if !ok {
		matrix = interpreter.GetGraphicContext().GetMatrixTransform()
		y = value.(float64)
	} else {
		y = interpreter.PopFloat()
	}
	x := interpreter.PopFloat()
	if !ok {
		interpreter.GetGraphicContext().Translate(x, y)
	} else {
		result := matrix.Copy()
		result.Translate(x, y)
		interpreter.Push(result)
	}
}

func rotate(interpreter *Interpreter) {
	value := interpreter.Pop()
	matrix, ok := value.(draw2d.Matrix)
	var angle float64
	if !ok {
		matrix = interpreter.GetGraphicContext().GetMatrixTransform()
		angle = value.(float64) * math.Pi / 180
	} else {
		angle = interpreter.PopFloat() * math.Pi / 180
	}
	if !ok {
		interpreter.GetGraphicContext().Rotate(angle)
	} else {
		result := matrix.Copy()
		result.Rotate(angle)
		interpreter.Push(result)
	}
}

func scale(interpreter *Interpreter) {
	value := interpreter.Pop()
	matrix, ok := value.(draw2d.Matrix)
	var y float64
	if !ok {
		matrix = interpreter.GetGraphicContext().GetMatrixTransform()
		y = value.(float64)
	} else {
		y = interpreter.PopFloat()
	}
	x := interpreter.PopFloat()
	if !ok {
		interpreter.GetGraphicContext().Scale(x, y)
	} else {
		result := matrix.Copy()
		result.Scale(x, y)
		interpreter.Push(result)
	}
}

func initDrawingOperators(interpreter *Interpreter) {

	interpreter.SystemDefine("stroke", NewOperator(stroke))
	interpreter.SystemDefine("fill", NewOperator(fill))
	interpreter.SystemDefine("show", NewOperator(show))
	interpreter.SystemDefine("ashow", NewOperator(ashow))
	interpreter.SystemDefine("showpage", NewOperator(showpage))

	interpreter.SystemDefine("findfont", NewOperator(findfont))
	interpreter.SystemDefine("scalefont", NewOperator(scalefont))
	interpreter.SystemDefine("setfont", NewOperator(setfont))
	interpreter.SystemDefine("stringwidth", NewOperator(stringwidth))

	// Graphic state operators
	interpreter.SystemDefine("gsave", NewOperator(gsave))
	interpreter.SystemDefine("grestore", NewOperator(grestore))
	interpreter.SystemDefine("setrgbcolor", NewOperator(setrgbcolor))
	interpreter.SystemDefine("sethsbcolor", NewOperator(sethsbcolor))
	interpreter.SystemDefine("setcmybcolor", NewOperator(setcmybcolor))
	interpreter.SystemDefine("setcmykcolor", NewOperator(setcmybcolor))
	interpreter.SystemDefine("setgray", NewOperator(setgray))
	interpreter.SystemDefine("setdash", NewOperator(setdash))
	interpreter.SystemDefine("setlinejoin", NewOperator(setlinejoin))
	interpreter.SystemDefine("setlinecap", NewOperator(setlinecap))
	interpreter.SystemDefine("setmiterlimit", NewOperator(setmiterlimit))
	interpreter.SystemDefine("setlinewidth", NewOperator(setlinewidth))
	// Graphic state operators device dependent
	interpreter.SystemDefine("setflat", NewOperator(setflat))
	interpreter.SystemDefine("currentflat", NewOperator(currentflat))

	// Coordinate System and Matrix operators
	interpreter.SystemDefine("matrix", NewOperator(matrix))
	interpreter.SystemDefine("initmatrix", NewOperator(initmatrix))
	interpreter.SystemDefine("identmatrix", NewOperator(identmatrix))
	interpreter.SystemDefine("defaultmatrix", NewOperator(defaultmatrix))
	interpreter.SystemDefine("currentmatrix", NewOperator(currentmatrix))
	interpreter.SystemDefine("setmatrix", NewOperator(setmatrix))
	interpreter.SystemDefine("concat", NewOperator(concat))
	interpreter.SystemDefine("concatmatrix", NewOperator(concatmatrix))

	interpreter.SystemDefine("transform", NewOperator(transform))
	interpreter.SystemDefine("itransform", NewOperator(itransform))
	interpreter.SystemDefine("translate", NewOperator(translate))
	interpreter.SystemDefine("rotate", NewOperator(rotate))
	interpreter.SystemDefine("scale", NewOperator(scale))

	//Path Construction Operators
	interpreter.SystemDefine("newpath", NewOperator(newpath))
	interpreter.SystemDefine("closepath", NewOperator(closepath))
	interpreter.SystemDefine("currentpoint", NewOperator(currentpoint))
	interpreter.SystemDefine("moveto", NewOperator(moveto))
	interpreter.SystemDefine("rmoveto", NewOperator(rmoveto))
	interpreter.SystemDefine("lineto", NewOperator(lineto))
	interpreter.SystemDefine("rlineto", NewOperator(rlineto))
	interpreter.SystemDefine("curveto", NewOperator(curveto))
	interpreter.SystemDefine("rcurveto", NewOperator(rcurveto))
	interpreter.SystemDefine("arc", NewOperator(arc))
	interpreter.SystemDefine("clippath", NewOperator(clippath))
}

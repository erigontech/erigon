// Package postscript reads the tiger.ps file and draws it to a backend.
package postscript

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/llgcode/ps"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/samples"
)

// Main draws the tiger
func Main(gc draw2d.GraphicContext, ext string) (string, error) {
	gc.Save()

	// flip the image
	gc.Translate(0, 200)
	gc.Scale(0.35, -0.35)
	gc.Translate(70, -200)

	// Tiger postscript drawing
	tiger := samples.Resource("image", "tiger.ps", ext)

	// Draw tiger
	Draw(gc, tiger)
	gc.Restore()

	// Return the output filename
	return samples.Output("postscript", ext), nil
}

// Draw a tiger
func Draw(gc draw2d.GraphicContext, filename string) {
	// Open the postscript
	src, err := os.OpenFile(filename, 0, 0)
	if err != nil {
		panic(err)
	}
	defer src.Close()
	bytes, err := ioutil.ReadAll(src)
	reader := strings.NewReader(string(bytes))

	// Initialize and interpret the postscript
	interpreter := ps.NewInterpreter(gc)
	interpreter.Execute(reader)
}

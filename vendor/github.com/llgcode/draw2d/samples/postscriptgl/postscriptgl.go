// Open a OpenGL window and display a tiger interpreting a postscript file
package main

import (
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/go-gl/gl/v2.1/gl"
	"github.com/go-gl/glfw/v3.1/glfw"
	"github.com/llgcode/draw2d/draw2dgl"
	"github.com/llgcode/ps"
)

var postscriptContent string

var (
	width, height int
	rotate        int
	window        *glfw.Window
)

func reshape(window *glfw.Window, w, h int) {
	gl.ClearColor(1, 1, 1, 1)
	//fmt.Println(gl.GetString(gl.EXTENSIONS))
	gl.Viewport(0, 0, int32(w), int32(h))         /* Establish viewing area to cover entire window. */
	gl.MatrixMode(gl.PROJECTION)                  /* Start modifying the projection matrix. */
	gl.LoadIdentity()                             /* Reset project matrix. */
	gl.Ortho(0, float64(w), 0, float64(h), -1, 1) /* Map abstract coords directly to window coords. */
	gl.Scalef(1, -1, 1)                           /* Invert Y axis so increasing Y goes down. */
	gl.Translatef(0, float32(-h), 0)              /* Shift origin up to upper-left corner. */
	gl.Enable(gl.BLEND)
	gl.BlendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA)
	gl.Disable(gl.DEPTH_TEST)
	width, height = w, h
}

func display() {

	gl.Clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT)

	lastTime := time.Now()
	gl.LineWidth(1)
	gc := draw2dgl.NewGraphicContext(width, height)

	gc.Translate(380, 400)
	gc.Scale(1, -1)
	rotate = (rotate + 1) % 360
	gc.Rotate(float64(rotate) * math.Pi / 180)
	gc.Translate(-380, -400)

	interpreter := ps.NewInterpreter(gc)
	reader := strings.NewReader(postscriptContent)

	interpreter.Execute(reader)
	dt := time.Now().Sub(lastTime)
	log.Printf("Redraw in : %f ms\n", float64(dt)*1e-6)
	gl.Flush() /* Single buffered, so needs a flush. */
}

func main() {
	src, err := os.OpenFile("tiger.ps", 0, 0)
	if err != nil {
		log.Println("can't find postscript file.")
		return
	}
	defer src.Close()
	bytes, err := ioutil.ReadAll(src)
	postscriptContent = string(bytes)
	err = glfw.Init()
	if err != nil {
		panic(err)
	}
	defer glfw.Terminate()

	window, err = glfw.CreateWindow(800, 800, "Show Tiger in OpenGL", nil, nil)
	if err != nil {
		panic(err)
	}

	window.MakeContextCurrent()
	window.SetSizeCallback(reshape)
	window.SetKeyCallback(onKey)

	glfw.SwapInterval(1)

	err = gl.Init()
	if err != nil {
		panic(err)
	}
	reshape(window, 800, 800)
	for !window.ShouldClose() {
		display()
		window.SwapBuffers()
		glfw.PollEvents()
		//		time.Sleep(2 * time.Second)
	}
}

func onKey(w *glfw.Window, key glfw.Key, scancode int, action glfw.Action, mods glfw.ModifierKey) {
	switch {
	case key == glfw.KeyEscape && action == glfw.Press,
		key == glfw.KeyQ && action == glfw.Press:
		w.SetShouldClose(true)
	}
}

func init() {
	runtime.LockOSThread()
}

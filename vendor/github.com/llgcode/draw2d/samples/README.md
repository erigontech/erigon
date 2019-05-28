draw2d samples
==============

Various samples for using draw2d

Using the image backend
-----------------------

The following Go code draws the android sample on a png image:

```
import (
	"image"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/samples/android"
)

function main(){}
	// Initialize the graphic context on an RGBA image
	dest := image.NewRGBA(image.Rect(0, 0, 297, 210.0))
	gc := draw2dimg.NewGraphicContext(dest)
	// Draw Android logo
	fn, err := android.Main(gc, "png")
	if err != nil {
		t.Errorf("Drawing %q failed: %v", fn, err)
		return
	}
	// Save to png
	err = draw2d.SaveToPngFile(fn, dest)
	if err != nil {
		t.Errorf("Saving %q failed: %v", fn, err)
	}
}
```

Using the pdf backend
---------------------

The following Go code draws the android sample on a pdf document:

```
import (
	"image"

	"github.com/llgcode/draw2d/draw2dpdf"
	"github.com/llgcode/draw2d/samples/android"
)

function main(){}
	// Initialize the graphic context on a pdf document
	dest := draw2dpdf.NewPdf("L", "mm", "A4")
	gc := draw2dpdf.NewGraphicContext(dest)
	// Draw Android logo
	fn, err := android.Main(gc, "png")
	if err != nil {
		t.Errorf("Drawing %q failed: %v", fn, err)
		return
	}
	// Save to pdf
	err = draw2dpdf.SaveToPdfFile(fn, dest)
	if err != nil {
		t.Errorf("Saving %q failed: %v", fn, err)
	}
}
```

Testing
-------

These samples are run as tests from the root package folder `draw2d` by:
```
go test ./...
```
Or if you want to run with test coverage:
```
go test -cover ./... | grep -v "no test"
```
The following files are responsible to run the image tests:
```
draw2d/test_test.go
draw2d/samples_test.go
```
The following files are responsible to run the pdf tests:
```
draw2d/pdf/test_test.go
draw2dpdf/samples_test.go
```

// Package samples provides examples which can be used with different
// backends. They are also used for testing and coverage of the
// draw2d package.
package samples

import "fmt"

// Resource returns a resource filename for testing.
func Resource(folder, filename, ext string) string {
	var root string
	if ext == "pdf" || ext == "svg" {
		root = "../"
	}
	return fmt.Sprintf("%sresource/%s/%s", root, folder, filename)
}

// Output returns the output filename for testing.
func Output(name, ext string) string {
	var root string
	if ext == "pdf" || ext == "svg" {
		root = "../"
	}
	return fmt.Sprintf("%soutput/samples/%s.%s", root, name, ext)
}

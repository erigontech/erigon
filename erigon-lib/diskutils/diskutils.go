//go:build !darwin

package diskutils

import "fmt"

func MountPointForDirPath(dirPath string) string {
	fmt.Println("Implemented only for darwin")
	return "/"
}

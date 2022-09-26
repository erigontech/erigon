package utils

import (
	"fmt"
	"os/exec"
)

// ClearDevDB cleans up the dev folder used for the operations
func ClearDevDB() {
	fmt.Printf("\nDeleting ./dev folder\n")

	cmd := exec.Command("rm", "-rf", "./dev")
	err := cmd.Run()
	if err != nil {
		fmt.Println("Error occurred clearing Dev DB")
		panic("could not clear dev DB")
	}

	fmt.Printf("SUCCESS => Deleted ./dev\n")
}

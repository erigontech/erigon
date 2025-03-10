//go:build linux

package gdbme

import (
	"fmt"
	"github.com/erigontech/erigon/cmd/utils"
	"os"
	"os/exec"
)

const gdbPath = "/usr/bin/gdb"

// restartUnderGDB relaunches the current process under GDB for debugging purposes.
func RestartUnderGDB() {
	exePath, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: could not determine executable path:", err)
		os.Exit(1)
	}

	args := os.Args[1:]
	filteredArgs := []string{}
	for _, arg := range args {
		if arg != "--"+utils.GDBMeFlag.Name {
			filteredArgs = append(filteredArgs, arg)
		}
	}

	gdbArgs := []string{
		"-q",
		"-batch",
		"-nx",
		"-nh",
		"-return-child-result",
		"-ex", "run",
		"-ex", "bt full",
		"--args",
		exePath,
	}
	gdbArgs = append(gdbArgs, filteredArgs...)

	fmt.Fprintln(os.Stderr, "Restarting under GDB for crash diagnostics...")
	cmd := exec.Command(gdbPath, gdbArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	err = cmd.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to restart under GDB:", err)
		os.Exit(1)
	}
}

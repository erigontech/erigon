//go:build linux

package gdbme

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/erigontech/erigon/cmd/utils"
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

	gdbCommands := []string{
		"set debuginfod enabled off",
		"set pagination off",
		"set confirm off",
		"set width 200",
		"set logging file /tmp/gdb_backtrace.log",
		"set logging on",
		"info sharedlibrary",
		"info threads",
		"thread apply all backtrace",
		"thread apply all disassemble",
		"thread apply all info all-registers",
		"thread apply all backtrace full",
		"shell uname -a",
		"shell df -h",
		"show environment",
		"run",
		"bt full",
		"info all-registers",
		"disassemble",
		"thread",
		"backtrace",
		"frame 1",
		"backtrace full",
		"quit",
	}

	// Формируем аргументы для GDB
	gdbArgs := []string{
		"-q",
		"-batch",
		"-nx",
		"-nh",
		"-return-child-result",
	}
	for _, cmd := range gdbCommands {
		gdbArgs = append(gdbArgs, "-ex", cmd)
	}
	gdbArgs = append(gdbArgs, "--args", exePath)
	gdbArgs = append(gdbArgs, filteredArgs...)

	fmt.Fprintln(os.Stderr, "Restarting under GDB for crash diagnostics...")
	cmd := exec.CommandContext(context.Background(), gdbPath, gdbArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	err = cmd.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to restart under GDB:", err)
		os.Exit(1)
	}
}

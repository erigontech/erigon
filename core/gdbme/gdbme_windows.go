//go:build windows

package gdbme

import (
	"fmt"
	"github.com/erigontech/erigon/cmd/utils"
	"os"
	"os/exec"
	"path/filepath"
)

const gdbExecutable = "gdb.exe" // Ensure GDB for Windows is installed and in your PATH

// RestartUnderGDB relaunches the current process under GDB on Windows so that
// any C‐level crashes or binding issues can be diagnosed. It creates a temporary
// gdb script that:
//  1. Disables pagination and confirmation.
//  2. Turns on logging (to a file under %TEMP%) to capture backtraces, registers, etc.
//  3. Prints out shared library info, threads, and then does multiple backtrace commands.
//  4. Finally runs the program, captures the C backtrace if it crashes, and exits.
//
// To use this, compile your Go binary with debug symbols (default `go build` on Windows keeps symbols).
// Then invoke your program with the special flag (e.g. `--gdbme`); this function will drop that flag,
// re-exec the same .exe under Win-GDB, dump the logs to a file, and then exit.
//
// Make sure you have installed GDB for Windows (e.g. via MinGW-w64) and that `gdb.exe` is on your PATH.
// Alternatively, change `gdbExecutable` to the full path of your `gdb.exe`.
func RestartUnderGDB() {
	// 1) Determine path to this executable.
	exePath, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: could not determine executable path:", err)
		os.Exit(1)
	}

	// 2) Filter out the special flag (e.g. "--gdbme") so we don’t recurse infinitely.
	flagName := "--" + utils.GDBMeFlag.Name
	origArgs := os.Args[1:]
	filteredArgs := make([]string, 0, len(origArgs))
	for _, arg := range origArgs {
		if arg == flagName {
			continue
		}
		filteredArgs = append(filteredArgs, arg)
	}

	// 3) Create a temporary file under %TEMP% to hold our GDB commands.
	//    We set logging to a file in %TEMP% as well.
	tempDir := os.TempDir()
	logPath := filepath.Join(tempDir, "gdb_backtrace.log")
	// GDB on Windows accepts forward slashes even if running natively, so convert:
	logPathForGDB := filepath.ToSlash(logPath)

	gdbCommands := []string{
		// Disable any external symbol servers or debuginfod (if present)
		"set debuginfod enabled off",
		"set pagination off",
		"set confirm off",
		// Make the GDB output wider so long C backtraces fit
		"set width 200",
		// Point GDB’s logging to our temp file
		"set logging file " + logPathForGDB,
		"set logging on",
		// Collect shared library info and thread list
		"info sharedlibrary",
		"info threads",
		// Print a backtrace of every thread (addresses + source lines, if available)
		"thread apply all backtrace",
		"thread apply all disassemble",
		"thread apply all info all-registers",
		"thread apply all backtrace full",
		// Dump some OS info (if the user’s PATH has `uname` or similar)—optional on Windows,
		// but harmless if not found. If you prefer purely Windows, comment these two out.
		"shell uname -a",
		"shell df -h",
		"show environment",
		// Now actually run the program. If it crashes, GDB will stop and keep logging.
		"run",
		// If run finishes (or crashes), capture more info
		"bt full",
		"info all-registers",
		"disassemble",
		"thread",
		"backtrace",
		"frame 1",
		"backtrace full",
		"quit",
	}

	// 4) Build the GDB argument list. We run in batch mode (no interactive prompts),
	//    skip reading ~/.gdbinit, and return the child’s exit code to us.
	gdbArgs := []string{
		"-q",                   // quiet: suppress introductory messages
		"-batch",               // exit after processing commands
		"-nx",                  // do not load ~/.gdbinit
		"-nh",                  // do not load c:\.gdbinit either
		"-return-child-result", // propagate the target’s exit code
	}
	for _, cmd := range gdbCommands {
		gdbArgs = append(gdbArgs, "-ex", cmd)
	}
	// Tell GDB which program to debug:
	gdbArgs = append(gdbArgs, "--args", exePath)
	// Pass along the user’s filtered arguments (after “--args”)
	gdbArgs = append(gdbArgs, filteredArgs...)

	// 5) Print a message so the user knows what’s happening:
	fmt.Fprintln(os.Stderr, "Restarting under GDB for C‐level crash diagnostics...")
	fmt.Fprintf(os.Stderr, "  -> GDB executable: %s\n", gdbExecutable)
	fmt.Fprintf(os.Stderr, "  -> Logging to: %s\n", logPath)

	// 6) Launch GDB. We inherit Stdin (so Ctrl+C can still be sent), and let GDB’s own
	//    stdout/stderr go to the console. The actual program’s stdout/stderr (and GDB’s
	//    logging) will be captured to the log file.
	cmd := exec.Command(gdbExecutable, gdbArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to restart under GDB:", err)
		os.Exit(1)
	}
}

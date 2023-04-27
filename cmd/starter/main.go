package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type ErrorTracker struct {
	Substring string
	Limit     int
	Window    time.Duration
	Errors    []time.Time
}

func (et *ErrorTracker) AddError(t time.Time) {
	et.Errors = append(et.Errors, t)
	if len(et.Errors) > et.Limit {
		et.Errors = et.Errors[1:]
	}
}

func (et *ErrorTracker) ShouldRestartProcess() bool {
	if len(et.Errors) < et.Limit {
		return false
	}

	firstError := et.Errors[0]
	lastError := et.Errors[len(et.Errors)-1]
	return lastError.Sub(firstError) <= et.Window
}

func parseErrorTrackers() ([]ErrorTracker, []string) {
	trackers := make([]ErrorTracker, 0)
	args := os.Args[1:]
	maxArg := -1

	for i, arg := range args {
		if arg == "--errortrack" {
			if i+3 >= len(args) {
				log("Invalid --errortrack parameters")
				os.Exit(1)
			}

			substring := args[i+1]
			limit, err := strconv.Atoi(args[i+2])
			if err != nil {
				log("Error parsing limit: %v", err)
				os.Exit(1)
			}

			window, err := time.ParseDuration(args[i+3])
			if err != nil {
				log("Error parsing time window: %v", err)
				os.Exit(1)
			}

			trackers = append(trackers, ErrorTracker{Substring: substring, Limit: limit, Window: window})
			if i+3 > maxArg {
				maxArg = i + 3
			}
		}
	}

	if maxArg >= len(args)-1 {
		log("No command provided")
		os.Exit(1)
	}

	cmdArgs := args[maxArg+1:]
	return trackers, cmdArgs
}

func log(format string, a ...interface{}) {
	fmt.Printf(fmt.Sprintf("[starter] %s ", time.Now().Format("[01-02|15:04:05.000]"))+format+"\n", a...)
}

func main() {
	trackers, cmdArgs := parseErrorTrackers()
	if len(cmdArgs) == 0 {
		log("No command provided")
		os.Exit(1)
	}

	cmdName := cmdArgs[0]
	cmdArgs = cmdArgs[1:]

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	restartChan := make(chan struct{}, 2)
	var cmd *exec.Cmd
	var stdoutPipe, stderrPipe io.ReadCloser
	var wg sync.WaitGroup

	scanAndTrackErrors := func(scanner *bufio.Scanner, trackers []ErrorTracker, lasrwriter io.Writer, restartChan chan struct{}) {
		restartInit := false
		for scanner.Scan() {

			line := scanner.Text()

			fmt.Fprintln(lasrwriter, line)

			if restartInit {
				continue
			}

			for i := range trackers {
				if strings.Contains(line, trackers[i].Substring) {
					trackers[i].AddError(time.Now())
					log("Find error substring %q, %d errors in %s", trackers[i].Substring, len(trackers[i].Errors), trackers[i].Window)

					if trackers[i].ShouldRestartProcess() {
						log("Restarting process due to error limit for substring %q", trackers[i].Substring)
						restartChan <- struct{}{}
						restartInit = true
					}
				}
			}
		}
	}

	runCmd := func() {
		cmd = exec.Command(cmdName, cmdArgs...)
		stdoutPipe, _ = cmd.StdoutPipe()
		stderrPipe, _ = cmd.StderrPipe()

		wg.Add(2)
		go func() {
			defer wg.Done()
			scanner := bufio.NewScanner(stdoutPipe)
			scanAndTrackErrors(scanner, trackers, os.Stdout, restartChan)
		}()

		go func() {
			defer wg.Done()
			scanner := bufio.NewScanner(stderrPipe)
			scanAndTrackErrors(scanner, trackers, os.Stderr, restartChan)
		}()
		cmd.Start()
	}

	runCmd()

	for {
		select {
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				cmd.Process.Signal(sig)
				cmd.Process.Wait()
				wg.Wait()
				os.Exit(0)
			}
		case <-restartChan:
			if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
				log("Error sending signal to process: %v", err)
			}
			cmd.Process.Wait()
			wg.Wait()
			log("Restarting process due to error limits")
			for i := range trackers {
				trackers[i].Errors = []time.Time{}
			}
			runCmd()
		}
	}
}

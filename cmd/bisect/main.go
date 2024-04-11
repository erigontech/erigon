package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"time"
	"bytes"
	"flag"
	"bufio"
	"strings"
	"errors"
	"os"
)

type Flags struct {
	Limit       uint
	Step        uint
	StepAfter   uint
	Config      string
	SeedDataDir string
}

/// EXAMPLE ///
// go run cmd/bisect/main.go --startingBlock=10 --maxBlock=100 --step=10 --stepAfter=50 --config=./hermezconfig-mainnet.yaml
///////////////

func main() {
	var startingBlock, maxBlock uint
	var step, stepAfter uint
	var config, seedDataDir string

	flag.UintVar(&startingBlock, "startingBlock", 1, "Starting block number")
	flag.UintVar(&maxBlock, "maxBlock", 100, "Maximum block number to check")
	flag.UintVar(&step, "step", 1, "Number of blocks to process each run of the stage loop")
	flag.UintVar(&stepAfter, "stepAfter", 0, "Start incrementing by debug.step after this block")
	flag.StringVar(&config, "config", "./hermezconfig-mainnet.yaml", "Path to the config file")
	flag.StringVar(&seedDataDir, "seedDataDir", "", "Path to the seed data directory")
	flag.Parse()

	flags := Flags{
		Limit:       maxBlock,
		Step:        step,
		StepAfter:   stepAfter,
		Config:      config,
		SeedDataDir: seedDataDir,
	}

	fmt.Println("Starting bisect with the following configuration:")
	fmt.Printf("Starting Block: %d, Max Block: %d, Step: %d, Step After: %d, Config: %s\n",
		startingBlock, maxBlock, step, stepAfter, config)

	fmt.Println("Starting bisect...")
	highestFailing, err := bisect(startingBlock, maxBlock, flags)
	if err != nil {
		fmt.Printf("An error occurred during bisect: %v\n", err)
		return
	}

	fmt.Printf("Highest failing block: %d\n", highestFailing)
}

func bisect(low, high uint, flags Flags) (uint, error) {
	initialRange := high - low
	initialStepSize := flags.Step
	minStepSize := uint(1)

	for low <= high {
		mid := low + (high-low)/2
		flags.Limit = mid

		currentRange := high - low
		stepSizeReductionFactor := float64(currentRange) / float64(initialRange)
		dynamicStepSize := uint(float64(initialStepSize) * stepSizeReductionFactor)

		if dynamicStepSize < minStepSize {
			flags.Step = minStepSize
		} else {
			flags.Step = dynamicStepSize
		}

		fmt.Println("Bisect range:", low, "to", high, "Checking block", mid, "with dynamic step size", flags.Step)
		success, execNo, err := runProgram(flags)
		if err != nil {
			fmt.Println("Error running program:", err)
			return 0, err
		}

		if success {
			fmt.Println("Success at block", mid)
			low = mid + 1
		} else {
			fmt.Println("Failure at block", mid, "with execution failure at block", execNo)
			if execNo > 0 && uint(execNo) < high {
				fmt.Println("Adjusting high to", execNo, "based on execution failure")
				high = uint(execNo)
			} else {
				high = mid - 1
			}
		}
		fmt.Println("Updated bisect range:", low, "to", high)
	}

	return high, nil
}

func runProgram(flags Flags) (bool, uint64, error) {
	ts := time.Now().UnixMilli()
	dirName := "/tmp/datadirs/hermez-bisect-" + strconv.Itoa(int(ts)) + "-" + strconv.Itoa(int(flags.Limit))

	if err := os.MkdirAll(dirName, 0755); err != nil {
		fmt.Printf("Failed to create target directory %s: %v\n", dirName, err)
		return false, 0, err
	}

	if flags.SeedDataDir != "" {
		copyCmd := exec.Command("cp", "-a", flags.SeedDataDir, dirName)
		fmt.Println("Executing command:", copyCmd.String())
		if err := copyCmd.Run(); err != nil {
			fmt.Printf("Failed to copy seed data directory: %v\n", err)
			return false, 0, err
		} else {
			fmt.Println("Seed data directory copied successfully.")
		}
	}

	cmd := exec.Command("./build/bin/cdk-erigon",
		"--config="+flags.Config,
		"--datadir="+dirName,
		"--debug.limit="+strconv.Itoa(int(flags.Limit)),
		"--debug.step="+strconv.Itoa(int(flags.Step)),
		"--debug.step-after="+strconv.Itoa(int(flags.StepAfter)),
	)

	fmt.Println(cmd.String())

	defer func() {
		cmd := exec.Command("rm", "-rf", dirName)
		err := cmd.Run()
		if err != nil {
			fmt.Printf("Error deleting directory %s: %v\n", dirName, err)
		}
	}()

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		var exiterr *exec.ExitError
		if errors.As(err, &exiterr) {
			fmt.Printf("Command failed with exit code %d for limit %d\n", exiterr.ExitCode(), flags.Limit)
			fmt.Printf("Stderr: %s\n", stderr.String())
			exitCode := exiterr.ExitCode()
			if exitCode == 2 {
				scanner := bufio.NewScanner(&stderr)
				var lastLine string
				for scanner.Scan() {
					lastLine = scanner.Text()
				}

				blockNumber := parseBlockNumberFromStderr(lastLine)
				if blockNumber > 0 {
					return false, uint64(blockNumber - 1), nil
				}
			}

			return false, 0, nil
		}
	}
	return true, 0, nil
}

func parseBlockNumberFromStderr(stderrLine string) uint {
	const prefix = "block="
	start := strings.Index(stderrLine, prefix)
	if start == -1 {
		return 0
	}
	start += len(prefix)

	end := start
	for end < len(stderrLine) && (stderrLine[end] >= '0' && stderrLine[end] <= '9') {
		end++
	}

	blockNumberStr := stderrLine[start:end]
	if num, err := strconv.Atoi(blockNumberStr); err == nil {
		return uint(num)
	}
	return 0
}

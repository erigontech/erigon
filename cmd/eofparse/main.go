package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/erigontech/erigon/core/vm"
)

func main() {
	// Define a flag for the file path
	filePathFlag := flag.String("file", "", "Absolute path to the input file (optional)")
	flag.Parse()

	var scanner *bufio.Scanner
	var inputSource string

	if *filePathFlag != "" {
		// If the file flag is provided, open the file
		file, err := os.Open(*filePathFlag)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error opening file:", err)
			return
		}
		defer file.Close()

		scanner = bufio.NewScanner(file)
		inputSource = "file"
	} else {
		// Otherwise, read from stdin
		scanner = bufio.NewScanner(os.Stdin)
		inputSource = "stdin"
	}

	eofJt := vm.NewEOFInstructionSet()

	fmt.Printf("Reading hex input from %s:\n", inputSource)

	// Read each line from the input
	for scanner.Scan() {
		hexInput := scanner.Text() // Read input as a string
		if strings.TrimSpace(hexInput) == "" {
			continue // Skip empty lines
		}

		// fmt.Println("Hex input:", hexInput)

		// Remove the "0x" prefix if present
		hexInput = strings.TrimPrefix(hexInput, "0x")

		// If the length of the input is odd, prepend a "0" to make it even
		if len(hexInput)%2 != 0 {
			hexInput = "0" + hexInput
		}

		// Decode the hex input into bytes
		input, err := hex.DecodeString(hexInput)
		if err != nil {
			fmt.Println("error decoding hex input:", err)
			continue
		}

		// Process the decoded bytes, requires correct containerKind as well
		header, err := vm.ParseEOFHeader(input, &eofJt, 1, true, 0) //
		if err != nil {
			fmt.Println("err:", err)
			continue
		}

		fmt.Printf("OK %s\n", header.GetHexCodeSections(input))
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}

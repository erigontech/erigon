package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

type Input struct {
	ContractName string            `json:"contractName,omitempty"`
	AccountName  string            `json:"accountName,omitempty"`
	Balance      string            `json:"balance"`
	Nonce        string            `json:"nonce"`
	Address      string            `json:"address"`
	Bytecode     string            `json:"bytecode,omitempty"`
	Storage      map[string]string `json:"storage,omitempty"`
}

type Wrapper struct {
	Root    string  `json:"root"`
	Genesis []Input `json:"genesis"`
}

type Output struct {
	ContractName *string          `json:"contractName"`
	Balance      *string          `json:"balance"`
	Nonce        *string          `json:"nonce"`
	Code         *string          `json:"code"`
	Storage      *json.RawMessage `json:"storage"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run script.go <inputfile.json>")
		os.Exit(1)
	}

	inputFileName := os.Args[1]

	file, err := os.ReadFile(inputFileName)
	if err != nil {
		fmt.Println("Error reading file:", err)
		os.Exit(1)
	}

	var wrapper Wrapper
	err = json.Unmarshal(file, &wrapper)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		os.Exit(1)
	}

	var sb strings.Builder
	sb.WriteString("{\n")

	for i, input := range wrapper.Genesis {
		storageJSON, err := getOrderedStorageJSON(input.Storage)
		if err != nil {
			fmt.Println("Error creating storage JSON:", err)
			os.Exit(1)
		}

		output := Output{
			ContractName: getStringPointer(input.ContractName),
			Balance:      getStringPointer(input.Balance),
			Nonce:        getStringPointer(input.Nonce),
			Code:         getStringPointer(input.Bytecode),
			Storage:      storageJSON,
		}

		outputJSON, err := json.MarshalIndent(output, "  ", "  ")
		if err != nil {
			fmt.Println("Error marshalling JSON:", err)
			os.Exit(1)
		}

		sb.WriteString(fmt.Sprintf("  %q: %s", input.Address, string(outputJSON)))
		if i < len(wrapper.Genesis)-1 {
			sb.WriteString(",\n")
		}
	}

	sb.WriteString("\n}")

	err = os.WriteFile("allocs.json", []byte(sb.String()), 0644)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		os.Exit(1)
	}

	fmt.Println("Transformation complete. Output written to allocs.json")
}

func getStringPointer(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func getOrderedStorageJSON(storage map[string]string) (*json.RawMessage, error) {
	if len(storage) == 0 {
		nullJSON := json.RawMessage("null")
		return &nullJSON, nil
	}

	keys := make([]string, 0, len(storage))
	for key := range storage {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var storageBuilder strings.Builder
	storageBuilder.WriteString("{")
	for i, key := range keys {
		storageBuilder.WriteString(fmt.Sprintf("%q: %q", key, storage[key]))
		if i < len(keys)-1 {
			storageBuilder.WriteString(", ")
		}
	}
	storageBuilder.WriteString("}")

	storageJSON := json.RawMessage(storageBuilder.String())
	return &storageJSON, nil
}

package devnetutils

import (
	"encoding/json"
	"fmt"
	"os/exec"
)

// ClearDevDB cleans up the dev folder used for the operations
func ClearDevDB() {
	fmt.Printf("\nDeleting ./dev folders\n")

	cmd := exec.Command("rm", "-rf", "./dev0")
	err := cmd.Run()
	if err != nil {
		fmt.Println("Error occurred clearing Dev DB")
		panic("could not clear dev DB")
	}

	cmd2 := exec.Command("rm", "-rf", "./dev2")
	err2 := cmd2.Run()
	if err2 != nil {
		fmt.Println("Error occurred clearing Dev DB")
		panic("could not clear dev2 DB")
	}

	fmt.Printf("SUCCESS => Deleted ./dev0 and ./dev2\n")
}

func DeleteLogs() {
	fmt.Printf("\nRemoving old logs to create new ones...\nBefore re-running the devnet tool, make sure to copy out old logs if you need them!!!\n\n")

	cmd := exec.Command("rm", "-rf", "./erigon_node_1")
	err := cmd.Run()
	if err != nil {
		fmt.Println("Error occurred removing log node_1")
		panic("could not remove old logs")
	}

	cmd2 := exec.Command("rm", "-rf", "./erigon_node_2")
	err2 := cmd2.Run()
	if err2 != nil {
		fmt.Println("Error occurred removing log node_2")
		panic("could not remove old logs")
	}
}

// UniqueIDFromEnode returns the unique ID from a node's enode, removing the `?discport=0` part
func UniqueIDFromEnode(enode string) (string, error) {
	if len(enode) == 0 {
		return "", fmt.Errorf("invalid enode string")
	}

	// iterate through characters in the string until we reach '?'
	// using index iteration because enode characters have single codepoints
	var i int
	for i < len(enode) && enode[i] != byte('?') {
		i++
	}

	// if '?' is not found in the enode, return an error
	if i == len(enode) {
		return "", fmt.Errorf("invalid enode string")
	}

	return enode[:i], nil
}

// ParseResponse converts any of the rpctest interfaces to a string for readability
func ParseResponse(resp interface{}) (string, error) {
	result, err := json.Marshal(resp)
	if err != nil {
		return "", fmt.Errorf("error trying to marshal response: %v", err)
	}

	return string(result), nil
}

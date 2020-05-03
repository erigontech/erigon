// EVMC: Ethereum Client-VM Connector API.
// Copyright 2018-2019 The EVMC Authors.
// Licensed under the Apache License, Version 2.0.

//go:generate gcc -shared ../../../examples/example_vm/example_vm.c -I../../../include -o example_vm.so

package evmc

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

var modulePath = "./example_vm.so"

func TestLoad(t *testing.T) {
	i, err := Load(modulePath)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer i.Destroy()
	if i.Name() != "example_vm" {
		t.Fatalf("name is %s", i.Name())
	}
	if i.Version()[0] < '0' || i.Version()[0] > '9' {
		t.Fatalf("version number is weird: %s", i.Version())
	}
}

func TestLoadConfigure(t *testing.T) {
	i, err := LoadAndConfigure(modulePath)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer i.Destroy()
	if i.Name() != "example_vm" {
		t.Fatalf("name is %s", i.Name())
	}
	if i.Version()[0] < '0' || i.Version()[0] > '9' {
		t.Fatalf("version number is weird: %s", i.Version())
	}
}

func TestExecute(t *testing.T) {
	vm, _ := Load(modulePath)
	defer vm.Destroy()

	addr := common.Address{}
	h := common.Hash{}
	output, gasLeft, err := vm.Execute(nil, Byzantium, Call, false, 1, 999, addr, addr, nil, h, nil, h)

	if bytes.Compare(output, []byte("Welcome to Byzantium!")) != 0 {
		t.Errorf("execution unexpected output: %s", output)
	}
	if gasLeft != 99 {
		t.Error("execution gas left is incorrect")
	}
	if err != Failure {
		t.Error("execution returned unexpected error")
	}
}

// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
)

var cacheTestID atomic.Uint64

type countingPrecompile struct {
	calls  int
	output []byte
	err    error
}

func (p *countingPrecompile) Run([]byte) ([]byte, error) { p.calls++; return p.output, p.err }
func (p *countingPrecompile) RequiredGas([]byte) uint64  { return 100 }
func (p *countingPrecompile) Name() string               { return "counting" }

func TestCachedPrecompileOwnershipAndGas(t *testing.T) {
	p := &countingPrecompile{output: []byte{1, 2, 3}}
	c := &cachedPrecompile{PrecompiledContract: p, domain: [21]byte{201}}
	input := fmt.Appendf(nil, "%s/%d", t.Name(), cacheTestID.Add(1))
	out, gas, err := RunPrecompiledContract(c, input, mdgas.MdGas{Execution: 150}, nil)
	if err != nil || gas.Execution != 50 {
		t.Fatalf("gas=%v err=%v", gas, err)
	}
	out[0] = 9
	if p.output[0] != 1 {
		t.Fatal("returned output aliases the precompile result")
	}
	out, gas, err = RunPrecompiledContract(c, input, mdgas.MdGas{Execution: 150}, nil)
	if err != nil || gas.Execution != 50 || !bytes.Equal(out, []byte{1, 2, 3}) || p.calls != 1 {
		t.Fatalf("output=%x gas=%v calls=%d err=%v", out, gas, p.calls, err)
	}
	out[1] = 9
	out, _, err = RunPrecompiledContract(c, input, mdgas.MdGas{Execution: 100}, nil)
	if err != nil || !bytes.Equal(out, []byte{1, 2, 3}) {
		t.Fatal("cache output was mutated")
	}
	_, _, err = RunPrecompiledContract(c, input, mdgas.MdGas{Execution: 99}, nil)
	if !errors.Is(err, ErrOutOfGas) || p.calls != 1 {
		t.Fatal("cache hit bypassed gas check")
	}
}

func TestCachedPrecompileDomainAndErrors(t *testing.T) {
	input := fmt.Appendf(nil, "%s/%d", t.Name(), cacheTestID.Add(1))
	p := &countingPrecompile{output: []byte{7}}
	a := &cachedPrecompile{PrecompiledContract: p, domain: [21]byte{202}}
	b := &cachedPrecompile{PrecompiledContract: p, domain: [21]byte{203}}
	for _, c := range []*cachedPrecompile{a, a, b, b} {
		if _, err := c.Run(input); err != nil {
			t.Fatal(err)
		}
	}
	if p.calls != 2 {
		t.Fatalf("different domains shared a result: %d calls", p.calls)
	}
	p = &countingPrecompile{err: errors.New("invalid")}
	c := &cachedPrecompile{PrecompiledContract: p, domain: [21]byte{204}}
	for range 2 {
		if _, err := c.Run(input); err == nil {
			t.Fatal("lost error")
		}
	}
	if p.calls != 2 {
		t.Fatal("cached an error")
	}
	p = &countingPrecompile{output: []byte{7}}
	c = &cachedPrecompile{PrecompiledContract: p, domain: [21]byte{205}}
	for range 2 {
		if _, err := c.Run(make([]byte, 64*1024+1)); err != nil {
			t.Fatal(err)
		}
	}
	if p.calls != 2 {
		t.Fatal("cached oversized input")
	}
	if cachePrecompile(p, forkOsaka, common.Address{}) != p {
		t.Fatal("wrapped custom contract")
	}
}

func TestCachedPrecompileParallel(t *testing.T) {
	c := &cachedPrecompile{PrecompiledContract: &sha256hash{}, domain: [21]byte{206}}
	input := fmt.Appendf(nil, "%s/%d", t.Name(), cacheTestID.Add(1))
	want, _ := c.PrecompiledContract.Run(input)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 100 {
				output, err := c.Run(input)
				if err != nil || !bytes.Equal(output, want) {
					t.Error("concurrent cache output mismatch")
					return
				}
				output[0] ^= 255
			}
		})
	}
	wg.Wait()
}

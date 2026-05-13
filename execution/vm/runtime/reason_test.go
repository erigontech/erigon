// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package runtime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// nonceChangeEvent records a single OnNonceChangeV2 invocation.
type nonceChangeEvent struct {
	Addr   accounts.Address
	Prev   uint64
	New    uint64
	Reason tracing.NonceChangeReason
}

// codeChangeEvent records a single OnCodeChangeV2 invocation.
type codeChangeEvent struct {
	Addr   accounts.Address
	Reason tracing.CodeChangeReason
}

// recordingTracer returns a Hooks that captures all nonce and code change V2
// events into the provided slices.
func recordingTracer(nonceEvents *[]nonceChangeEvent, codeEvents *[]codeChangeEvent) *tracing.Hooks {
	return &tracing.Hooks{
		OnNonceChangeV2: func(addr accounts.Address, prev, cur uint64, reason tracing.NonceChangeReason) {
			*nonceEvents = append(*nonceEvents, nonceChangeEvent{addr, prev, cur, reason})
		},
		OnCodeChangeV2: func(addr accounts.Address, prevCodeHash accounts.CodeHash, prevCode []byte, codeHash accounts.CodeHash, code []byte, reason tracing.CodeChangeReason) {
			*codeEvents = append(*codeEvents, codeChangeEvent{addr, reason})
		},
	}
}

// newTestIBS creates a fresh IntraBlockState backed by a temporary DB,
// with the given tracing hooks wired into both the IBS and the returned
// Config. The caller must defer cleanup of tx and sd.
func newTestIBS(t *testing.T, tracer *tracing.Hooks) *Config {
	t.Helper()

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { tx.Rollback() })

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(func() { sd.Close() })

	ibs := state.New(state.NewReaderV3(sd.AsGetter(tx)))
	ibs.SetHooks(tracer)

	cfg := new(Config)
	setDefaults(cfg)
	cfg.State = ibs
	cfg.EVMConfig.Tracer = tracer
	return cfg
}

// TestCreateEmitsNonceChangeContractCreator verifies that evm.Create
// increments the caller's nonce with NonceChangeContractCreator.
//
// The EVM flow is:
//
//	evm.create → SetNonce(caller, nonce+1, NonceChangeContractCreator)
func TestCreateEmitsNonceChangeContractCreator(t *testing.T) {
	var nonceEvents []nonceChangeEvent
	var codeEvents []codeChangeEvent

	// Init code: PUSH1 0x00 PUSH1 0x00 RETURN (deploys empty contract)
	initCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.RETURN)}

	cfg := newTestIBS(t, recordingTracer(&nonceEvents, &codeEvents))

	_, _, _, err := Create(initCode, cfg, 0)
	require.NoError(t, err)

	// Filter for NonceChangeContractCreator events.
	var creatorEvents []nonceChangeEvent
	for _, e := range nonceEvents {
		if e.Reason == tracing.NonceChangeContractCreator {
			creatorEvents = append(creatorEvents, e)
		}
	}

	require.Len(t, creatorEvents, 1,
		"CREATE must emit exactly one NonceChangeContractCreator event")
	require.Equal(t, cfg.Origin, creatorEvents[0].Addr,
		"NonceChangeContractCreator must be for the caller address")
	require.Equal(t, uint64(0), creatorEvents[0].Prev,
		"caller nonce should start at 0")
	require.Equal(t, uint64(1), creatorEvents[0].New,
		"caller nonce should be incremented to 1")
}

// TestCreateEmitsNonceChangeNewContract verifies that evm.Create sets
// the new contract's nonce to 1 with NonceChangeNewContract (EIP-161).
//
// The EVM flow (post-SpuriousDragon) is:
//
//	evm.create → CreateAccount(addr) → SetNonce(addr, 1, NonceChangeNewContract)
func TestCreateEmitsNonceChangeNewContract(t *testing.T) {
	var nonceEvents []nonceChangeEvent
	var codeEvents []codeChangeEvent

	initCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.RETURN)}

	cfg := newTestIBS(t, recordingTracer(&nonceEvents, &codeEvents))

	_, contractAddr, _, err := Create(initCode, cfg, 0)
	require.NoError(t, err)

	newContractAddr := accounts.InternAddress(contractAddr)

	var newContractEvents []nonceChangeEvent
	for _, e := range nonceEvents {
		if e.Reason == tracing.NonceChangeNewContract {
			newContractEvents = append(newContractEvents, e)
		}
	}

	require.Len(t, newContractEvents, 1,
		"CREATE must emit exactly one NonceChangeNewContract event")
	require.Equal(t, newContractAddr, newContractEvents[0].Addr,
		"NonceChangeNewContract must be for the new contract address")
	require.Equal(t, uint64(0), newContractEvents[0].Prev,
		"new contract nonce should start at 0")
	require.Equal(t, uint64(1), newContractEvents[0].New,
		"new contract nonce should be set to 1 (EIP-161)")
}

// TestCreateEmitsCodeChangeContractCreation verifies that evm.Create
// sets the new contract's code with CodeChangeContractCreation upon
// successful deployment.
//
// The EVM flow is:
//
//	evm.create → run init code → SetCode(addr, ret, CodeChangeContractCreation)
func TestCreateEmitsCodeChangeContractCreation(t *testing.T) {
	var nonceEvents []nonceChangeEvent
	var codeEvents []codeChangeEvent

	// Init code that returns 1 byte (0xFF) as runtime code:
	//   PUSH1 0xFF  → memory[0] = 0xFF
	//   PUSH1 0x00
	//   MSTORE8
	//   PUSH1 0x01  → return 1 byte from offset 0
	//   PUSH1 0x00
	//   RETURN
	initCode := []byte{
		byte(vm.PUSH1), 0xFF,
		byte(vm.PUSH1), 0x00,
		byte(vm.MSTORE8),
		byte(vm.PUSH1), 0x01,
		byte(vm.PUSH1), 0x00,
		byte(vm.RETURN),
	}

	cfg := newTestIBS(t, recordingTracer(&nonceEvents, &codeEvents))

	_, contractAddr, _, err := Create(initCode, cfg, 0)
	require.NoError(t, err)

	newContractAddr := accounts.InternAddress(contractAddr)

	var creationEvents []codeChangeEvent
	for _, e := range codeEvents {
		if e.Reason == tracing.CodeChangeContractCreation {
			creationEvents = append(creationEvents, e)
		}
	}

	require.Len(t, creationEvents, 1,
		"CREATE must emit exactly one CodeChangeContractCreation event")
	require.Equal(t, newContractAddr, creationEvents[0].Addr,
		"CodeChangeContractCreation must be for the new contract address")
}

// TestCreateReasonOrdering verifies the complete ordering of reason-code
// events during a successful CREATE: caller nonce bump, then new contract
// nonce initialization, then code deployment.
func TestCreateReasonOrdering(t *testing.T) {
	type event struct {
		Kind   string // "nonce" or "code"
		Reason string
	}
	var events []event

	tracer := &tracing.Hooks{
		OnNonceChangeV2: func(_ accounts.Address, _, _ uint64, reason tracing.NonceChangeReason) {
			events = append(events, event{"nonce", reason.String()})
		},
		OnCodeChangeV2: func(_ accounts.Address, _ accounts.CodeHash, _ []byte, _ accounts.CodeHash, _ []byte, reason tracing.CodeChangeReason) {
			events = append(events, event{"code", reason.String()})
		},
	}

	initCode := []byte{
		byte(vm.PUSH1), 0xFF,
		byte(vm.PUSH1), 0x00,
		byte(vm.MSTORE8),
		byte(vm.PUSH1), 0x01,
		byte(vm.PUSH1), 0x00,
		byte(vm.RETURN),
	}

	cfg := newTestIBS(t, tracer)

	_, _, _, err := Create(initCode, cfg, 0)
	require.NoError(t, err)

	expected := []event{
		{"nonce", "ContractCreator"},
		{"nonce", "NewContract"},
		{"code", "ContractCreation"},
	}
	require.Equal(t, expected, events,
		"CREATE must emit reason-coded events in order: caller nonce → contract nonce → code deployment")
}

// TestCodeChangeUnspecifiedOnSetup verifies that runtime.Execute's initial
// SetCode (which loads the contract code before execution) emits
// CodeChangeUnspecified — not a deployment-specific reason. This ensures
// the setup path is distinguishable from actual contract creation.
func TestCodeChangeUnspecifiedOnSetup(t *testing.T) {
	var codeEvents []codeChangeEvent

	tracer := &tracing.Hooks{
		OnCodeChangeV2: func(addr accounts.Address, _ accounts.CodeHash, _ []byte, _ accounts.CodeHash, _ []byte, reason tracing.CodeChangeReason) {
			codeEvents = append(codeEvents, codeChangeEvent{addr, reason})
		},
	}

	// Simple code: STOP
	code := []byte{byte(vm.STOP)}
	cfg := newTestIBS(t, tracer)

	_, _, err := Execute(code, nil, cfg, t.TempDir())
	require.NoError(t, err)

	// The setup SetCode in runtime.Execute must produce CodeChangeUnspecified.
	require.NotEmpty(t, codeEvents,
		"runtime.Execute should emit at least one code change for setup")
	require.Equal(t, tracing.CodeChangeUnspecified, codeEvents[0].Reason,
		"setup SetCode must use CodeChangeUnspecified, not a deployment reason")
}

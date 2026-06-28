package commands

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/decodedstate"
	"github.com/erigontech/erigon/rpc"
)

func TestDecodedStateCheckLooksLikeAddressHash(t *testing.T) {
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	require.True(t, looksLikeAddressHash(common.BytesToHash(common.LeftPadBytes(addr[:], 32))))
	require.False(t, looksLikeAddressHash(common.HexToHash("0x0100000000000000000000001111111111111111111111111111111111111111")))
}

func TestDecodedStateCheckCollectBalanceCandidates(t *testing.T) {
	slot := common.HexToHash("0x01")
	addrA := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	addrB := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	addrC := common.HexToAddress("0x00000000000000000000000000000000000000cc")

	storage := map[common.Hash][]decodedstate.DecodedEntry{
		slot: {
			{
				MappingSlot: slot,
				EntryType:   decodedstate.MappingEntry,
				Keys:        []common.Hash{common.BytesToHash(common.LeftPadBytes(addrA[:], 32))},
				Value:       common.HexToHash("0x10"),
			},
			{
				MappingSlot: slot,
				EntryType:   decodedstate.MappingEntry,
				Keys:        []common.Hash{common.BytesToHash(common.LeftPadBytes(addrB[:], 32))},
				Value:       common.HexToHash("0x20"),
			},
			{
				MappingSlot: slot,
				EntryType:   decodedstate.MappingEntry,
				Keys:        []common.Hash{common.BytesToHash(common.LeftPadBytes(addrC[:], 32))},
				Value:       common.HexToHash("0x21"),
			},
			{
				MappingSlot: slot,
				EntryType:   decodedstate.MappingEntry,
				Keys:        []common.Hash{common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000099")},
				Value:       common.HexToHash("0x30"),
			},
		},
	}

	candidates := collectBalanceCandidates(storage, 3)
	require.Len(t, candidates, 1)
	require.Equal(t, slot, candidates[0].Slot)
	require.Len(t, candidates[0].Samples, 3)
	require.Equal(t, addrA, candidates[0].Samples[0].Address)
	require.Equal(t, addrB, candidates[0].Samples[1].Address)
	require.Equal(t, addrC, candidates[0].Samples[2].Address)
}

func TestDecodedStateCheckCollectAllowanceCandidates(t *testing.T) {
	slot := common.HexToHash("0x02")
	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	spender := common.HexToAddress("0x00000000000000000000000000000000000000bb")

	storage := map[common.Hash][]decodedstate.DecodedEntry{
		slot: {
			{
				MappingSlot: slot,
				EntryType:   decodedstate.NestedMappingEntry,
				Keys: []common.Hash{
					common.BytesToHash(common.LeftPadBytes(owner[:], 32)),
					common.BytesToHash(common.LeftPadBytes(spender[:], 32)),
				},
				Value: common.HexToHash("0x40"),
			},
			{
				MappingSlot: slot,
				EntryType:   decodedstate.NestedMappingEntry,
				Keys: []common.Hash{
					common.BytesToHash(common.LeftPadBytes(owner[:], 32)),
					common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000001"),
				},
				Value: common.HexToHash("0x50"),
			},
		},
	}

	candidates := collectAllowanceCandidates(storage, 3)
	require.Len(t, candidates, 0)

	storage[slot] = append(storage[slot], decodedstate.DecodedEntry{
		MappingSlot: slot,
		EntryType:   decodedstate.NestedMappingEntry,
		Keys: []common.Hash{
			common.BytesToHash(common.LeftPadBytes(common.FromHex("0x00000000000000000000000000000000000000cc"), 32)),
			common.BytesToHash(common.LeftPadBytes(common.FromHex("0x00000000000000000000000000000000000000dd"), 32)),
		},
		Value: common.HexToHash("0x60"),
	})

	candidates = collectAllowanceCandidates(storage, 3)
	require.Len(t, candidates, 1)
	require.Equal(t, slot, candidates[0].Slot)
	require.Len(t, candidates[0].Samples, 2)
	require.Equal(t, owner, candidates[0].Samples[0].Owner)
	require.Equal(t, spender, candidates[0].Samples[0].Spender)
}

func TestDecodedStateCheckMappingSlotsDiffer(t *testing.T) {
	slot := common.HexToHash("0x33")
	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	spender := common.HexToAddress("0x00000000000000000000000000000000000000bb")

	balanceSlot := mappingSlotForAddress(owner, slot)
	allowanceSlot := nestedMappingSlotForAddresses(owner, spender, slot)

	require.NotEqual(t, common.Hash{}, balanceSlot)
	require.NotEqual(t, common.Hash{}, allowanceSlot)
	require.NotEqual(t, balanceSlot, allowanceSlot)
}

// Bug 1: eth_call data must carry a 0x prefix. A bare 4-byte selector
// (decimals/totalSupply call sites) must be normalized; an already-prefixed
// string must be left untouched (no double prefix).
func TestDecodedStateCheckEnsureHexPrefix(t *testing.T) {
	require.Equal(t, "0x313ce567", ensureHexPrefix("313ce567"))
	require.Equal(t, "0x18160ddd", ensureHexPrefix("18160ddd"))
	require.Equal(t, "0x70a08231deadbeef", ensureHexPrefix("0x70a08231deadbeef"))
	require.Equal(t, "0x", ensureHexPrefix(""))
	require.Equal(t, "0x", ensureHexPrefix("0x"))
}

// Bug 1 (behavioral): erc20Uint must send data with a 0x prefix to the node
// regardless of how the caller spelled it, so a bare selector no longer trips
// "cannot unmarshal hex string without 0x prefix".
func TestDecodedStateCheckErc20UintNormalizesData(t *testing.T) {
	var seen string
	caller := ethCallerFunc(func(_ context.Context, result any, method string, args ...any) error {
		require.Equal(t, "eth_call", method)
		require.NotEmpty(t, args)
		call := args[0].(map[string]any)
		seen = call["data"].(string)
		if out, ok := result.(*hexString); ok {
			*out = hexString("0x0000000000000000000000000000000000000000000000000000000000000012")
		}
		return nil
	})

	blockArg := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(1))
	v, err := erc20Uint(context.Background(), caller, common.Address{}, "313ce567", blockArg)
	require.NoError(t, err)
	require.Equal(t, "0x313ce567", seen)
	require.Equal(t, uint64(18), v.Uint64())
}

// Bug 2: an "execution reverted" error from the ERC20 probe means the contract
// is not a verifiable ERC20 and must be classified as such, not as a hard error.
func TestDecodedStateCheckIsRevertErr(t *testing.T) {
	require.True(t, isRevertErr(errors.New("execution reverted")))
	require.True(t, isRevertErr(errors.New("execution reverted: insufficient balance")))
	require.False(t, isRevertErr(nil))
	require.False(t, isRevertErr(errors.New("connection refused")))
}

// Bug 2: bestBalanceCandidate must skip (ok=false, err=nil), not error, when the
// balanceOf probe reverts on a non-ERC20 contract.
func TestDecodedStateCheckBestBalanceCandidateRevertSkips(t *testing.T) {
	slot := common.HexToHash("0x01")
	storage := erc20LikeStorage(slot)

	caller := ethCallerFunc(func(_ context.Context, _ any, _ string, _ ...any) error {
		return errors.New("execution reverted")
	})

	_, ok, err := bestBalanceCandidate(context.Background(), caller, common.Address{}, 1, storage)
	require.NoError(t, err)
	require.False(t, ok)
}

// Bug 2: a non-revert RPC error (e.g. transport failure) must still propagate as
// a hard error from the probe.
func TestDecodedStateCheckBestBalanceCandidateHardError(t *testing.T) {
	slot := common.HexToHash("0x01")
	storage := erc20LikeStorage(slot)

	caller := ethCallerFunc(func(_ context.Context, _ any, _ string, _ ...any) error {
		return errors.New("connection refused")
	})

	_, _, err := bestBalanceCandidate(context.Background(), caller, common.Address{}, 1, storage)
	require.Error(t, err)
}

// Bug 2: a real ERC20 whose decoded values match canonical must PASS.
func TestDecodedStateCheckInspectPass(t *testing.T) {
	slot := common.HexToHash("0x01")
	storage := erc20LikeStorage(slot)
	caller := matchingERC20Caller(storage, slot)

	report, status, err := inspectDecodedContract(context.Background(), storage, caller, 1, common.Address{})
	require.NoError(t, err)
	require.Equal(t, decodedCheckPass, status)
	require.NotNil(t, report)
	require.True(t, report.Pass)
}

// Bug 2: a real ERC20 whose decoded value disagrees with canonical balanceOf is
// a genuine decoded bug and must FAIL (not be silently skipped).
func TestDecodedStateCheckInspectFailOnMismatch(t *testing.T) {
	slot := common.HexToHash("0x01")
	storage := erc20LikeStorage(slot)
	caller := matchingERC20Caller(storage, slot)
	// Corrupt one canonical balanceOf answer so decoded != canonical.
	caller.overrideBalance = map[common.Address]common.Hash{
		decodedSampleAddr("aa"): common.HexToHash("0xdead"),
	}

	report, status, err := inspectDecodedContract(context.Background(), storage, caller, 1, common.Address{})
	require.NoError(t, err)
	require.Equal(t, decodedCheckFail, status)
	require.NotNil(t, report)
	require.False(t, report.Pass)
}

// Bug 2: a contract whose balanceOf probe reverts everywhere is SKIPPED, not
// FAILED, and produces no genuine-failure signal.
func TestDecodedStateCheckInspectSkipOnRevert(t *testing.T) {
	slot := common.HexToHash("0x01")
	storage := erc20LikeStorage(slot)
	caller := ethCallerFunc(func(_ context.Context, _ any, _ string, _ ...any) error {
		return errors.New("execution reverted")
	})

	report, status, err := inspectDecodedContract(context.Background(), storage, caller, 1, common.Address{})
	require.NoError(t, err)
	require.Equal(t, decodedCheckSkipped, status)
	require.Nil(t, report)
}

type ethCallerFunc func(ctx context.Context, result any, method string, args ...any) error

func (f ethCallerFunc) CallContext(ctx context.Context, result any, method string, args ...any) error {
	return f(ctx, result, method, args...)
}

func decodedSampleAddr(suffix string) common.Address {
	return common.HexToAddress("0x00000000000000000000000000000000000000" + suffix)
}

func erc20LikeStorage(slot common.Hash) map[common.Hash][]decodedstate.DecodedEntry {
	mk := func(suffix, val string) decodedstate.DecodedEntry {
		addr := decodedSampleAddr(suffix)
		return decodedstate.DecodedEntry{
			MappingSlot: slot,
			EntryType:   decodedstate.MappingEntry,
			Keys:        []common.Hash{common.BytesToHash(common.LeftPadBytes(addr[:], 32))},
			Value:       common.HexToHash(val),
		}
	}
	return map[common.Hash][]decodedstate.DecodedEntry{
		slot: {mk("aa", "0x10"), mk("bb", "0x20"), mk("cc", "0x21")},
	}
}

type stubERC20Caller struct {
	storage         map[common.Hash][]decodedstate.DecodedEntry
	slot            common.Hash
	overrideBalance map[common.Address]common.Hash
}

func matchingERC20Caller(storage map[common.Hash][]decodedstate.DecodedEntry, slot common.Hash) *stubERC20Caller {
	return &stubERC20Caller{storage: storage, slot: slot}
}

func (s *stubERC20Caller) balanceFor(addr common.Address) common.Hash {
	if v, ok := s.overrideBalance[addr]; ok {
		return v
	}
	for _, entry := range s.storage[s.slot] {
		if len(entry.Keys) == 1 && common.BytesToAddress(entry.Keys[0][12:]) == addr {
			return entry.Value
		}
	}
	return common.Hash{}
}

func (s *stubERC20Caller) CallContext(_ context.Context, result any, method string, args ...any) error {
	switch method {
	case "eth_call":
		call := args[0].(map[string]any)
		data := call["data"].(string)
		// balanceOf(address) selector 0x70a08231; decode the trailing address arg.
		if len(data) >= 10 && data[:10] == "0x70a08231" {
			addr := common.HexToAddress(data[len(data)-40:])
			out := result.(*hexString)
			*out = hexString(s.balanceFor(addr).Hex())
			return nil
		}
		// name/symbol/decimals/totalSupply: return a harmless value.
		out := result.(*hexString)
		*out = hexString("0x0000000000000000000000000000000000000000000000000000000000000012")
		return nil
	case "eth_getStorageAt":
		contract := args[0].(common.Address)
		rawSlot := args[1].(common.Hash)
		out := result.(*hexString)
		*out = hexString(s.rawStorage(contract, rawSlot).Hex())
		return nil
	case "erigon_getMappingValue":
		addrHash := args[2].(common.Hash)
		out := result.(*common.Hash)
		*out = s.balanceFor(common.BytesToAddress(addrHash[12:]))
		return nil
	default:
		return errors.New("unexpected method " + method)
	}
}

func (s *stubERC20Caller) rawStorage(contract common.Address, rawSlot common.Hash) common.Hash {
	for _, entry := range s.storage[s.slot] {
		if len(entry.Keys) != 1 {
			continue
		}
		addr := common.BytesToAddress(entry.Keys[0][12:])
		if mappingSlotForAddress(addr, s.slot) == rawSlot {
			return s.balanceFor(addr)
		}
	}
	return common.Hash{}
}

var _ ethCaller = (*stubERC20Caller)(nil)
var _ ethCaller = ethCallerFunc(nil)

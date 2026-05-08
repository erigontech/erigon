// Copyright 2026 The Erigon Authors
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

package scenarios_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/lifecycle"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// These tests replicate the publisher's file-flow end-to-end at the
// storage-component level: a fresh "publisher" Inventory + Driver,
// files arriving via the disk-scan path or directly via AddFile, the
// lifecycle's per-step batch hook running, advance to Advertisable
// firing the bridge subscriber. This is the deterministic counterpart
// to the multi-hour mainnet runs — failures are reproducible in
// seconds.
//
// What the tests gate:
//   - Block files placed in snap-dir get discovered, advance through
//     Indexing → Indexed → Advertisable. (publisher_block_flow)
//   - State files (commitment.kv, accounts.kv, etc.) placed in
//     subdirs of snap-dir get discovered. (publisher_state_subdir,
//     EXPECTED FAIL today — discoverNewFiles only scans top-level.)
//   - The bridge subscriber fires Seed + RepublishChainToml when
//     files reach Advertisable. (publisher_bridge_fires)

// fakeBridge captures Seed + RepublishChainToml calls a real provider
// would make, mirrors the bridge subscriber the production code wires
// in. Provides assertions on what advanced-to-Advertisable transitions
// triggered.
type fakeBridge struct {
	mu             sync.Mutex
	seedCalls      [][]string
	republishCalls int32
}

func (f *fakeBridge) seed(names []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dup := append([]string(nil), names...)
	f.seedCalls = append(f.seedCalls, dup)
}

func (f *fakeBridge) republish() error {
	atomic.AddInt32(&f.republishCalls, 1)
	return nil
}

func (f *fakeBridge) seedNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, batch := range f.seedCalls {
		out = append(out, batch...)
	}
	return out
}

// fakeBindingValidator simulates CommitmentDomainValidator's binding
// side-effect (registering a step→block boundary in the inventory)
// without the DB / BlockReader infrastructure. When invoked on a
// commitment-domain step group, it registers the binding the real
// validator would derive from KeyCommitmentState. Non-commitment
// groups are no-op.
//
// Lets the test exercise the full lifecycle path — state file landed
// → step batch validation runs → CommitmentDomainValidator registers
// binding → block-step wait gate releases — without standing up a
// real chain DB.
type fakeBindingValidator struct {
	inv     *snapshot.Inventory
	binding func(toStep uint64) (block uint64, ok bool)
	fired   int32
	fireMu  sync.Mutex
	fireLog []uint64
}

func (*fakeBindingValidator) Name() string { return "fake_commitment_binding" }

func (v *fakeBindingValidator) ValidateStep(_ context.Context, files []*snapshot.FileEntry) error {
	if len(files) == 0 || files[0].Domain != snapshot.DomainCommitment {
		return nil
	}
	toStep := files[0].ToStep
	block, ok := v.binding(toStep)
	if !ok {
		return nil
	}
	v.inv.RegisterStepBlockBoundary(toStep, block)
	atomic.AddInt32(&v.fired, 1)
	v.fireMu.Lock()
	v.fireLog = append(v.fireLog, toStep)
	v.fireMu.Unlock()
	return nil
}

// buildPublisherStack wires the same shape the production provider
// builds: a Driver with batch-validation OnValidation handler + a
// goroutine subscribed to inventory ChangeSets that calls the
// fakeBridge on advance-to-Advertisable. Returns the Driver, the
// Inventory, and the fakeBridge for assertions.
func buildPublisherStack(t *testing.T, ctx context.Context, snapDir string) (*lifecycle.Driver, *snapshot.Inventory, *fakeBridge) {
	t.Helper()
	return buildPublisherStackWithChain(t, ctx, snapDir, nil)
}

// buildPublisherStackWithChain builds the publisher stack with the
// given extra StepValidators inserted BEFORE AllFilesPresent. Used by
// tests that want to plug in a fake CommitmentDomainValidator-like
// step validator without the DB infrastructure.
func buildPublisherStackWithChain(t *testing.T, ctx context.Context, snapDir string, extra validation.StepChain) (*lifecycle.Driver, *snapshot.Inventory, *fakeBridge) {
	t.Helper()
	inv := snapshot.NewInventory()
	bridge := &fakeBridge{}

	chain := append(validation.StepChain{}, extra...)
	chain = append(chain, validation.AllFilesPresent{SnapDir: snapDir})

	driver := &lifecycle.Driver{
		Inv:           inv,
		SnapDir:       snapDir,
		SweepInterval: 100 * time.Millisecond,
		OnIndexing: func(_ context.Context, e *snapshot.FileEntry) error {
			// Test harness: simulate "deps already local" by
			// advancing directly to Indexed. Real production builds
			// dep files via BuildMissedIndices.
			inv.AdvanceTo(e.Name, snapshot.LifecycleIndexed)
			return nil
		},
		OnValidation: lifecycle.BuildOnBatchValidation(chain, inv, nil),
	}

	// Bridge subscriber — mirrors production provider.go:325.
	sub, unsub := inv.Subscribe()
	go func() {
		defer unsub()
		for {
			select {
			case <-ctx.Done():
				return
			case cs, ok := <-sub:
				if !ok {
					return
				}
				var advanced []string
				for _, name := range cs.Files {
					state, exists := inv.LifecycleState(name)
					if !exists {
						continue
					}
					if state == snapshot.LifecycleAdvertisable {
						// Mirror production provider.go: translate
						// basename → snap-dir-relative form before
						// handing off to the downloader.
						advanced = append(advanced, snapshot.RelPathForName(name))
					}
				}
				if len(advanced) > 0 {
					bridge.seed(advanced)
					_ = bridge.republish()
				}
			}
		}
	}()

	require.NoError(t, driver.Start(ctx))
	t.Cleanup(driver.Stop)
	return driver, inv, bridge
}

// TestPublisher_BlockFlow_AdvancesToAdvertisable: block files arriving
// via disk-scan should advance through Indexed → Advertisable, with
// the bridge firing Seed + RepublishChainToml.
//
// This tests the happy path with a (step, block) binding pre-registered
// (simulating either a bootstrap commitment file or a post-tip retire
// commitment that already validated). On 2026-05-06 mainnet publisher 5
// run this path was BROKEN: 0 advance-to-Advertisable despite 189
// reaching Indexed, because no binding existed.
func TestPublisher_BlockFlow_AdvancesToAdvertisable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snapDir := t.TempDir()
	_, inv, bridge := buildPublisherStack(t, ctx, snapDir)

	// Pre-register a (step, block) binding so the block-step wait
	// gate releases. In production this comes from the bootstrap
	// commitment seeder OR from a fresh commitment file going
	// through the lifecycle's batch validation.
	inv.RegisterStepBlockBoundary(8957, 25_030_000)

	// Drop block files in snap-dir simulating retire output.
	// Step group: headers + bodies for blocks 25020-25030.
	wantNames := []string{
		"v1.1-025020-025030-headers.seg",
		"v1.1-025020-025030-bodies.seg",
	}
	for _, n := range wantNames {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, n), []byte("x"), 0o644))
	}

	// Wait for them to reach Advertisable.
	require.Eventually(t, func() bool {
		for _, n := range wantNames {
			state, ok := inv.LifecycleState(n)
			if !ok || state != snapshot.LifecycleAdvertisable {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond,
		"both files should advance to Advertisable; current states: "+
			func() string {
				s := ""
				for _, n := range wantNames {
					st, ok := inv.LifecycleState(n)
					s += n + "=" + map[bool]string{true: st.String(), false: "not-in-inv"}[ok] + " "
				}
				return s
			}())

	// Bridge subscriber fired Seed + RepublishChainToml for the
	// advanced files.
	require.GreaterOrEqual(t, atomic.LoadInt32(&bridge.republishCalls), int32(1),
		"RepublishChainToml should have fired at least once")
	seeded := bridge.seedNames()
	for _, n := range wantNames {
		require.Contains(t, seeded, n,
			"%q should have been Seed-ed by the bridge subscriber", n)
	}
}

// TestPublisher_BlockFlow_BlockedWithoutBinding: without a (step, block)
// binding registered, block files should sit at Indexed indefinitely.
// This is the "wait for binding" gate working as designed.
func TestPublisher_BlockFlow_BlockedWithoutBinding(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snapDir := t.TempDir()
	_, inv, bridge := buildPublisherStack(t, ctx, snapDir)

	// NO binding registered.

	wantNames := []string{
		"v1.1-025020-025030-headers.seg",
		"v1.1-025020-025030-bodies.seg",
	}
	for _, n := range wantNames {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, n), []byte("x"), 0o644))
	}

	// Wait for files to be discovered + reach Indexed (the gate is
	// AT the Indexed→Advertisable transition).
	require.Eventually(t, func() bool {
		for _, n := range wantNames {
			state, ok := inv.LifecycleState(n)
			if !ok || state != snapshot.LifecycleIndexed {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond,
		"files should reach Indexed (block-step wait gate stops them there)")

	// Hold for half a second to confirm they DON'T advance further.
	time.Sleep(500 * time.Millisecond)
	for _, n := range wantNames {
		state, _ := inv.LifecycleState(n)
		require.Equal(t, snapshot.LifecycleIndexed, state,
			"%q must stay at Indexed without a binding", n)
	}
	require.Equal(t, int32(0), atomic.LoadInt32(&bridge.republishCalls),
		"bridge should NOT fire when files stay at Indexed")
}

// TestPublisher_StateFiles_DiscoveredFromSubdirs verifies that state
// files (placed in domain/, history/, accessor/ subdirectories of
// snap-dir per Erigon's actual layout) are discovered by the
// lifecycle. On 2026-05-06 this FAILS — discoverNewFiles only scans
// the top-level snap-dir, not subdirectories — meaning commitment
// files never go through the lifecycle, no (step, block) bindings
// register via the lifecycle path, and the block-step wait gate
// blocks ALL block files on a fresh publisher.
func TestPublisher_StateFiles_DiscoveredFromSubdirs(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snapDir := t.TempDir()
	// Erigon places state files in subdirectories of snap-dir.
	// Recreate the layout.
	require.NoError(t, os.MkdirAll(filepath.Join(snapDir, "domain"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(snapDir, "history"), 0o755))

	_, inv, _ := buildPublisherStack(t, ctx, snapDir)

	// Drop a commitment.kv in domain/ — this is what the publisher
	// retires post-tip; the lifecycle MUST discover it for the
	// CommitmentDomainValidator to fire and register a binding.
	commitmentName := "v1.1-commitment.0-256.kv"
	require.NoError(t, os.WriteFile(
		filepath.Join(snapDir, "domain", commitmentName),
		[]byte("x"), 0o644))

	// Wait for the file to be discovered. The disk scan registers
	// state files by their relative-to-snap-dir path so they match
	// the keys the aggregator's onFilesChange / onFilesDelete
	// callbacks use (e.g. "domain/v1.1-commitment.0-256.kv").
	require.Eventually(t, func() bool {
		_, ok := inv.LifecycleState(snapshot.RelPathForName(commitmentName))
		return ok
	}, 5*time.Second, 50*time.Millisecond,
		"commitment file in domain/ subdir should be discovered by the lifecycle")
}

// TestPublisher_StateFile_RegistersBinding_UnblocksBlockFiles is the
// fresh-publisher end-to-end gate: a commitment file lands in the
// domain/ subdir, gets discovered, runs through the lifecycle's per-
// step batch validation chain, the (fake) CommitmentDomainValidator
// registers a (step, block) binding, AND block files sitting at
// Indexed under a wait-for-binding gate then unblock and advance to
// Advertisable.
//
// This is the test that would have failed on the 2026-05-06 mainnet
// publisher 5 run (0 advance-to-Advertisable for 189 indexed files):
// no commitment file goes through the lifecycle on a fresh publisher
// → no binding → block-step wait gate stays closed forever.
func TestPublisher_StateFile_RegistersBinding_UnblocksBlockFiles(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snapDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(snapDir, "domain"), 0o755))

	// Wire the binding-registering step validator FIRST in the chain.
	// In production, CommitmentDomainValidator runs first; on success
	// it registers the binding via Inventory.RegisterStepBlockBoundary.
	// AllFilesPresent then runs as a presence floor-check.
	binding := &fakeBindingValidator{}
	binding.binding = func(toStep uint64) (uint64, bool) {
		// Map step 8958 → block 25_030_000 (matches what a real
		// commitment-domain file at step [8957, 8958) would attest to).
		if toStep == 8958 {
			return 25_030_000, true
		}
		return 0, false
	}
	_, inv, bridge := buildPublisherStackWithChain(t, ctx, snapDir,
		validation.StepChain{binding})
	binding.inv = inv

	// Drop the commitment file in domain/ (the path the publisher's
	// post-tip retire produces).
	commitmentName := "v1.1-commitment.8957-8958.kv"
	require.NoError(t, os.WriteFile(
		filepath.Join(snapDir, "domain", commitmentName),
		[]byte("x"), 0o644))

	// Drop block files in top-level — these will sit at Indexed
	// behind the block-step wait gate until the binding registers.
	wantBlockFiles := []string{
		"v1.1-025020-025030-headers.seg",
		"v1.1-025020-025030-bodies.seg",
	}
	for _, n := range wantBlockFiles {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, n), []byte("x"), 0o644))
	}

	// Wait for the binding to be registered (proves: state file
	// discovered → lifecycle ran the binding validator on it →
	// binding registered).
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&binding.fired) > 0
	}, 5*time.Second, 50*time.Millisecond,
		"binding validator should fire on the discovered commitment file")
	_, hasBinding := inv.BlockToStep(25_030_000)
	require.True(t, hasBinding,
		"inventory should have a (step, block) binding after binding validator ran")

	// And — the gating result — block files unblock and advance to
	// Advertisable.
	require.Eventually(t, func() bool {
		for _, n := range wantBlockFiles {
			state, ok := inv.LifecycleState(n)
			if !ok || state != snapshot.LifecycleAdvertisable {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond,
		"block files should advance to Advertisable once the binding registers; current states: "+
			func() string {
				s := ""
				for _, n := range wantBlockFiles {
					st, ok := inv.LifecycleState(n)
					s += n + "=" + map[bool]string{true: st.String(), false: "not-in-inv"}[ok] + " "
				}
				return s
			}())

	// Bridge subscriber fires for the advanced files.
	require.GreaterOrEqual(t, atomic.LoadInt32(&bridge.republishCalls), int32(1),
		"RepublishChainToml should fire for advanced files")
	seeded := bridge.seedNames()
	for _, n := range wantBlockFiles {
		require.Contains(t, seeded, n,
			"%q should have been Seed-ed by the bridge subscriber", n)
	}

	// And — the commitment file ITSELF must reach Advertisable (the
	// publisher needs to seed it to peers; staying at Indexed means
	// the bridge never fires on it). On 2026-05-06 this fails because
	// AllFilesPresent stats SnapDir/{name} but state files actually
	// live at SnapDir/domain/{name} → presence check fails on subdir
	// files → step never reaches Advertisable.
	commitmentRel := snapshot.RelPathForName(commitmentName)
	require.Eventually(t, func() bool {
		state, ok := inv.LifecycleState(commitmentRel)
		return ok && state == snapshot.LifecycleAdvertisable
	}, 5*time.Second, 50*time.Millisecond,
		"commitment file in domain/ subdir should ALSO reach Advertisable so the publisher can seed it")
	require.Contains(t, seeded, commitmentRel,
		"the commitment file must be seeded too — without it the publisher can't share the binding with peers")
}

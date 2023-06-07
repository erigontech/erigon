package regression

import (
	"runtime"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/log/v3"
)

const (
	regressionPath        = "regression"
	startingStatePath     = "beaconState/0/data.bin"
	signedBeaconBlockPath = "signedBeaconBlock"
)

type RegressionTester struct {
	testDirectory string

	// block list
	blockList []*cltypes.SignedBeaconBlock
}

func NewRegressionTester(testDirectory string) (*RegressionTester, error) {
	reg := &RegressionTester{
		testDirectory: testDirectory,
	}
	return reg, reg.initBlocks()
}

func (r *RegressionTester) Run(name string, fn func(*forkchoice.ForkChoiceStore, *cltypes.SignedBeaconBlock) error, step int) error {
	for true {
		state, err := r.readStartingState()
		if err != nil {
			return err
		}
		store, err := forkchoice.NewForkChoiceStore(state, nil, nil, true)
		if err != nil {
			return err
		}
		log.Info("Loading public keys into memory")
		bls.SetEnabledCaching(true)
		state.ForEachValidator(func(v solid.Validator, idx, total int) bool {
			pk := v.PublicKey()
			if err := bls.LoadPublicKeyIntoCache(pk[:], false); err != nil {
				panic(err)
			}
			return true
		})
		store.OnTick(uint64(time.Now().Unix()))
		begin := time.Now()
		beginStep := time.Now()
		log.Info("Starting test, CTRL+C to stop.", "name", name)
		for _, block := range r.blockList {
			if err := fn(store, block); err != nil {
				return err
			}
			if block.Block.Slot%uint64(step) == 0 {
				elapsed := time.Since(beginStep)
				log.Info("Processed", "slot", block.Block.Slot, "elapsed", elapsed, "sec/blk", elapsed/time.Duration(step))
				beginStep = time.Now()
			}
		}

		var m runtime.MemStats
		dbg.ReadMemStats(&m)
		sum := time.Since(begin)
		log.Info("Finished/Restarting test", "name", name, "averageBlockTime", sum/time.Duration(len(r.blockList)), "sys", common.ByteCount(m.Sys))
	}
	return nil
}

// 3 regression tests

// Test fullValidation=true
func TestRegressionWithValidation(store *forkchoice.ForkChoiceStore, block *cltypes.SignedBeaconBlock) error {
	if err := store.OnBlock(block, false, true); err != nil {
		return err
	}
	block.Block.Body.Attestations.Range(func(index int, value *solid.Attestation, length int) bool {
		store.OnAttestation(value, true)
		return true
	})
	return nil
}

// Test fullValidation=false
func TestRegressionWithoutValidation(store *forkchoice.ForkChoiceStore, block *cltypes.SignedBeaconBlock) error {
	if err := store.OnBlock(block, false, false); err != nil {
		return err
	}
	return nil
}

// Test fullValidation=false, one badBlock, one good block (intervaled)
func TestRegressionBadBlocks(store *forkchoice.ForkChoiceStore, block *cltypes.SignedBeaconBlock) error {
	block.Block.ProposerIndex++
	store.OnBlock(block, false, false)
	// restore the block
	block.Block.ProposerIndex--
	if err := store.OnBlock(block, false, false); err != nil {
		return err
	}
	return nil
}

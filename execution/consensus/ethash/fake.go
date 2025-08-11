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

package ethash

import (
	"time"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/ethash/ethashcfg"
	"github.com/erigontech/erigon/execution/types"
)

type FakeEthash struct {
	Ethash
	fakeFail  uint64        // Block number which fails PoW check even in fake mode
	fakeDelay time.Duration // Time delay to sleep for before returning from verify
}

// NewFakeFailer creates a ethash consensus engine with a fake PoW scheme that
// accepts all blocks as valid apart from the single one specified, though they
// still have to conform to the Ethereum consensus rules.
func NewFakeFailer(fail uint64) *FakeEthash {
	return &FakeEthash{
		Ethash:   newFakeEth(ethashcfg.ModeFake),
		fakeFail: fail,
	}
}

// NewFaker creates a ethash consensus engine with a fake PoW scheme that accepts
// all blocks' seal as valid, though they still have to conform to the Ethereum
// consensus rules.
func NewFaker() *FakeEthash {
	return &FakeEthash{
		Ethash: newFakeEth(ethashcfg.ModeFake),
	}
}

// NewFakeDelayer creates a ethash consensus engine with a fake PoW scheme that
// accepts all blocks as valid, but delays verifications by some time, though
// they still have to conform to the Ethereum consensus rules.
func NewFakeDelayer(delay time.Duration) *FakeEthash {
	return &FakeEthash{
		Ethash:    newFakeEth(ethashcfg.ModeFake),
		fakeDelay: delay,
	}
}

func newFakeEth(mode ethashcfg.Mode) Ethash {
	return Ethash{
		config: ethashcfg.Config{
			PowMode: mode,
			Log:     log.Root(),
		},
	}
}

func (f *FakeEthash) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	err := f.Ethash.VerifyHeader(chain, header, false)
	if err != nil {
		return err
	}

	if seal {
		return f.VerifySeal(chain, header)
	}

	return nil
}

func (f *FakeEthash) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	if len(uncles) > maxUncles {
		return errTooManyUncles
	}
	if len(uncles) == 0 {
		return nil
	}

	uncleBlocks, ancestors := getUncles(chain, header)

	for _, uncle := range uncles {
		if err := f.VerifyUncle(chain, header, uncle, uncleBlocks, ancestors, true); err != nil {
			return err
		}
	}
	return nil
}

func (f *FakeEthash) VerifyUncle(chain consensus.ChainHeaderReader, block *types.Header, uncle *types.Header, uncles mapset.Set[common.Hash], ancestors map[common.Hash]*types.Header, seal bool) error {
	err := f.Ethash.VerifyUncle(chain, block, uncle, uncles, ancestors, false)
	if err != nil {
		return err
	}

	if seal {
		return f.VerifySeal(chain, uncle)
	}
	return nil
}

// If we're running a fake PoW, accept any seal as valid
func (f *FakeEthash) VerifySeal(_ consensus.ChainHeaderReader, header *types.Header) error {
	if f.fakeDelay > 0 {
		time.Sleep(f.fakeDelay)
	}

	if f.fakeFail == header.Number.Uint64() {
		return errInvalidPoW
	}

	return nil
}

// If we're running a fake PoW, simply return a 0 nonce immediately
func (f *FakeEthash) Seal(_ consensus.ChainHeaderReader, blockWithReceipts *types.BlockWithReceipts, results chan<- *types.BlockWithReceipts, stop <-chan struct{}) error {
	block := blockWithReceipts.Block
	receipts := blockWithReceipts.Receipts
	header := block.Header()
	header.Nonce, header.MixDigest = types.BlockNonce{}, common.Hash{}

	select {
	case results <- &types.BlockWithReceipts{Block: block.WithSeal(header), Receipts: receipts}:
	default:
		f.Ethash.config.Log.Warn("Sealing result is not read by miner", "mode", "fake", "sealhash", f.SealHash(block.Header()))
	}

	return nil
}

type FullFakeEthash FakeEthash

// NewFullFaker creates an ethash consensus engine with a full fake scheme that
// accepts all blocks as valid, without checking any consensus rules whatsoever.
func NewFullFaker() *FullFakeEthash {
	return &FullFakeEthash{
		Ethash: Ethash{
			config: ethashcfg.Config{
				PowMode: ethashcfg.ModeFullFake,
				Log:     log.Root(),
			},
		},
	}
}

// If we're running a full engine faking, accept any input as valid
func (f *FullFakeEthash) VerifyHeader(_ consensus.ChainHeaderReader, _ *types.Header, _ bool) error {
	return nil
}

func (f *FullFakeEthash) VerifyUncles(_ consensus.ChainReader, _ *types.Header, _ []*types.Header) error {
	return nil
}

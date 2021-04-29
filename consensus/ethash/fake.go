package ethash

import (
	"time"

	mapset "github.com/deckarep/golang-set"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
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
		Ethash:   newFakeEth(ModeFake),
		fakeFail: fail,
	}
}

// NewFaker creates a ethash consensus engine with a fake PoW scheme that accepts
// all blocks' seal as valid, though they still have to conform to the Ethereum
// consensus rules.
func NewFaker() *FakeEthash {
	return &FakeEthash{
		Ethash: newFakeEth(ModeFake),
	}
}

// NewFakeDelayer creates a ethash consensus engine with a fake PoW scheme that
// accepts all blocks as valid, but delays verifications by some time, though
// they still have to conform to the Ethereum consensus rules.
func NewFakeDelayer(delay time.Duration) *FakeEthash {
	return &FakeEthash{
		Ethash:    newFakeEth(ModeFake),
		fakeDelay: delay,
	}
}

func newFakeEth(mode Mode) Ethash {
	return Ethash{
		config: Config{
			PowMode: mode,
			Log:     log.Root(),
		},
	}
}

func (f *FakeEthash) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) (*consensus.ParentsRequest, error) {
	fakeSeals := make([]bool, len(seals))
	parentsReq, err := f.Ethash.VerifyHeaders(chain, headers, fakeSeals)
	if err != nil {
		return nil, err
	}
	if parentsReq != nil {
		return parentsReq, nil
	}
	for i, header := range headers {
		if seals[i] {
			if err = f.VerifySeal(chain, header); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func (f *FakeEthash) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) (*consensus.ParentsRequest, error) {
	parentsReq, err := f.Ethash.VerifyHeader(chain, header, false)
	if err != nil {
		return nil, err
	}
	if parentsReq != nil {
		return parentsReq, nil
	}

	if seal {
		return nil, f.VerifySeal(chain, header)
	}

	return nil, nil
}

func (f *FakeEthash) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	if len(block.Uncles()) > maxUncles {
		return errTooManyUncles
	}
	if len(block.Uncles()) == 0 {
		return nil
	}

	uncles, ancestors := getUncles(chain, block)

	for _, uncle := range block.Uncles() {
		if err := f.VerifyUncle(chain, block, uncle, uncles, ancestors, true); err != nil {
			return err
		}
	}
	return nil
}

func (f *FakeEthash) VerifyUncle(chain consensus.ChainHeaderReader, block *types.Block, uncle *types.Header, uncles mapset.Set, ancestors map[common.Hash]*types.Header, seal bool) error {
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
func (f *FakeEthash) Seal(_ consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	header := block.Header()
	header.Nonce, header.MixDigest = types.BlockNonce{}, common.Hash{}

	select {
	case results <- block.WithSeal(header):
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
			config: Config{
				PowMode: ModeFullFake,
				Log:     log.Root(),
			},
		},
	}
}

// If we're running a full engine faking, accept any input as valid
func (f *FullFakeEthash) VerifyHeader(_ consensus.ChainHeaderReader, _ *types.Header, _ bool) (*consensus.ParentsRequest, error) {
	return nil, nil
}

// If we're running a full engine faking, accept any input as valid
func (f *FullFakeEthash) VerifyHeaders(_ consensus.ChainHeaderReader, headers []*types.Header, _ []bool) (*consensus.ParentsRequest, error) {
	return nil, nil
}

func (f *FullFakeEthash) VerifyUncles(_ consensus.ChainReader, _ *types.Block) error {
	return nil
}

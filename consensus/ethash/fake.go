package ethash

import (
	"errors"
	"fmt"
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

func (f *FakeEthash) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) (func(), <-chan error) {
	fakeSeals := make([]bool, len(seals))
	fn, results := f.Ethash.VerifyHeaders(chain, headers, fakeSeals)

	errs := make(chan error, cap(results))
	isClosed := make(chan struct{})

	go func() {
		var i int
		var sealErr error
		for {
			select {
			case <-isClosed:
				return
			case res := <-results:
				if seals[i] {
					sealErr = f.VerifySeal(chain, headers[i])
				}

				if res == nil {
					res = sealErr
				}

				select {
				case <-isClosed:
					close(errs)
					return
				case errs <- res:
					// nothing to do
				}

				i++
				if len(headers) == i {
					return
				}
			}
		}
	}()

	closeFn := func() {
		close(isClosed)
		fn()
	}

	return closeFn, errs
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
func (f *FakeEthash) Seal(ctx consensus.Cancel, _ consensus.ChainHeaderReader, block *types.Block, results chan<- consensus.ResultWithContext, stop <-chan struct{}) error {
	header := block.Header()
	header.Nonce, header.MixDigest = types.BlockNonce{}, common.Hash{}

	select {
	case <-stop:
		ctx.CancelFunc()
	case results <- consensus.ResultWithContext{Cancel: ctx, Block: block.WithSeal(header)}:
		// nothing to do
	default:
		f.Ethash.config.Log.Warn("Sealing result is not read by miner", "mode", "fake", "sealhash", f.Ethash.SealHash(block.Header()))
	}
	return nil
}

func (f *FakeEthash) Verify(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header, _ bool, seal bool) error {
	if len(parents) == 0 {
		return errors.New("need a parent to verify the header")
	}
	err := f.verifyHeader(chain, header, parents[len(parents)-1], false, false)
	if err != nil {
		fmt.Println("FAKE-1", err)
		return err
	}

	if seal {
		fmt.Println("FAKE-2", err)
		return f.VerifySeal(chain, header)
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
func (f *FullFakeEthash) VerifyHeader(_ consensus.ChainHeaderReader, _ *types.Header, _ bool) error {
	return nil
}

// If we're running a full engine faking, accept any input as valid
func (f *FullFakeEthash) VerifyHeaders(_ consensus.ChainHeaderReader, headers []*types.Header, _ []bool) (func(), <-chan error) {
	results := make(chan error, len(headers))
	for i := 0; i < len(headers); i++ {
		results <- nil
	}
	return func() {}, results
}

func (f *FullFakeEthash) VerifyUncles(_ consensus.ChainReader, _ *types.Block) error {
	return nil
}

func (f *FullFakeEthash) Verify(_ consensus.ChainHeaderReader, _ *types.Header, parents []*types.Header, _ bool, _ bool) error {
	if len(parents) == 0 {
		return errors.New("need a parent to verify the header")
	}
	return nil
}

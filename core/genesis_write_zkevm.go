package core

import (
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/zkevm/hex"
)

func HermezMainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HermezMainnetChainConfig,
		Timestamp:  1679653163,
		GasLimit:   0x0,
		Difficulty: big.NewInt(0x0),
		Alloc:      readPrealloc("allocs/hermez.json"),
	}
}

func HermezTestnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HermezTestnetChainConfig,
		Timestamp:  1677601932,
		GasLimit:   0x0,
		Difficulty: big.NewInt(0x0),
		Alloc:      readPrealloc("allocs/hermez-testnet.json"),
	}
}

func HermezBlueberryGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HermezBlueberryChainConfig,
		Timestamp:  1676996964,
		GasLimit:   0x0,
		Difficulty: big.NewInt(0x0),
		Alloc:      readPrealloc("allocs/hermez-blueberry.json"),
	}
}

func HermezEtrogGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HermezEtrogChainConfig,
		Timestamp:  1703260380,
		GasLimit:   0x0,
		Difficulty: big.NewInt(0x0),
		Alloc:      readPrealloc("allocs/hermez-etrog.json"),
	}
}

func HermezCardonaGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HermezCardonaChainConfig,
		Timestamp:  1676996964,
		GasLimit:   0x0,
		Difficulty: big.NewInt(0x0),
		Alloc:      readPrealloc("allocs/hermez-cardona.json"),
	}
}

func HermezCardonaInternalGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HermezCardonaInternalChainConfig,
		Timestamp:  1676996964,
		GasLimit:   0x0,
		Difficulty: big.NewInt(0x0),
		Alloc:      readPrealloc("allocs/hermez-cardona-internal.json"),
	}
}

func HermezLocalDevnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HermezLocalDevnetChainConfig,
		Timestamp:  1706732232,
		GasLimit:   0x0,
		Difficulty: big.NewInt(0x0),
		Alloc:      readPrealloc("allocs/hermez-dev.json"),
	}
}

func X1TestnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.X1TestnetChainConfig,
		Timestamp:  1699369668,
		GasLimit:   0x0,
		Difficulty: big.NewInt(0x0),
		Alloc:      readPrealloc("allocs/x1-testnet.json"),
	}
}

func processAccount(s *smt.SMT, root *big.Int, a *types.GenesisAccount, addr libcommon.Address) (*big.Int, error) {

	// store the account balance and nonce
	r, err := s.SetAccountState(addr.String(), a.Balance, new(big.Int).SetUint64(a.Nonce))
	if err != nil {
		return nil, err
	}

	if len(a.Code) > 0 {
		xs := hex.EncodeToString(a.Code)
		err = s.SetContractBytecode(addr.String(), xs)
		if err != nil {
			return nil, err
		}
	}

	// parse the storage into map[string]string by splitting the storage hex into two 32 bit values
	sm := make(map[string]string)
	for k, v := range a.Storage {
		sm[k.String()] = v.String()
	}

	// store the account storage
	if len(sm) > 0 {
		r, err = s.SetContractStorage(addr.String(), sm, nil)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

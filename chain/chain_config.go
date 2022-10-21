/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package chain

import (
	"math/big"
)

// Config is the core config which determines the blockchain settings.
//
// Config is stored in the database on a per block basis. This means
// that any network, identified by its genesis block, can have its own
// set of configuration options.
type Config struct {
	MuirGlacierBlock *big.Int `json:"muirGlacierBlock,omitempty"` // EIP-2384 (bomb delay) switch block (nil = no fork, 0 = already activated)
	EIP155Block      *big.Int `json:"eip155Block,omitempty"`      // EIP155 HF block
	HomesteadBlock   *big.Int `json:"homesteadBlock,omitempty"`   // Homestead switch block (nil = no fork, 0 = already homestead)
	DAOForkBlock     *big.Int `json:"daoForkBlock,omitempty"`     // TheDAO hard-fork switch block (nil = no fork)
	ByzantiumBlock   *big.Int `json:"byzantiumBlock,omitempty"`   // Byzantium switch block (nil = no fork, 0 = already on byzantium)
	// EIP150 implements the Gas price changes (https://github.com/ethereum/EIPs/issues/150)
	EIP150Block         *big.Int `json:"eip150Block,omitempty"`         // EIP150 HF block (nil = no fork)
	ChainID             *big.Int `json:"chainId"`                       // chainId identifies the current chain and is used for replay protection
	EIP158Block         *big.Int `json:"eip158Block,omitempty"`         // EIP158 HF block
	ArrowGlacierBlock   *big.Int `json:"arrowGlacierBlock,omitempty"`   // EIP-4345 (bomb delay) switch block (nil = no fork, 0 = already activated)
	ConstantinopleBlock *big.Int `json:"constantinopleBlock,omitempty"` // Constantinople switch block (nil = no fork, 0 = already activated)
	PetersburgBlock     *big.Int `json:"petersburgBlock,omitempty"`     // Petersburg switch block (nil = same as Constantinople)
	IstanbulBlock       *big.Int `json:"istanbulBlock,omitempty"`       // Istanbul switch block (nil = no fork, 0 = already on istanbul)
	LondonBlock         *big.Int `json:"londonBlock,omitempty"`         // London switch block (nil = no fork, 0 = already on london)
	BerlinBlock         *big.Int `json:"berlinBlock,omitempty"`         // Berlin switch block (nil = no fork, 0 = already on berlin)
	ChainName           string
	DAOForkSupport      bool `json:"daoForkSupport,omitempty"` // Whether the nodes supports or opposes the DAO hard-fork
}

// Rules wraps Config and is merely syntactic sugar or can be used for functions
// that do not have or require information about the block.
//
// Rules is a one time interface meaning that it shouldn't be used in between transition
// phases.
type Rules struct {
	IsHomestead, IsEIP150, IsEIP155, IsEIP158               bool
	IsByzantium, IsConstantinople, IsPetersburg, IsIstanbul bool
	IsBerlin, IsLondon                                      bool
}

func NewRules(c *Config, num uint64) Rules {
	return Rules{
		IsHomestead:      isForked(c.HomesteadBlock, num),
		IsEIP150:         isForked(c.EIP150Block, num),
		IsEIP155:         isForked(c.EIP155Block, num),
		IsEIP158:         isForked(c.EIP158Block, num),
		IsByzantium:      isForked(c.ByzantiumBlock, num),
		IsConstantinople: isForked(c.ConstantinopleBlock, num),
		IsPetersburg:     isForked(c.PetersburgBlock, num),
		IsIstanbul:       isForked(c.IstanbulBlock, num),
		IsBerlin:         isForked(c.BerlinBlock, num),
		IsLondon:         isForked(c.LondonBlock, num),
	}
}

func (r Rules) Changed(c *Config, num uint64) bool {
	return r.IsLondon != isForked(c.LondonBlock, num) ||
		r.IsBerlin != isForked(c.BerlinBlock, num) ||
		r.IsIstanbul != isForked(c.IstanbulBlock, num) ||
		r.IsPetersburg != isForked(c.PetersburgBlock, num) ||
		r.IsConstantinople != isForked(c.ConstantinopleBlock, num) ||
		r.IsByzantium != isForked(c.ByzantiumBlock, num) ||
		r.IsEIP158 != isForked(c.EIP158Block, num) ||
		r.IsEIP155 != isForked(c.EIP155Block, num) ||
		r.IsEIP150 != isForked(c.EIP150Block, num) ||
		r.IsHomestead != isForked(c.HomesteadBlock, num)
}

// isForked returns whether a fork scheduled at block s is active at the given head block.
func isForked(s *big.Int, head uint64) bool { return s != nil && s.Uint64() <= head }

var (
	MainnetRules = Rules{
		IsHomestead:      true,
		IsEIP150:         true,
		IsEIP155:         true,
		IsEIP158:         true,
		IsByzantium:      true,
		IsConstantinople: true,
		IsPetersburg:     true,
		IsIstanbul:       true,
		IsBerlin:         true,
		IsLondon:         true,
	}
	CliqueRules = Rules{
		IsHomestead:      true,
		IsEIP150:         true,
		IsEIP155:         true,
		IsEIP158:         true,
		IsByzantium:      true,
		IsConstantinople: true,
		IsPetersburg:     true,
		IsIstanbul:       true,
		IsBerlin:         true,
		IsLondon:         false,
	}
	DevRules = CliqueRules
)

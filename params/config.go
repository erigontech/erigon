// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package params

import (
	"embed"
	"encoding/json"
	"fmt"
	"math/big"
	"path"
	"sort"
	"strconv"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/params/networkname"
)

//go:embed chainspecs
var chainspecs embed.FS

func readChainSpec(filename string) *ChainConfig {
	f, err := chainspecs.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Could not open chainspec for %s: %v", filename, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	spec := &ChainConfig{}
	err = decoder.Decode(&spec)
	if err != nil {
		panic(fmt.Sprintf("Could not parse chainspec for %s: %v", filename, err))
	}
	return spec
}

type ConsensusType string

const (
	AuRaConsensus   ConsensusType = "aura"
	EtHashConsensus ConsensusType = "ethash"
	CliqueConsensus ConsensusType = "clique"
	ParliaConsensus ConsensusType = "parlia"
	BorConsensus    ConsensusType = "bor"
)

// Genesis hashes to enforce below configs on.
var (
	MainnetGenesisHash    = common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
	SepoliaGenesisHash    = common.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9")
	RopstenGenesisHash    = common.HexToHash("0x41941023680923e0fe4d74a34bdac8141f2540e3ae90623718e47d66d1ca4a2d")
	RinkebyGenesisHash    = common.HexToHash("0x6341fd3daf94b748c72ced5a5b26028f2474f5f00d824504e4fa37a75767e177")
	GoerliGenesisHash     = common.HexToHash("0xbf7e331f7f7c1dd2e05159666b3bf8bc7a8a3a9eb1d518969eab529dd9b88c1a")
	SokolGenesisHash      = common.HexToHash("0x5b28c1bfd3a15230c9a46b399cd0f9a6920d432e85381cc6a140b06e8410112f")
	FermionGenesisHash    = common.HexToHash("0x0658360d8680ead416900a552b67b84e6d575c7f0ecab3dbe42406f9f8c34c35")
	BSCGenesisHash        = common.HexToHash("0x0d21840abff46b96c84b2ac9e10e4f5cdaeb5693cb665db62a2f3b02d2d57b5b")
	ChapelGenesisHash     = common.HexToHash("0x6d3c66c5357ec91d5c43af47e234a939b22557cbb552dc45bebbceeed90fbe34")
	RialtoGenesisHash     = common.HexToHash("0xee835a629f9cf5510b48b6ba41d69e0ff7d6ef10f977166ef939db41f59f5501")
	MumbaiGenesisHash     = common.HexToHash("0x7b66506a9ebdbf30d32b43c5f15a3b1216269a1ec3a75aa3182b86176a2b1ca7")
	BorMainnetGenesisHash = common.HexToHash("0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b")
	BorDevnetGenesisHash  = common.HexToHash("0x5a06b25b0c6530708ea0b98a3409290e39dce6be7f558493aeb6e4b99a172a87")
	GnosisGenesisHash     = common.HexToHash("0x4f1dd23188aab3a76b463e4af801b52b1248ef073c648cbdc4c9333d3da79756")
	ChiadoGenesisHash     = common.HexToHash("0xada44fd8d2ecab8b08f256af07ad3e777f17fb434f8f8e678b312f576212ba9a")
	ShandongGenesisHash   = common.HexToHash("0x314dd74a67fa1dbd85498d42871b1bb6a5bb23a9dbcc07848311b8e5a71cab38") // Source: https://explorer.shandong.ethdevops.io/block/0/transactions
)

var (
	SokolGenesisEpochProof = common.FromHex("0xf91a8c80b91a87f91a84f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f91871b914c26060604052600436106100fc576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff16806303aca79214610101578063108552691461016457806340a141ff1461019d57806340c9cdeb146101d65780634110a489146101ff57806345199e0a1461025757806349285b58146102c15780634d238c8e14610316578063752862111461034f578063900eb5a8146103645780639a573786146103c7578063a26a47d21461041c578063ae4b1b5b14610449578063b3f05b971461049e578063b7ab4db5146104cb578063d3e848f114610535578063fa81b2001461058a578063facd743b146105df575b600080fd5b341561010c57600080fd5b6101226004808035906020019091905050610630565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561016f57600080fd5b61019b600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190505061066f565b005b34156101a857600080fd5b6101d4600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610807565b005b34156101e157600080fd5b6101e9610bb7565b6040518082815260200191505060405180910390f35b341561020a57600080fd5b610236600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610bbd565b60405180831515151581526020018281526020019250505060405180910390f35b341561026257600080fd5b61026a610bee565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156102ad578082015181840152602081019050610292565b505050509050019250505060405180910390f35b34156102cc57600080fd5b6102d4610c82565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561032157600080fd5b61034d600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610d32565b005b341561035a57600080fd5b610362610fcc565b005b341561036f57600080fd5b61038560048080359060200190919050506110fc565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156103d257600080fd5b6103da61113b565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561042757600080fd5b61042f6111eb565b604051808215151515815260200191505060405180910390f35b341561045457600080fd5b61045c6111fe565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156104a957600080fd5b6104b1611224565b604051808215151515815260200191505060405180910390f35b34156104d657600080fd5b6104de611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b83811015610521578082015181840152602081019050610506565b505050509050019250505060405180910390f35b341561054057600080fd5b6105486112cb565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561059557600080fd5b61059d6112f1565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156105ea57600080fd5b610616600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050611317565b604051808215151515815260200191505060405180910390f35b60078181548110151561063f57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156106cb57600080fd5b600460019054906101000a900460ff161515156106e757600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff161415151561072357600080fd5b80600a60006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055506001600460016101000a81548160ff0219169083151502179055507f600bcf04a13e752d1e3670a5a9f1c21177ca2a93c6f5391d4f1298d098097c22600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a150565b600080600061081461113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff1614151561084d57600080fd5b83600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff1615156108a957600080fd5b600960008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101549350600160078054905003925060078381548110151561090857fe5b906000526020600020900160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1691508160078581548110151561094657fe5b906000526020600020900160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555083600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506007838154811015156109e557fe5b906000526020600020900160006101000a81549073ffffffffffffffffffffffffffffffffffffffff02191690556000600780549050111515610a2757600080fd5b6007805480919060019003610a3c9190611370565b506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160006101000a81548160ff0219169083151502179055506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610ba257602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610b58575b50509250505060405180910390a25050505050565b60085481565b60096020528060005260406000206000915090508060000160009054906101000a900460ff16908060010154905082565b610bf661139c565b6007805480602002602001604051908101604052809291908181526020018280548015610c7857602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610c2e575b5050505050905090565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166349285b586000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b1515610d1257600080fd5b6102c65a03f11515610d2357600080fd5b50505060405180519050905090565b610d3a61113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141515610d7357600080fd5b80600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff16151515610dd057600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168273ffffffffffffffffffffffffffffffffffffffff1614151515610e0c57600080fd5b6040805190810160405280600115158152602001600780549050815250600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008201518160000160006101000a81548160ff0219169083151502179055506020820151816001015590505060078054806001018281610ea991906113b0565b9160005260206000209001600084909190916101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff160217905550506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610fba57602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610f70575b50509250505060405180910390a25050565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480156110365750600460009054906101000a900460ff16155b151561104157600080fd5b6001600460006101000a81548160ff0219169083151502179055506007600690805461106e9291906113dc565b506006805490506008819055507f8564cd629b15f47dc310d45bcbfc9bcf5420b0d51bf0659a16c67f91d27632536110a4611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156110e75780820151818401526020810190506110cc565b505050509050019250505060405180910390a1565b60068181548110151561110b57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16639a5737866000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b15156111cb57600080fd5b6102c65a03f115156111dc57600080fd5b50505060405180519050905090565b600460019054906101000a900460ff1681565b600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460009054906101000a900460ff1681565b61123f61139c565b60068054806020026020016040519081016040528092919081815260200182805480156112c157602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311611277575b5050505050905090565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600960008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff169050919050565b81548183558181151161139757818360005260206000209182019101611396919061142e565b5b505050565b602060405190810160405280600081525090565b8154818355818115116113d7578183600052602060002091820191016113d6919061142e565b5b505050565b82805482825590600052602060002090810192821561141d5760005260206000209182015b8281111561141c578254825591600101919060010190611401565b5b50905061142a9190611453565b5090565b61145091905b8082111561144c576000816000905550600101611434565b5090565b90565b61149391905b8082111561148f57600081816101000a81549073ffffffffffffffffffffffffffffffffffffffff021916905550600101611459565b5090565b905600a165627a7a7230582036ea35935c8246b68074adece2eab70c40e69a0193c08a6277ce06e5b25188510029b86bf869a033aa5d69545785694b808840be50c182dad2ec3636dfccbe6572fb69828742c0b846f8440101a0663ce0d171e545a26aa67e4ca66f72ba96bb48287dbcc03beea282867f80d44ba01f0e7726926cb43c03a0abf48197dba78522ec8ba1b158e2aa30da7d2a2c6f9eb8f3f8f1a08023c0d95fc2364e0bf7593f5ff32e1db8ef9f4b41c0bd474eae62d1af896e99808080a0b47b4f0b3e73b5edc8f9a9da1cbcfed562eb06bf54619b6aefeadebf5b3604c280a0da6ec08940a924cb08c947dd56cdb40076b29a6f0ea4dba4e2d02d9a9a72431b80a030cc4138c9e74b6cf79d624b4b5612c0fd888e91f55316cfee7d1694e1a90c0b80a0c5d54b915b56a888eee4e6eeb3141e778f9b674d1d322962eed900f02c29990aa017256b36ef47f907c6b1378a2636942ce894c17075e56fc054d4283f6846659e808080a03340bbaeafcda3a8672eb83099231dbbfab8dae02a1e8ec2f7180538fac207e080b838f7a03868bdfa8727775661e4ccf117824a175a33f8703d728c04488fbfffcafda9f99594e8ddc5c7a2d2f0d7a9798459c0104fdf5e987acab853f851808080a07bb75cabebdcbd1dbb4331054636d0c6d7a2b08483b9e04df057395a7434c9e080808080808080a0e61e567237b49c44d8f906ceea49027260b4010c10a547b38d8b131b9d3b6f848080808080b853f851808080a0a87d9bb950836582673aa0eecc0ff64aac607870637a2dd2012b8b1b31981f698080a08da6d5c36a404670c553a2c9052df7cd604f04e3863c4c7b9e0027bfd54206d680808080808080808080b86bf869a02080c7b7ae81a58eb98d9c78de4a1fd7fd9535fc953ed2be602daaa41767312ab846f8448080a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470b8d3f8d1a0dc277c93a9f9dcee99aac9b8ba3cfa4c51821998522469c37715644e8fbac0bfa0ab8cdb808c8303bb61fb48e276217be9770fa83ecf3f90f2234d558885f5abf1808080a0fe137c3a474fbde41d89a59dd76da4c55bf696b86d3af64a55632f76cf30786780808080a06301b39b2ea8a44df8b0356120db64b788e71f52e1d7a6309d0d2e5b86fee7cb80a0da5d8b08dea0c5a4799c0f44d8a24d7cdf209f9b7a5588c1ecafb5361f6b9f07a01b7779e149cadf24d4ffb77ca7e11314b8db7097e4d70b2a173493153ca2e5a0808080a3e2a02052222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f0180")
)

var (
	SokolGenesisStateRoot    = common.HexToHash("0xfad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3")
	FermionGenesisStateRoot  = common.HexToHash("0x08982dc16236c51b6d9aff8b76cd0faa7067eb55eba62395d5a82649d8fb73c4")
	GnosisGenesisStateRoot   = common.HexToHash("0x40cf4430ecaa733787d1a65154a3b9efb560c95d9e324a23b97f0609b539133b")
	ChiadoGenesisStateRoot   = common.HexToHash("0x9ec3eaf4e6188dfbdd6ade76eaa88289b57c63c9a2cde8d35291d5a29e143d31")
	ShandongGenesisStateRoot = common.HexToHash("0xe794e45a596856bcd5412788f46752a559a4aa89fe556ab26a8c2cf0fc24cb5e")
)

var (
	// MainnetChainConfig is the chain parameters to run a node on the main network.
	MainnetChainConfig = readChainSpec("chainspecs/mainnet.json")

	// SepoliaChainConfig contains the chain parameters to run a node on the Sepolia test network.
	SepoliaChainConfig = readChainSpec("chainspecs/sepolia.json")

	// RopstenChainConfig contains the chain parameters to run a node on the Ropsten test network.
	RopstenChainConfig = readChainSpec("chainspecs/ropsten.json")

	// RinkebyChainConfig contains the chain parameters to run a node on the Rinkeby test network.
	RinkebyChainConfig = readChainSpec("chainspecs/rinkeby.json")

	// GoerliChainConfig contains the chain parameters to run a node on the Görli test network.
	GoerliChainConfig = readChainSpec("chainspecs/goerli.json")

	BSCChainConfig = readChainSpec("chainspecs/bsc.json")

	ChapelChainConfig = readChainSpec("chainspecs/chapel.json")

	RialtoChainConfig = readChainSpec("chainspecs/rialto.json")

	SokolChainConfig = readChainSpec("chainspecs/sokol.json")

	FermionChainConfig = readChainSpec("chainspecs/fermion.json")

	ShandongChainConfig = readChainSpec("chainspecs/shandong.json")

	// AllProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the main net protocol.
	AllProtocolChanges = &ChainConfig{
		ChainID:                       big.NewInt(1337),
		Consensus:                     EtHashConsensus,
		HomesteadBlock:                big.NewInt(0),
		TangerineWhistleBlock:         big.NewInt(0),
		TangerineWhistleHash:          common.Hash{},
		SpuriousDragonBlock:           big.NewInt(0),
		ByzantiumBlock:                big.NewInt(0),
		ConstantinopleBlock:           big.NewInt(0),
		PetersburgBlock:               big.NewInt(0),
		IstanbulBlock:                 big.NewInt(0),
		MuirGlacierBlock:              big.NewInt(0),
		BerlinBlock:                   big.NewInt(0),
		LondonBlock:                   big.NewInt(0),
		ArrowGlacierBlock:             big.NewInt(0),
		GrayGlacierBlock:              big.NewInt(0),
		TerminalTotalDifficulty:       big.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiBlock:                 big.NewInt(0),
		Ethash:                        new(EthashConfig),
	}

	// AllCliqueProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the Clique consensus.
	AllCliqueProtocolChanges = &ChainConfig{
		ChainID:               big.NewInt(1337),
		Consensus:             CliqueConsensus,
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		TangerineWhistleHash:  common.Hash{},
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		Clique:                &CliqueConfig{Period: 0, Epoch: 30000},
	}

	MumbaiChainConfig = readChainSpec("chainspecs/mumbai.json")

	BorMainnetChainConfig = readChainSpec("chainspecs/bor-mainnet.json")

	BorDevnetChainConfig = readChainSpec("chainspecs/bor-devnet.json")

	GnosisChainConfig = readChainSpec("chainspecs/gnosis.json")

	ChiadoChainConfig = readChainSpec("chainspecs/chiado.json")

	CliqueSnapshot = NewSnapshotConfig(10, 1024, 16384, true, "")

	TestChainConfig = &ChainConfig{
		ChainID:               big.NewInt(1337),
		Consensus:             EtHashConsensus,
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		TangerineWhistleHash:  common.Hash{},
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		Ethash:                new(EthashConfig),
	}

	TestChainAuraConfig = &ChainConfig{
		ChainID:               big.NewInt(1),
		Consensus:             AuRaConsensus,
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		TangerineWhistleHash:  common.Hash{},
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		Aura:                  &AuRaConfig{},
	}

	TestRules = TestChainConfig.Rules(0)
)

// ChainConfig is the core config which determines the blockchain settings.
//
// ChainConfig is stored in the database on a per block basis. This means
// that any network, identified by its genesis block, can have its own
// set of configuration options.
type ChainConfig struct {
	ChainName string
	ChainID   *big.Int `json:"chainId"` // chainId identifies the current chain and is used for replay protection

	Consensus ConsensusType `json:"consensus,omitempty"` // aura, ethash or clique

	HomesteadBlock *big.Int `json:"homesteadBlock,omitempty"` // Homestead switch block (nil = no fork, 0 = already homestead)

	DAOForkBlock   *big.Int `json:"daoForkBlock,omitempty"`   // TheDAO hard-fork switch block (nil = no fork)
	DAOForkSupport bool     `json:"daoForkSupport,omitempty"` // Whether the nodes supports or opposes the DAO hard-fork

	// Tangerine Whistle (EIP150) implements the Gas price changes (https://github.com/ethereum/EIPs/issues/150)
	TangerineWhistleBlock *big.Int    `json:"eip150Block,omitempty"` // EIP150 HF block (nil = no fork)
	TangerineWhistleHash  common.Hash `json:"eip150Hash,omitempty"`  // EIP150 HF hash (needed for header only clients as only gas pricing changed)

	SpuriousDragonBlock *big.Int `json:"eip155Block,omitempty"` // Spurious Dragon HF block

	ByzantiumBlock      *big.Int `json:"byzantiumBlock,omitempty"`      // Byzantium switch block (nil = no fork, 0 = already on byzantium)
	ConstantinopleBlock *big.Int `json:"constantinopleBlock,omitempty"` // Constantinople switch block (nil = no fork, 0 = already activated)
	PetersburgBlock     *big.Int `json:"petersburgBlock,omitempty"`     // Petersburg switch block (nil = same as Constantinople)
	IstanbulBlock       *big.Int `json:"istanbulBlock,omitempty"`       // Istanbul switch block (nil = no fork, 0 = already on istanbul)
	MuirGlacierBlock    *big.Int `json:"muirGlacierBlock,omitempty"`    // EIP-2384 (bomb delay) switch block (nil = no fork, 0 = already activated)
	BerlinBlock         *big.Int `json:"berlinBlock,omitempty"`         // Berlin switch block (nil = no fork, 0 = already on berlin)
	LondonBlock         *big.Int `json:"londonBlock,omitempty"`         // London switch block (nil = no fork, 0 = already on london)
	ArrowGlacierBlock   *big.Int `json:"arrowGlacierBlock,omitempty"`   // EIP-4345 (bomb delay) switch block (nil = no fork, 0 = already activated)
	GrayGlacierBlock    *big.Int `json:"grayGlacierBlock,omitempty"`    // EIP-5133 (bomb delay) switch block (nil = no fork, 0 = already activated)

	// EIP-3675: Upgrade consensus to Proof-of-Stake
	TerminalTotalDifficulty       *big.Int `json:"terminalTotalDifficulty,omitempty"`       // The merge happens when terminal total difficulty is reached
	TerminalTotalDifficultyPassed bool     `json:"terminalTotalDifficultyPassed,omitempty"` // Disable PoW sync for networks that have already passed through the Merge
	MergeNetsplitBlock            *big.Int `json:"mergeNetsplitBlock,omitempty"`            // Virtual fork after The Merge to use as a network splitter; see FORK_NEXT_VALUE in EIP-3675

	ShanghaiBlock *big.Int `json:"shanghaiBlock,omitempty"` // Shanghai switch block (nil = no fork, 0 = already activated)
	CancunBlock   *big.Int `json:"cancunBlock,omitempty"`   // Cancun switch block (nil = no fork, 0 = already activated)

	// Parlia fork blocks
	RamanujanBlock  *big.Int `json:"ramanujanBlock,omitempty" toml:",omitempty"`  // ramanujanBlock switch block (nil = no fork, 0 = already activated)
	NielsBlock      *big.Int `json:"nielsBlock,omitempty" toml:",omitempty"`      // nielsBlock switch block (nil = no fork, 0 = already activated)
	MirrorSyncBlock *big.Int `json:"mirrorSyncBlock,omitempty" toml:",omitempty"` // mirrorSyncBlock switch block (nil = no fork, 0 = already activated)
	BrunoBlock      *big.Int `json:"brunoBlock,omitempty" toml:",omitempty"`      // brunoBlock switch block (nil = no fork, 0 = already activated)
	EulerBlock      *big.Int `json:"eulerBlock,omitempty" toml:",omitempty"`      // eulerBlock switch block (nil = no fork, 0 = already activated)
	GibbsBlock      *big.Int `json:"gibbsBlock,omitempty" toml:",omitempty"`      // gibbsBlock switch block (nil = no fork, 0 = already activated)
	NanoBlock       *big.Int `json:"nanoBlock,omitempty" toml:",omitempty"`       // nanoBlock switch block (nil = no fork, 0 = already activated)
	MoranBlock      *big.Int `json:"moranBlock,omitempty" toml:",omitempty"`      // moranBlock switch block (nil = no fork, 0 = already activated)

	// Gnosis Chain fork blocks
	PosdaoBlock *big.Int `json:"posdaoBlock,omitempty"`

	Eip1559FeeCollector           *common.Address `json:"eip1559FeeCollector,omitempty"`           // (Optional) Address where burnt EIP-1559 fees go to
	Eip1559FeeCollectorTransition *big.Int        `json:"eip1559FeeCollectorTransition,omitempty"` // (Optional) Block from which burnt EIP-1559 fees go to the Eip1559FeeCollector

	// Various consensus engines
	Ethash *EthashConfig `json:"ethash,omitempty"`
	Clique *CliqueConfig `json:"clique,omitempty"`
	Aura   *AuRaConfig   `json:"aura,omitempty"`
	Parlia *ParliaConfig `json:"parlia,omitempty" toml:",omitempty"`
	Bor    *BorConfig    `json:"bor,omitempty"`
}

// EthashConfig is the consensus engine configs for proof-of-work based sealing.
type EthashConfig struct{}

// String implements the stringer interface, returning the consensus engine details.
func (c *EthashConfig) String() string {
	return "ethash"
}

// CliqueConfig is the consensus engine configs for proof-of-authority based sealing.
type CliqueConfig struct {
	Period uint64 `json:"period"` // Number of seconds between blocks to enforce
	Epoch  uint64 `json:"epoch"`  // Epoch length to reset votes and checkpoint
}

// String implements the stringer interface, returning the consensus engine details.
func (c *CliqueConfig) String() string {
	return "clique"
}

// AuRaConfig is the consensus engine configs for proof-of-authority based sealing.
type AuRaConfig struct {
	DBPath    string
	InMemory  bool
	Etherbase common.Address // same as miner etherbase
}

// String implements the stringer interface, returning the consensus engine details.
func (c *AuRaConfig) String() string {
	return "aura"
}

type ParliaConfig struct {
	DBPath   string
	InMemory bool
	Period   uint64 `json:"period"` // Number of seconds between blocks to enforce
	Epoch    uint64 `json:"epoch"`  // Epoch length to update validatorSet
}

// String implements the stringer interface, returning the consensus engine details.
func (b *ParliaConfig) String() string {
	return "parlia"
}

// BorConfig is the consensus engine configs for Matic bor based sealing.
type BorConfig struct {
	Period                map[string]uint64 `json:"period"`                // Number of seconds between blocks to enforce
	ProducerDelay         map[string]uint64 `json:"producerDelay"`         // Number of seconds delay between two producer interval
	Sprint                map[string]uint64 `json:"sprint"`                // Epoch length to proposer
	BackupMultiplier      map[string]uint64 `json:"backupMultiplier"`      // Backup multiplier to determine the wiggle time
	ValidatorContract     string            `json:"validatorContract"`     // Validator set contract
	StateReceiverContract string            `json:"stateReceiverContract"` // State receiver contract

	OverrideStateSyncRecords map[string]int         `json:"overrideStateSyncRecords"` // override state records count
	BlockAlloc               map[string]interface{} `json:"blockAlloc"`

	JaipurBlock *big.Int `json:"jaipurBlock"` // Jaipur switch block (nil = no fork, 0 = already on jaipur)
	DelhiBlock  *big.Int `json:"delhiBlock"`  // Delhi switch block (nil = no fork, 0 = already on delhi)
}

// String implements the stringer interface, returning the consensus engine details.
func (b *BorConfig) String() string {
	return "bor"
}

func (c *BorConfig) CalculateProducerDelay(number uint64) uint64 {
	return c.calculateSprintSizeHelper(c.ProducerDelay, number)
}

func (c *BorConfig) CalculateSprint(number uint64) uint64 {
	return c.calculateSprintSizeHelper(c.Sprint, number)
}

func (c *BorConfig) CalculateBackupMultiplier(number uint64) uint64 {
	return c.calculateBorConfigHelper(c.BackupMultiplier, number)
}

func (c *BorConfig) CalculatePeriod(number uint64) uint64 {
	return c.calculateBorConfigHelper(c.Period, number)
}

func (c *BorConfig) IsJaipur(number uint64) bool {
	return isForked(c.JaipurBlock, number)
}

func (c *BorConfig) IsDelhi(number uint64) bool {
	return isForked(c.DelhiBlock, number)
}

func (c *BorConfig) calculateBorConfigHelper(field map[string]uint64, number uint64) uint64 {
	keys := make([]string, 0, len(field))
	for k := range field {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i := 0; i < len(keys)-1; i++ {
		valUint, _ := strconv.ParseUint(keys[i], 10, 64)
		valUintNext, _ := strconv.ParseUint(keys[i+1], 10, 64)
		if number > valUint && number < valUintNext {
			return field[keys[i]]
		}
	}
	return field[keys[len(keys)-1]]
}

func (c *BorConfig) calculateSprintSizeHelper(field map[string]uint64, number uint64) uint64 {
	keys := make([]string, 0, len(field))
	for k := range field {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for i := 0; i < len(keys)-1; i++ {
		valUint, _ := strconv.ParseUint(keys[i], 10, 64)
		valUintNext, _ := strconv.ParseUint(keys[i+1], 10, 64)

		if number >= valUint && number < valUintNext {
			return field[keys[i]]
		}
	}

	return field[keys[len(keys)-1]]
}

// String implements the fmt.Stringer interface.
func (c *ChainConfig) String() string {
	var engine interface{}
	switch {
	case c.Ethash != nil:
		engine = c.Ethash
	case c.Clique != nil:
		engine = c.Clique
	case c.Parlia != nil:
		engine = c.Parlia
	case c.Bor != nil:
		engine = c.Bor
	case c.Aura != nil:
		engine = c.Aura
	default:
		engine = "unknown"
	}

	// TODO Covalent: Refactor to more generic approach and potentially introduce tag for "ecosystem" field (Ethereum, BSC, etc.)
	if c.Consensus == ParliaConsensus {
		return fmt.Sprintf("{ChainID: %v Ramanujan: %v, Niels: %v, MirrorSync: %v, Bruno: %v, Euler: %v, Gibbs: %v, Nano: %v, Moran: %v, Engine: %v}",
			c.ChainID,
			c.RamanujanBlock,
			c.NielsBlock,
			c.MirrorSyncBlock,
			c.BrunoBlock,
			c.EulerBlock,
			c.GibbsBlock,
			c.NanoBlock,
			c.MoranBlock,
			engine,
		)
	}

	return fmt.Sprintf("{ChainID: %v, Homestead: %v, DAO: %v, DAO Support: %v, Tangerine Whistle: %v, Spurious Dragon: %v, Byzantium: %v, Constantinople: %v, Petersburg: %v, Istanbul: %v, Muir Glacier: %v, Berlin: %v, London: %v, Arrow Glacier: %v, Gray Glacier: %v, Terminal Total Difficulty: %v, Merge Netsplit: %v, Shanghai: %v, Cancun: %v, Engine: %v}",
		c.ChainID,
		c.HomesteadBlock,
		c.DAOForkBlock,
		c.DAOForkSupport,
		c.TangerineWhistleBlock,
		c.SpuriousDragonBlock,
		c.ByzantiumBlock,
		c.ConstantinopleBlock,
		c.PetersburgBlock,
		c.IstanbulBlock,
		c.MuirGlacierBlock,
		c.BerlinBlock,
		c.LondonBlock,
		c.ArrowGlacierBlock,
		c.GrayGlacierBlock,
		c.TerminalTotalDifficulty,
		c.MergeNetsplitBlock,
		c.ShanghaiBlock,
		c.CancunBlock,
		engine,
	)
}

func (c *ChainConfig) IsHeaderWithSeal() bool {
	return c.Consensus == AuRaConsensus
}

type ConsensusSnapshotConfig struct {
	CheckpointInterval uint64 // Number of blocks after which to save the vote snapshot to the database
	InmemorySnapshots  int    // Number of recent vote snapshots to keep in memory
	InmemorySignatures int    // Number of recent block signatures to keep in memory
	DBPath             string
	InMemory           bool
}

const cliquePath = "clique"

func NewSnapshotConfig(checkpointInterval uint64, inmemorySnapshots int, inmemorySignatures int, inmemory bool, dbPath string) *ConsensusSnapshotConfig {
	if len(dbPath) == 0 {
		dbPath = paths.DefaultDataDir()
	}

	return &ConsensusSnapshotConfig{
		checkpointInterval,
		inmemorySnapshots,
		inmemorySignatures,
		path.Join(dbPath, cliquePath),
		inmemory,
	}
}

// IsHomestead returns whether num is either equal to the homestead block or greater.
func (c *ChainConfig) IsHomestead(num uint64) bool {
	return isForked(c.HomesteadBlock, num)
}

// IsDAOFork returns whether num is either equal to the DAO fork block or greater.
func (c *ChainConfig) IsDAOFork(num uint64) bool {
	return isForked(c.DAOForkBlock, num)
}

// IsTangerineWhistle returns whether num is either equal to the Tangerine Whistle (EIP150) fork block or greater.
func (c *ChainConfig) IsTangerineWhistle(num uint64) bool {
	return isForked(c.TangerineWhistleBlock, num)
}

// IsSpuriousDragon returns whether num is either equal to the Spurious Dragon fork block or greater.
func (c *ChainConfig) IsSpuriousDragon(num uint64) bool {
	return isForked(c.SpuriousDragonBlock, num)
}

// IsByzantium returns whether num is either equal to the Byzantium fork block or greater.
func (c *ChainConfig) IsByzantium(num uint64) bool {
	return isForked(c.ByzantiumBlock, num)
}

// IsConstantinople returns whether num is either equal to the Constantinople fork block or greater.
func (c *ChainConfig) IsConstantinople(num uint64) bool {
	return isForked(c.ConstantinopleBlock, num)
}

// IsRamanujan returns whether num is either equal to the IsRamanujan fork block or greater.
func (c *ChainConfig) IsRamanujan(num uint64) bool {
	return isForked(c.RamanujanBlock, num)
}

// IsOnRamanujan returns whether num is equal to the Ramanujan fork block
func (c *ChainConfig) IsOnRamanujan(num *big.Int) bool {
	return configNumEqual(c.RamanujanBlock, num)
}

// IsNiels returns whether num is either equal to the Niels fork block or greater.
func (c *ChainConfig) IsNiels(num uint64) bool {
	return isForked(c.NielsBlock, num)
}

// IsOnNiels returns whether num is equal to the IsNiels fork block
func (c *ChainConfig) IsOnNiels(num *big.Int) bool {
	return configNumEqual(c.NielsBlock, num)
}

// IsMirrorSync returns whether num is either equal to the MirrorSync fork block or greater.
func (c *ChainConfig) IsMirrorSync(num uint64) bool {
	return isForked(c.MirrorSyncBlock, num)
}

// IsOnMirrorSync returns whether num is equal to the MirrorSync fork block
func (c *ChainConfig) IsOnMirrorSync(num *big.Int) bool {
	return configNumEqual(c.MirrorSyncBlock, num)
}

// IsBruno returns whether num is either equal to the Burn fork block or greater.
func (c *ChainConfig) IsBruno(num uint64) bool {
	return isForked(c.BrunoBlock, num)
}

// IsOnBruno returns whether num is equal to the Burn fork block
func (c *ChainConfig) IsOnBruno(num *big.Int) bool {
	return configNumEqual(c.BrunoBlock, num)
}

// IsEuler returns whether num is either equal to the euler fork block or greater.
func (c *ChainConfig) IsEuler(num *big.Int) bool {
	return isForked(c.EulerBlock, num.Uint64())
}

func (c *ChainConfig) IsOnEuler(num *big.Int) bool {
	return configNumEqual(c.EulerBlock, num)
}

// IsGibbs returns whether num is either equal to the euler fork block or greater.
func (c *ChainConfig) IsGibbs(num *big.Int) bool {
	return isForked(c.GibbsBlock, num.Uint64())
}

func (c *ChainConfig) IsOnGibbs(num *big.Int) bool {
	return configNumEqual(c.GibbsBlock, num)
}

func (c *ChainConfig) IsMoran(num uint64) bool {
	return isForked(c.MoranBlock, num)
}

func (c *ChainConfig) IsOnMoran(num *big.Int) bool {
	return configNumEqual(c.MoranBlock, num)
}

// IsNano returns whether num is either equal to the euler fork block or greater.
func (c *ChainConfig) IsNano(num uint64) bool {
	return isForked(c.NanoBlock, num)
}

func (c *ChainConfig) IsOnNano(num *big.Int) bool {
	return configNumEqual(c.NanoBlock, num)
}

// IsMuirGlacier returns whether num is either equal to the Muir Glacier (EIP-2384) fork block or greater.
func (c *ChainConfig) IsMuirGlacier(num uint64) bool {
	return isForked(c.MuirGlacierBlock, num)
}

// IsPetersburg returns whether num is either
// - equal to or greater than the PetersburgBlock fork block,
// - OR is nil, and Constantinople is active
func (c *ChainConfig) IsPetersburg(num uint64) bool {
	return isForked(c.PetersburgBlock, num) || c.PetersburgBlock == nil && isForked(c.ConstantinopleBlock, num)
}

// IsIstanbul returns whether num is either equal to the Istanbul fork block or greater.
func (c *ChainConfig) IsIstanbul(num uint64) bool {
	return isForked(c.IstanbulBlock, num)
}

// IsBerlin returns whether num is either equal to the Berlin fork block or greater.
func (c *ChainConfig) IsBerlin(num uint64) bool {
	return isForked(c.BerlinBlock, num)
}

// IsLondon returns whether num is either equal to the London fork block or greater.
func (c *ChainConfig) IsLondon(num uint64) bool {
	return isForked(c.LondonBlock, num)
}

// IsArrowGlacier returns whether num is either equal to the Arrow Glacier (EIP-4345) fork block or greater.
func (c *ChainConfig) IsArrowGlacier(num uint64) bool {
	return isForked(c.ArrowGlacierBlock, num)
}

// IsGrayGlacier returns whether num is either equal to the Gray Glacier (EIP-5133) fork block or greater.
func (c *ChainConfig) IsGrayGlacier(num uint64) bool {
	return isForked(c.GrayGlacierBlock, num)
}

// IsShanghai returns whether num is either equal to the Shanghai fork block or greater.
func (c *ChainConfig) IsShanghai(num uint64) bool {
	return isForked(c.ShanghaiBlock, num)
}

// IsCancun returns whether num is either equal to the Cancun fork block or greater.
func (c *ChainConfig) IsCancun(num uint64) bool {
	return isForked(c.CancunBlock, num)
}

func (c *ChainConfig) IsEip1559FeeCollector(num uint64) bool {
	return c.Eip1559FeeCollector != nil && isForked(c.Eip1559FeeCollectorTransition, num)
}

// CheckCompatible checks whether scheduled fork transitions have been imported
// with a mismatching chain configuration.
func (c *ChainConfig) CheckCompatible(newcfg *ChainConfig, height uint64) *ConfigCompatError {
	bhead := height

	// Iterate checkCompatible to find the lowest conflict.
	var lasterr *ConfigCompatError
	for {
		err := c.checkCompatible(newcfg, bhead)
		if err == nil || (lasterr != nil && err.RewindTo == lasterr.RewindTo) {
			break
		}
		lasterr = err
		bhead = err.RewindTo
	}
	return lasterr
}

// CheckConfigForkOrder checks that we don't "skip" any forks, geth isn't pluggable enough
// to guarantee that forks can be implemented in a different order than on official networks
func (c *ChainConfig) CheckConfigForkOrder() error {
	if c != nil && c.ChainID != nil && c.ChainID.Uint64() == 77 {
		return nil
	}
	type fork struct {
		name     string
		block    *big.Int
		optional bool // if true, the fork may be nil and next fork is still allowed
	}
	var lastFork fork
	for _, cur := range []fork{
		{name: "homesteadBlock", block: c.HomesteadBlock},
		{name: "daoForkBlock", block: c.DAOForkBlock, optional: true},
		{name: "eip150Block", block: c.TangerineWhistleBlock},
		{name: "eip155Block", block: c.SpuriousDragonBlock},
		{name: "byzantiumBlock", block: c.ByzantiumBlock},
		{name: "constantinopleBlock", block: c.ConstantinopleBlock},
		{name: "petersburgBlock", block: c.PetersburgBlock},
		{name: "istanbulBlock", block: c.IstanbulBlock},
		{name: "muirGlacierBlock", block: c.MuirGlacierBlock, optional: true},
		{name: "eulerBlock", block: c.EulerBlock, optional: true},
		{name: "gibbsBlock", block: c.GibbsBlock, optional: true},
		{name: "berlinBlock", block: c.BerlinBlock},
		{name: "londonBlock", block: c.LondonBlock},
		{name: "arrowGlacierBlock", block: c.ArrowGlacierBlock, optional: true},
		{name: "grayGlacierBlock", block: c.GrayGlacierBlock, optional: true},
		{name: "mergeNetsplitBlock", block: c.MergeNetsplitBlock, optional: true},
		{name: "shanghaiBlock", block: c.ShanghaiBlock},
		{name: "cancunBlock", block: c.CancunBlock},
	} {
		if lastFork.name != "" {
			// Next one must be higher number
			if lastFork.block == nil && cur.block != nil {
				return fmt.Errorf("unsupported fork ordering: %v not enabled, but %v enabled at %v",
					lastFork.name, cur.name, cur.block)
			}
			if lastFork.block != nil && cur.block != nil {
				if lastFork.block.Cmp(cur.block) > 0 {
					return fmt.Errorf("unsupported fork ordering: %v enabled at %v, but %v enabled at %v",
						lastFork.name, lastFork.block, cur.name, cur.block)
				}
			}
			// If it was optional and not set, then ignore it
		}
		if !cur.optional || cur.block != nil {
			lastFork = cur
		}
	}
	return nil
}

func (c *ChainConfig) checkCompatible(newcfg *ChainConfig, head uint64) *ConfigCompatError {
	// Ethereum mainnet forks
	if isForkIncompatible(c.HomesteadBlock, newcfg.HomesteadBlock, head) {
		return newCompatError("Homestead fork block", c.HomesteadBlock, newcfg.HomesteadBlock)
	}
	if isForkIncompatible(c.DAOForkBlock, newcfg.DAOForkBlock, head) {
		return newCompatError("DAO fork block", c.DAOForkBlock, newcfg.DAOForkBlock)
	}
	if c.IsDAOFork(head) && c.DAOForkSupport != newcfg.DAOForkSupport {
		return newCompatError("DAO fork support flag", c.DAOForkBlock, newcfg.DAOForkBlock)
	}
	if isForkIncompatible(c.TangerineWhistleBlock, newcfg.TangerineWhistleBlock, head) {
		return newCompatError("Tangerine Whistle fork block", c.TangerineWhistleBlock, newcfg.TangerineWhistleBlock)
	}
	if isForkIncompatible(c.SpuriousDragonBlock, newcfg.SpuriousDragonBlock, head) {
		return newCompatError("Spurious Dragon fork block", c.SpuriousDragonBlock, newcfg.SpuriousDragonBlock)
	}
	if c.IsSpuriousDragon(head) && !configNumEqual(c.ChainID, newcfg.ChainID) {
		return newCompatError("EIP155 chain ID", c.SpuriousDragonBlock, newcfg.SpuriousDragonBlock)
	}
	if isForkIncompatible(c.ByzantiumBlock, newcfg.ByzantiumBlock, head) {
		return newCompatError("Byzantium fork block", c.ByzantiumBlock, newcfg.ByzantiumBlock)
	}
	if isForkIncompatible(c.ConstantinopleBlock, newcfg.ConstantinopleBlock, head) {
		return newCompatError("Constantinople fork block", c.ConstantinopleBlock, newcfg.ConstantinopleBlock)
	}
	if isForkIncompatible(c.PetersburgBlock, newcfg.PetersburgBlock, head) {
		// the only case where we allow Petersburg to be set in the past is if it is equal to Constantinople
		// mainly to satisfy fork ordering requirements which state that Petersburg fork be set if Constantinople fork is set
		if isForkIncompatible(c.ConstantinopleBlock, newcfg.PetersburgBlock, head) {
			return newCompatError("Petersburg fork block", c.PetersburgBlock, newcfg.PetersburgBlock)
		}
	}
	if isForkIncompatible(c.IstanbulBlock, newcfg.IstanbulBlock, head) {
		return newCompatError("Istanbul fork block", c.IstanbulBlock, newcfg.IstanbulBlock)
	}
	if isForkIncompatible(c.MuirGlacierBlock, newcfg.MuirGlacierBlock, head) {
		return newCompatError("Muir Glacier fork block", c.MuirGlacierBlock, newcfg.MuirGlacierBlock)
	}
	if isForkIncompatible(c.BerlinBlock, newcfg.BerlinBlock, head) {
		return newCompatError("Berlin fork block", c.BerlinBlock, newcfg.BerlinBlock)
	}
	if isForkIncompatible(c.LondonBlock, newcfg.LondonBlock, head) {
		return newCompatError("London fork block", c.LondonBlock, newcfg.LondonBlock)
	}
	if isForkIncompatible(c.ArrowGlacierBlock, newcfg.ArrowGlacierBlock, head) {
		return newCompatError("Arrow Glacier fork block", c.ArrowGlacierBlock, newcfg.ArrowGlacierBlock)
	}
	if isForkIncompatible(c.GrayGlacierBlock, newcfg.GrayGlacierBlock, head) {
		return newCompatError("Gray Glacier fork block", c.GrayGlacierBlock, newcfg.GrayGlacierBlock)
	}
	if isForkIncompatible(c.MergeNetsplitBlock, newcfg.MergeNetsplitBlock, head) {
		return newCompatError("Merge netsplit block", c.MergeNetsplitBlock, newcfg.MergeNetsplitBlock)
	}
	if isForkIncompatible(c.ShanghaiBlock, newcfg.ShanghaiBlock, head) {
		return newCompatError("Shanghai fork block", c.ShanghaiBlock, newcfg.ShanghaiBlock)
	}
	if isForkIncompatible(c.CancunBlock, newcfg.CancunBlock, head) {
		return newCompatError("Cancun fork block", c.CancunBlock, newcfg.CancunBlock)
	}

	// Parlia forks
	if isForkIncompatible(c.RamanujanBlock, newcfg.RamanujanBlock, head) {
		return newCompatError("Ramanujan fork block", c.RamanujanBlock, newcfg.RamanujanBlock)
	}
	if isForkIncompatible(c.NielsBlock, newcfg.NielsBlock, head) {
		return newCompatError("Niels fork block", c.NielsBlock, newcfg.NielsBlock)
	}
	if isForkIncompatible(c.MirrorSyncBlock, newcfg.MirrorSyncBlock, head) {
		return newCompatError("MirrorSync fork block", c.MirrorSyncBlock, newcfg.MirrorSyncBlock)
	}
	if isForkIncompatible(c.BrunoBlock, newcfg.BrunoBlock, head) {
		return newCompatError("Bruno fork block", c.BrunoBlock, newcfg.BrunoBlock)
	}
	if isForkIncompatible(c.EulerBlock, newcfg.EulerBlock, head) {
		return newCompatError("Euler fork block", c.EulerBlock, newcfg.EulerBlock)
	}
	if isForkIncompatible(c.GibbsBlock, newcfg.GibbsBlock, head) {
		return newCompatError("Gibbs fork block", c.GibbsBlock, newcfg.GibbsBlock)
	}
	if isForkIncompatible(c.NanoBlock, newcfg.NanoBlock, head) {
		return newCompatError("Nano fork block", c.NanoBlock, newcfg.NanoBlock)
	}
	if isForkIncompatible(c.MoranBlock, newcfg.MoranBlock, head) {
		return newCompatError("moran fork block", c.MoranBlock, newcfg.MoranBlock)
	}
	return nil
}

// isForkIncompatible returns true if a fork scheduled at s1 cannot be rescheduled to
// block s2 because head is already past the fork.
func isForkIncompatible(s1, s2 *big.Int, head uint64) bool {
	return (isForked(s1, head) || isForked(s2, head)) && !configNumEqual(s1, s2)
}

// isForked returns whether a fork scheduled at block s is active at the given head block.
func isForked(s *big.Int, head uint64) bool {
	if s == nil {
		return false
	}
	return s.Uint64() <= head
}

func configNumEqual(x, y *big.Int) bool {
	if x == nil {
		return y == nil
	}
	if y == nil {
		return x == nil
	}
	return x.Cmp(y) == 0
}

// ConfigCompatError is raised if the locally-stored blockchain is initialised with a
// ChainConfig that would alter the past.
type ConfigCompatError struct {
	What string
	// block numbers of the stored and new configurations
	StoredConfig, NewConfig *big.Int
	// the block number to which the local chain must be rewound to correct the error
	RewindTo uint64
}

func newCompatError(what string, storedblock, newblock *big.Int) *ConfigCompatError {
	var rew *big.Int
	switch {
	case storedblock == nil:
		rew = newblock
	case newblock == nil || storedblock.Cmp(newblock) < 0:
		rew = storedblock
	default:
		rew = newblock
	}
	err := &ConfigCompatError{what, storedblock, newblock, 0}
	if rew != nil && rew.Sign() > 0 {
		err.RewindTo = rew.Uint64() - 1
	}
	return err
}

func (err *ConfigCompatError) Error() string {
	return fmt.Sprintf("mismatching %s in database (have %d, want %d, rewindto %d)", err.What, err.StoredConfig, err.NewConfig, err.RewindTo)
}

// Rules wraps ChainConfig and is merely syntactic sugar or can be used for functions
// that do not have or require information about the block.
//
// Rules is a one time interface meaning that it shouldn't be used in between transition
// phases.
type Rules struct {
	ChainID                                                 *big.Int
	IsHomestead, IsTangerineWhistle, IsSpuriousDragon       bool
	IsByzantium, IsConstantinople, IsPetersburg, IsIstanbul bool
	IsBerlin, IsLondon, IsShanghai, IsCancun                bool
	IsNano, IsMoran                                         bool
	IsEip1559FeeCollector                                   bool
	IsParlia, IsStarknet, IsAura                            bool
}

// Rules ensures c's ChainID is not nil.
func (c *ChainConfig) Rules(num uint64) *Rules {
	chainID := c.ChainID
	if chainID == nil {
		chainID = new(big.Int)
	}
	return &Rules{
		ChainID:               new(big.Int).Set(chainID),
		IsHomestead:           c.IsHomestead(num),
		IsTangerineWhistle:    c.IsTangerineWhistle(num),
		IsSpuriousDragon:      c.IsSpuriousDragon(num),
		IsByzantium:           c.IsByzantium(num),
		IsConstantinople:      c.IsConstantinople(num),
		IsPetersburg:          c.IsPetersburg(num),
		IsIstanbul:            c.IsIstanbul(num),
		IsBerlin:              c.IsBerlin(num),
		IsLondon:              c.IsLondon(num),
		IsShanghai:            c.IsShanghai(num),
		IsCancun:              c.IsCancun(num),
		IsNano:                c.IsNano(num),
		IsMoran:               c.IsMoran(num),
		IsEip1559FeeCollector: c.IsEip1559FeeCollector(num),
		IsParlia:              c.Parlia != nil,
		IsAura:                c.Aura != nil,
	}
}

func ChainConfigByChainName(chain string) *ChainConfig {
	switch chain {
	case networkname.MainnetChainName:
		return MainnetChainConfig
	case networkname.SepoliaChainName:
		return SepoliaChainConfig
	case networkname.RopstenChainName:
		return RopstenChainConfig
	case networkname.ShandongChainName:
		return ShandongChainConfig
	case networkname.RinkebyChainName:
		return RinkebyChainConfig
	case networkname.GoerliChainName:
		return GoerliChainConfig
	case networkname.SokolChainName:
		return SokolChainConfig
	case networkname.FermionChainName:
		return FermionChainConfig
	case networkname.BSCChainName:
		return BSCChainConfig
	case networkname.ChapelChainName:
		return ChapelChainConfig
	case networkname.RialtoChainName:
		return RialtoChainConfig
	case networkname.MumbaiChainName:
		return MumbaiChainConfig
	case networkname.BorMainnetChainName:
		return BorMainnetChainConfig
	case networkname.BorDevnetChainName:
		return BorDevnetChainConfig
	case networkname.GnosisChainName:
		return GnosisChainConfig
	case networkname.ChiadoChainName:
		return ChiadoChainConfig
	default:
		return nil
	}
}

func GenesisHashByChainName(chain string) *common.Hash {
	switch chain {
	case networkname.MainnetChainName:
		return &MainnetGenesisHash
	case networkname.SepoliaChainName:
		return &SepoliaGenesisHash
	case networkname.RopstenChainName:
		return &RopstenGenesisHash
	case networkname.ShandongChainName:
		return &ShandongGenesisHash
	case networkname.RinkebyChainName:
		return &RinkebyGenesisHash
	case networkname.GoerliChainName:
		return &GoerliGenesisHash
	case networkname.SokolChainName:
		return &SokolGenesisHash
	case networkname.FermionChainName:
		return &FermionGenesisHash
	case networkname.BSCChainName:
		return &BSCGenesisHash
	case networkname.ChapelChainName:
		return &ChapelGenesisHash
	case networkname.RialtoChainName:
		return &RialtoGenesisHash
	case networkname.MumbaiChainName:
		return &MumbaiGenesisHash
	case networkname.BorMainnetChainName:
		return &BorMainnetGenesisHash
	case networkname.BorDevnetChainName:
		return &BorDevnetGenesisHash
	case networkname.GnosisChainName:
		return &GnosisGenesisHash
	case networkname.ChiadoChainName:
		return &ChiadoGenesisHash
	default:
		return nil
	}
}

func ChainConfigByGenesisHash(genesisHash common.Hash) *ChainConfig {
	switch {
	case genesisHash == MainnetGenesisHash:
		return MainnetChainConfig
	case genesisHash == SepoliaGenesisHash:
		return SepoliaChainConfig
	case genesisHash == RopstenGenesisHash:
		return RopstenChainConfig
	case genesisHash == ShandongGenesisHash:
		return ShandongChainConfig
	case genesisHash == RinkebyGenesisHash:
		return RinkebyChainConfig
	case genesisHash == GoerliGenesisHash:
		return GoerliChainConfig
	case genesisHash == SokolGenesisHash:
		return SokolChainConfig
	case genesisHash == FermionGenesisHash:
		return FermionChainConfig
	case genesisHash == BSCGenesisHash:
		return BSCChainConfig
	case genesisHash == ChapelGenesisHash:
		return ChapelChainConfig
	case genesisHash == RialtoGenesisHash:
		return RialtoChainConfig
	case genesisHash == MumbaiGenesisHash:
		return MumbaiChainConfig
	case genesisHash == BorMainnetGenesisHash:
		return BorMainnetChainConfig
	case genesisHash == GnosisGenesisHash:
		return GnosisChainConfig
	case genesisHash == ChiadoGenesisHash:
		return ChiadoChainConfig
	default:
		return nil
	}
}

func NetworkIDByChainName(chain string) uint64 {
	switch chain {
	case networkname.RialtoChainName:
		return 97
	case networkname.DevChainName:
		return 1337
	default:
		config := ChainConfigByChainName(chain)
		if config == nil {
			return 0
		}
		return config.ChainID.Uint64()
	}
}

package params

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/ethereum-optimism/superchain-registry/superchain"
)

const (
	OPMainnetChainID   = 10
	BaseMainnetChainID = 8453
	baseSepoliaChainID = 84532
	pgnSepoliaChainID  = 58008
	devnetChainID      = 997
	chaosnetChainID    = 888
)

// OP Stack chain config
var (
	// March 17, 2023 @ 7:00:00 pm UTC
	OptimismGoerliRegolithTime = big.NewInt(1679079600)
	// May 4, 2023 @ 5:00:00 pm UTC
	BaseGoerliRegolithTime = big.NewInt(1683219600)
	// Apr 21, 2023 @ 6:30:00 pm UTC
	baseGoerliDevnetRegolithTime = big.NewInt(1682101800)
	// March 5, 2023 @ 2:48:00 am UTC
	devnetRegolithTime = big.NewInt(1677984480)
	// August 16, 2023 @ 3:34:22 am UTC
	chaosnetRegolithTime = big.NewInt(1692156862)
)

var OPStackSupport = ProtocolVersionV0{Build: [8]byte{}, Major: 9, Minor: 0, Patch: 0, PreRelease: 0}.Encode()

// OPStackChainConfigByName loads chain config corresponding to the chain name from superchain registry.
// This implementation is based on optimism monorepo(https://github.com/ethereum-optimism/optimism/blob/op-node/v1.4.1/op-node/chaincfg/chains.go#L59)
func OPStackChainConfigByName(name string) *superchain.ChainConfig {
	// Handle legacy name aliases
	name = networkname.HandleLegacyName(name)
	for _, chainCfg := range superchain.OPChains {
		if strings.EqualFold(chainCfg.Chain+"-"+chainCfg.Superchain, name) {
			return chainCfg
		}
	}
	return nil
}

// OPStackChainConfigByGenesisHash loads chain config corresponding to the genesis hash from superchain registry.
func OPStackChainConfigByGenesisHash(genesisHash common.Hash) *superchain.ChainConfig {
	if bytes.Equal(genesisHash.Bytes(), OPMainnetGenesisHash.Bytes()) {
		return superchain.OPChains[OPMainnetChainID]
	}
	for _, chainCfg := range superchain.OPChains {
		if bytes.Equal(chainCfg.Genesis.L2.Hash[:], genesisHash.Bytes()) {
			return chainCfg
		}
	}
	return nil
}

// ChainConfigByOpStackChainName loads chain config corresponding to the chain name from superchain registry, and builds erigon chain config.
func ChainConfigByOpStackChainName(name string) *chain.Config {
	opStackChainCfg := OPStackChainConfigByName(name)
	if opStackChainCfg == nil {
		return nil
	}
	return LoadSuperChainConfig(opStackChainCfg)
}

// ChainConfigByOpStackGenesisHash loads chain config corresponding to the genesis hash from superchain registry, and builds erigon chain config.
func ChainConfigByOpStackGenesisHash(genesisHash common.Hash) *chain.Config {
	opStackChainCfg := OPStackChainConfigByGenesisHash(genesisHash)
	if opStackChainCfg == nil {
		return nil
	}
	return LoadSuperChainConfig(opStackChainCfg)
}

// LoadSuperChainConfig loads superchain config from superchain registry for given chain, and builds erigon chain config.
// This implementation is based on op-geth(https://github.com/ethereum-optimism/op-geth/blob/c7871bc4454ffc924eb128fa492975b30c9c46ad/params/superchain.go#L39)
func LoadSuperChainConfig(opStackChainCfg *superchain.ChainConfig) *chain.Config {
	chConfig, ok := superchain.OPChains[opStackChainCfg.ChainID]
	if !ok {
		panic(fmt.Sprintf("LoadSuperChainConfig: unknown chain ID: %d", opStackChainCfg.ChainID))
	}
	out := &chain.Config{
		ChainName:                     opStackChainCfg.Name,
		ChainID:                       new(big.Int).SetUint64(opStackChainCfg.ChainID),
		HomesteadBlock:                common.Big0,
		DAOForkBlock:                  nil,
		TangerineWhistleBlock:         common.Big0,
		SpuriousDragonBlock:           common.Big0,
		ByzantiumBlock:                common.Big0,
		ConstantinopleBlock:           common.Big0,
		PetersburgBlock:               common.Big0,
		IstanbulBlock:                 common.Big0,
		MuirGlacierBlock:              common.Big0,
		BerlinBlock:                   common.Big0,
		LondonBlock:                   common.Big0,
		ArrowGlacierBlock:             common.Big0,
		GrayGlacierBlock:              common.Big0,
		MergeNetsplitBlock:            common.Big0,
		ShanghaiTime:                  nil,
		CancunTime:                    nil,
		PragueTime:                    nil,
		BedrockBlock:                  common.Big0,
		RegolithTime:                  big.NewInt(0),
		CanyonTime:                    nil,
		EcotoneTime:                   nil,
		FjordTime:                     nil,
		GraniteTime:                   nil,
		HoloceneTime:                  nil,
		TerminalTotalDifficulty:       common.Big0,
		TerminalTotalDifficultyPassed: true,
		Ethash:                        nil,
		Clique:                        nil,
		Optimism:                      nil,
	}

	if chConfig.CanyonTime != nil {
		out.ShanghaiTime = new(big.Int).SetUint64(*chConfig.CanyonTime) // Shanghai activates with Canyon
		out.CanyonTime = new(big.Int).SetUint64(*chConfig.CanyonTime)
	}
	if chConfig.EcotoneTime != nil {
		out.CancunTime = new(big.Int).SetUint64(*chConfig.EcotoneTime) // CancunTime activates with Ecotone
		out.EcotoneTime = new(big.Int).SetUint64(*chConfig.EcotoneTime)
	}
	if chConfig.FjordTime != nil {
		out.FjordTime = new(big.Int).SetUint64(*chConfig.FjordTime)
	}
	if chConfig.GraniteTime != nil {
		out.GraniteTime = new(big.Int).SetUint64(*chConfig.GraniteTime)
	}
	if chConfig.HoloceneTime != nil {
		out.HoloceneTime = new(big.Int).SetUint64(*chConfig.HoloceneTime)
	}

	if chConfig.Optimism != nil {
		out.Optimism = &chain.OptimismConfig{
			EIP1559Elasticity:  chConfig.Optimism.EIP1559Elasticity,
			EIP1559Denominator: chConfig.Optimism.EIP1559Denominator,
		}
		if chConfig.Optimism.EIP1559DenominatorCanyon != nil {
			out.Optimism.EIP1559DenominatorCanyon = *chConfig.Optimism.EIP1559DenominatorCanyon
		}
	}

	// special overrides for OP-Stack chains with pre-Regolith upgrade history
	switch opStackChainCfg.ChainID {
	case OPMainnetChainID:
		out.BerlinBlock = big.NewInt(3950000)
		out.LondonBlock = big.NewInt(105235063)
		out.ArrowGlacierBlock = big.NewInt(105235063)
		out.GrayGlacierBlock = big.NewInt(105235063)
		out.MergeNetsplitBlock = big.NewInt(105235063)
		out.BedrockBlock = big.NewInt(105235063)
	}

	return out
}

// ProtocolVersion encodes the OP-Stack protocol version. See OP-Stack superchain-upgrade specification.
type ProtocolVersion [32]byte

func (p ProtocolVersion) MarshalText() ([]byte, error) {
	return common.Hash(p).MarshalText()
}

func (p *ProtocolVersion) UnmarshalText(input []byte) error {
	return (*common.Hash)(p).UnmarshalText(input)
}

func (p ProtocolVersion) Parse() (versionType uint8, build [8]byte, major, minor, patch, preRelease uint32) {
	versionType = p[0]
	if versionType != 0 {
		return
	}
	// bytes 1:8 reserved for future use
	copy(build[:], p[8:16])                        // differentiates forks and custom-builds of standard protocol
	major = binary.BigEndian.Uint32(p[16:20])      // incompatible API changes
	minor = binary.BigEndian.Uint32(p[20:24])      // identifies additional functionality in backwards compatible manner
	patch = binary.BigEndian.Uint32(p[24:28])      // identifies backward-compatible bug-fixes
	preRelease = binary.BigEndian.Uint32(p[28:32]) // identifies unstable versions that may not satisfy the above
	return
}

func (p ProtocolVersion) String() string {
	versionType, build, major, minor, patch, preRelease := p.Parse()
	if versionType != 0 {
		return "v0.0.0-unknown." + common.Hash(p).String()
	}
	ver := fmt.Sprintf("v%d.%d.%d", major, minor, patch)
	if preRelease != 0 {
		ver += fmt.Sprintf("-%d", preRelease)
	}
	if build != ([8]byte{}) {
		if humanBuildTag(build) {
			ver += fmt.Sprintf("+%s", strings.TrimRight(string(build[:]), "\x00"))
		} else {
			ver += fmt.Sprintf("+0x%x", build)
		}
	}
	return ver
}

// humanBuildTag identifies which build tag we can stringify for human-readable versions
func humanBuildTag(v [8]byte) bool {
	for i, c := range v { // following semver.org advertised regex, alphanumeric with '-' and '.', except leading '.'.
		if c == 0 { // trailing zeroed are allowed
			for _, d := range v[i:] {
				if d != 0 {
					return false
				}
			}
			return true
		}
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || (c == '.' && i > 0)) {
			return false
		}
	}
	return true
}

// ProtocolVersionComparison is used to identify how far ahead/outdated a protocol version is relative to another.
// This value is used in metrics and switch comparisons, to easily identify each type of version difference.
// Negative values mean the version is outdated.
// Positive values mean the version is up-to-date.
// Matching versions have a 0.
type ProtocolVersionComparison int

const (
	AheadMajor         ProtocolVersionComparison = 4
	OutdatedMajor      ProtocolVersionComparison = -4
	AheadMinor         ProtocolVersionComparison = 3
	OutdatedMinor      ProtocolVersionComparison = -3
	AheadPatch         ProtocolVersionComparison = 2
	OutdatedPatch      ProtocolVersionComparison = -2
	AheadPrerelease    ProtocolVersionComparison = 1
	OutdatedPrerelease ProtocolVersionComparison = -1
	Matching           ProtocolVersionComparison = 0
	DiffVersionType    ProtocolVersionComparison = 100
	DiffBuild          ProtocolVersionComparison = 101
	EmptyVersion       ProtocolVersionComparison = 102
)

func (p ProtocolVersion) Compare(other ProtocolVersion) (cmp ProtocolVersionComparison) {
	if p == (ProtocolVersion{}) || (other == (ProtocolVersion{})) {
		return EmptyVersion
	}
	aVersionType, aBuild, aMajor, aMinor, aPatch, aPreRelease := p.Parse()
	bVersionType, bBuild, bMajor, bMinor, bPatch, bPreRelease := other.Parse()
	if aVersionType != bVersionType {
		return DiffVersionType
	}
	if aBuild != bBuild {
		return DiffBuild
	}
	fn := func(a, b uint32, ahead, outdated ProtocolVersionComparison) ProtocolVersionComparison {
		if a == b {
			return Matching
		}
		if a > b {
			return ahead
		}
		return outdated
	}
	if c := fn(aMajor, bMajor, AheadMajor, OutdatedMajor); c != Matching {
		return c
	}
	if c := fn(aMinor, bMinor, AheadMinor, OutdatedMinor); c != Matching {
		return c
	}
	if c := fn(aPatch, bPatch, AheadPatch, OutdatedPatch); c != Matching {
		return c
	}
	return fn(aPreRelease, bPreRelease, AheadPrerelease, OutdatedPrerelease)
}

type ProtocolVersionV0 struct {
	Build                           [8]byte
	Major, Minor, Patch, PreRelease uint32
}

func (v ProtocolVersionV0) Encode() (out ProtocolVersion) {
	copy(out[8:16], v.Build[:])
	binary.BigEndian.PutUint32(out[16:20], v.Major)
	binary.BigEndian.PutUint32(out[20:24], v.Minor)
	binary.BigEndian.PutUint32(out[24:28], v.Patch)
	binary.BigEndian.PutUint32(out[28:32], v.PreRelease)
	return
}

// Copyright 2022 The Erigon Authors
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

package clparams

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	mathrand "math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

var LatestStateFileName = "latest.ssz_snappy"

type CaplinConfig struct {
	// Archive related config
	ArchiveBlocks             bool
	ArchiveBlobs              bool
	ArchiveStates             bool
	ImmediateBlobsBackfilling bool
	BlobPruningDisabled       bool
	SnapshotGenerationEnabled bool
	// Network related config
	NetworkId NetworkType
	// DisableCheckpointSync is optional and is used to disable checkpoint sync used by default in the node
	DisabledCheckpointSync bool
	// CaplinMeVRelayUrl is optional and is used to connect to the external builder service.
	// If it's set, the node will start in builder mode
	MevRelayUrl string
	// EnableValidatorMonitor is used to enable the validator monitor metrics and corresponding logs
	EnableValidatorMonitor bool

	// Devnets config
	CustomConfigPath       string
	CustomGenesisStatePath string

	// Network stuff
	CaplinDiscoveryAddr         string
	CaplinDiscoveryPort         uint64
	CaplinDiscoveryTCPPort      uint64
	SentinelAddr                string
	SentinelPort                uint64
	SubscribeAllTopics          bool
	MaxPeerCount                uint64
	EnableUPnP                  bool
	MaxInboundTrafficPerPeer    datasize.ByteSize
	MaxOutboundTrafficPerPeer   datasize.ByteSize
	AdptableTrafficRequirements bool
	// Erigon Sync
	LoopBlockLimit uint64
	// Beacon API router configuration
	BeaconAPIRouter beacon_router_configuration.RouterConfiguration

	BootstrapNodes []string
	StaticPeers    []string

	// Extra
	EnableEngineAPI bool
}

func (c CaplinConfig) IsDevnet() bool {
	return c.CustomConfigPath != "" || c.CustomGenesisStatePath != ""
}

func (c CaplinConfig) HaveInvalidDevnetParams() bool {
	return c.CustomConfigPath == "" || c.CustomGenesisStatePath == ""
}

func (c CaplinConfig) RelayUrlExist() bool {
	return c.MevRelayUrl != ""
}

type NetworkType int

const CustomNetwork NetworkType = -1

const (
	MaxDialTimeout               = 15 * time.Second
	VersionLength  int           = 4
	MaxChunkSize   uint64        = 15 * 1024 * 1024
	ReqTimeout     time.Duration = 5 * time.Second
	RespTimeout    time.Duration = 10 * time.Second
)

const (
	SubDivisionFolderSize = 10_000
	SlotsPerDump          = 1536
)

var (
	MainnetBootstrapNodes = []string{
		"enr:-KG4QNTx85fjxABbSq_Rta9wy56nQ1fHK0PewJbGjLm1M4bMGx5-3Qq4ZX2-iFJ0pys_O90sVXNNOxp2E7afBsGsBrgDhGV0aDKQu6TalgMAAAD__________4JpZIJ2NIJpcIQEnfA2iXNlY3AyNTZrMaECGXWQ-rQ2KZKRH1aOW4IlPDBkY4XDphxg9pxKytFCkayDdGNwgiMog3VkcIIjKA",
		"enr:-KG4QF4B5WrlFcRhUU6dZETwY5ZzAXnA0vGC__L1Kdw602nDZwXSTs5RFXFIFUnbQJmhNGVU6OIX7KVrCSTODsz1tK4DhGV0aDKQu6TalgMAAAD__________4JpZIJ2NIJpcIQExNYEiXNlY3AyNTZrMaECQmM9vp7KhaXhI-nqL_R0ovULLCFSFTa9CPPSdb1zPX6DdGNwgiMog3VkcIIjKA",
		// Prylab team's bootnodes
		"enr:-Ku4QImhMc1z8yCiNJ1TyUxdcfNucje3BGwEHzodEZUan8PherEo4sF7pPHPSIB1NNuSg5fZy7qFsjmUKs2ea1Whi0EBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQOVphkDqal4QzPMksc5wnpuC3gvSC8AfbFOnZY_On34wIN1ZHCCIyg",
		"enr:-Ku4QP2xDnEtUXIjzJ_DhlCRN9SN99RYQPJL92TMlSv7U5C1YnYLjwOQHgZIUXw6c-BvRg2Yc2QsZxxoS_pPRVe0yK8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMeFF5GrS7UZpAH2Ly84aLK-TyvH-dRo0JM1i8yygH50YN1ZHCCJxA",
		"enr:-Ku4QPp9z1W4tAO8Ber_NQierYaOStqhDqQdOPY3bB3jDgkjcbk6YrEnVYIiCBbTxuar3CzS528d2iE7TdJsrL-dEKoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMw5fqqkw2hHC4F5HZZDPsNmPdB1Gi8JPQK7pRc9XHh-oN1ZHCCKvg",
		// Lighthouse team's bootnodes
		"enr:-Jq4QItoFUuug_n_qbYbU0OY04-np2wT8rUCauOOXNi0H3BWbDj-zbfZb7otA7jZ6flbBpx1LNZK2TDebZ9dEKx84LYBhGV0aDKQtTA_KgEAAAD__________4JpZIJ2NIJpcISsaa0ZiXNlY3AyNTZrMaEDHAD2JKYevx89W0CcFJFiskdcEzkH_Wdv9iW42qLK79ODdWRwgiMo",
		"enr:-Jq4QN_YBsUOqQsty1OGvYv48PMaiEt1AzGD1NkYQHaxZoTyVGqMYXg0K9c0LPNWC9pkXmggApp8nygYLsQwScwAgfgBhGV0aDKQtTA_KgEAAAD__________4JpZIJ2NIJpcISLosQxiXNlY3AyNTZrMaEDBJj7_dLFACaxBfaI8KZTh_SSJUjhyAyfshimvSqo22WDdWRwgiMo",
		// EF bootnodes
		"enr:-Ku4QHqVeJ8PPICcWk1vSn_XcSkjOkNiTg6Fmii5j6vUQgvzMc9L1goFnLKgXqBJspJjIsB91LTOleFmyWWrFVATGngBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAMRHkWJc2VjcDI1NmsxoQKLVXFOhp2uX6jeT0DvvDpPcU8FWMjQdR4wMuORMhpX24N1ZHCCIyg",
		"enr:-Ku4QG-2_Md3sZIAUebGYT6g0SMskIml77l6yR-M_JXc-UdNHCmHQeOiMLbylPejyJsdAPsTHJyjJB2sYGDLe0dn8uYBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhBLY-NyJc2VjcDI1NmsxoQORcM6e19T1T9gi7jxEZjk_sjVLGFscUNqAY9obgZaxbIN1ZHCCIyg",
		"enr:-Ku4QPn5eVhcoF1opaFEvg1b6JNFD2rqVkHQ8HApOKK61OIcIXD127bKWgAtbwI7pnxx6cDyk_nI88TrZKQaGMZj0q0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDayLMaJc2VjcDI1NmsxoQK2sBOLGcUb4AwuYzFuAVCaNHA-dy24UuEKkeFNgCVCsIN1ZHCCIyg",
		"enr:-Ku4QEWzdnVtXc2Q0ZVigfCGggOVB2Vc1ZCPEc6j21NIFLODSJbvNaef1g4PxhPwl_3kax86YPheFUSLXPRs98vvYsoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDZBrP2Jc2VjcDI1NmsxoQM6jr8Rb1ktLEsVcKAPa08wCsKUmvoQ8khiOl_SLozf9IN1ZHCCIyg",
		// Nimbus bootnodes
		"enr:-LK4QA8FfhaAjlb_BXsXxSfiysR7R52Nhi9JBt4F8SPssu8hdE1BXQQEtVDC3qStCW60LSO7hEsVHv5zm8_6Vnjhcn0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAN4aBKJc2VjcDI1NmsxoQJerDhsJ-KxZ8sHySMOCmTO6sHM3iCFQ6VMvLTe948MyYN0Y3CCI4yDdWRwgiOM",
		"enr:-LK4QKWrXTpV9T78hNG6s8AM6IO4XH9kFT91uZtFg1GcsJ6dKovDOr1jtAAFPnS2lvNltkOGA9k29BUN7lFh_sjuc9QBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhANAdd-Jc2VjcDI1NmsxoQLQa6ai7y9PMN5hpLe5HmiJSlYzMuzP7ZhwRiwHvqNXdoN0Y3CCI4yDdWRwgiOM",
	}
	SepoliaBootstrapNodes = append(MainnetBootstrapNodes,
		"enr:-Iq4QMCTfIMXnow27baRUb35Q8iiFHSIDBJh6hQM5Axohhf4b6Kr_cOCu0htQ5WvVqKvFgY28893DHAg8gnBAXsAVqmGAX53x8JggmlkgnY0gmlwhLKAlv6Jc2VjcDI1NmsxoQK6S-Cii_KmfFdUJL2TANL3ksaKUnNXvTCv1tLwXs0QgIN1ZHCCIyk",
		"enr:-KG4QE5OIg5ThTjkzrlVF32WT_-XT14WeJtIz2zoTqLLjQhYAmJlnk4ItSoH41_2x0RX0wTFIe5GgjRzU2u7Q1fN4vADhGV0aDKQqP7o7pAAAHAyAAAAAAAAAIJpZIJ2NIJpcISlFsStiXNlY3AyNTZrMaEC-Rrd_bBZwhKpXzFCrStKp1q_HmGOewxY3KwM8ofAj_ODdGNwgiMog3VkcIIjKA",
		"enr:-L64QC9Hhov4DhQ7mRukTOz4_jHm4DHlGL726NWH4ojH1wFgEwSin_6H95Gs6nW2fktTWbPachHJ6rUFu0iJNgA0SB2CARqHYXR0bmV0c4j__________4RldGgykDb6UBOQAABx__________-CaWSCdjSCaXCEA-2vzolzZWNwMjU2azGhA17lsUg60R776rauYMdrAz383UUgESoaHEzMkvm4K6k6iHN5bmNuZXRzD4N0Y3CCIyiDdWRwgiMo",
		// Teku bootnode
		"enr:-KO4QP7MmB3juk8rUjJHcUoxZDU9Np4FlW0HyDEGIjSO7GD9PbSsabu7713cWSUWKDkxIypIXg1A-6lG7ySRGOMZHeGCAmuEZXRoMpDTH2GRkAAAc___________gmlkgnY0gmlwhBSoyGOJc2VjcDI1NmsxoQNta5b_bexSSwwrGW2Re24MjfMntzFd0f2SAxQtMj3ueYN0Y3CCIyiDdWRwgiMo",
		// Lodestar bootnode
		"enr:-KG4QJejf8KVtMeAPWFhN_P0c4efuwu1pZHELTveiXUeim6nKYcYcMIQpGxxdgT2Xp9h-M5pr9gn2NbbwEAtxzu50Y8BgmlkgnY0gmlwhEEVkQCDaXA2kCoBBPnAEJg4AAAAAAAAAAGJc2VjcDI1NmsxoQLEh_eVvk07AQABvLkTGBQTrrIOQkzouMgSBtNHIRUxOIN1ZHCCIyiEdWRwNoIjKA",
		// EF bootnodes
		"enr:-Ku4QDZ_rCowZFsozeWr60WwLgOfHzv1Fz2cuMvJqN5iJzLxKtVjoIURY42X_YTokMi3IGstW5v32uSYZyGUXj9Q_IECh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCo_ujukAAAaf__________gmlkgnY0gmlwhIpEe5iJc2VjcDI1NmsxoQNHTpFdaNSCEWiN_QqT396nb0PzcUpLe3OVtLph-AciBYN1ZHCCIy0",
		"enr:-Ku4QHRyRwEPT7s0XLYzJ_EeeWvZTXBQb4UCGy1F_3m-YtCNTtDlGsCMr4UTgo4uR89pv11uM-xq4w6GKfKhqU31hTgCh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCo_ujukAAAaf__________gmlkgnY0gmlwhIrFM7WJc2VjcDI1NmsxoQI4diTwChN3zAAkarf7smOHCdFb1q3DSwdiQ_Lc_FdzFIN1ZHCCIy0",
		"enr:-Ku4QOkvvf0u5Hg4-HhY-SJmEyft77G5h3rUM8VF_e-Hag5cAma3jtmFoX4WElLAqdILCA-UWFRN1ZCDJJVuEHrFeLkDh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCo_ujukAAAaf__________gmlkgnY0gmlwhJK-AWeJc2VjcDI1NmsxoQLFcT5VE_NMiIC8Ll7GypWDnQ4UEmuzD7hF_Hf4veDJwIN1ZHCCIy0",
		"enr:-Ku4QH6tYsHKITYeHUu5kdfXgEZWI18EWk_2RtGOn1jBPlx2UlS_uF3Pm5Dx7tnjOvla_zs-wwlPgjnEOcQDWXey51QCh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCo_ujukAAAaf__________gmlkgnY0gmlwhIs7Mc6Jc2VjcDI1NmsxoQIET4Mlv9YzhrYhX_H9D7aWMemUrvki6W4J2Qo0YmFMp4N1ZHCCIy0",
		"enr:-Ku4QDmz-4c1InchGitsgNk4qzorWMiFUoaPJT4G0IiF8r2UaevrekND1o7fdoftNucirj7sFFTTn2-JdC2Ej0p1Mn8Ch2F0dG5ldHOIAAAAAAAAAACEZXRoMpCo_ujukAAAaf__________gmlkgnY0gmlwhKpA-liJc2VjcDI1NmsxoQMpHP5U1DK8O_JQU6FadmWbE42qEdcGlllR8HcSkkfWq4N1ZHCCIy0")

	GnosisBootstrapNodes = append(MainnetBootstrapNodes, []string{
		"enr:-Ly4QIAhiTHk6JdVhCdiLwT83wAolUFo5J4nI5HrF7-zJO_QEw3cmEGxC1jvqNNUN64Vu-xxqDKSM528vKRNCehZAfEBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhEFtZ5SJc2VjcDI1NmsxoQJwgL5C-30E8RJmW8gCb7sfwWvvfre7wGcCeV4X1G2wJYhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-Ly4QDhEjlkf8fwO5uWAadexy88GXZneTuUCIPHhv98v8ZfXMtC0S1S_8soiT0CMEgoeLe9Db01dtkFQUnA9YcnYC_8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhEFtZ5WJc2VjcDI1NmsxoQMRSho89q2GKx_l2FZhR1RmnSiQr6o_9hfXfQUuW6bjMohzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-Ly4QLKgv5M2D4DYJgo6s4NG_K4zu4sk5HOLCfGCdtgoezsbfRbfGpQ4iSd31M88ec3DHA5FWVbkgIas9EaJeXia0nwBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYRaJc2VjcDI1NmsxoQLpK_A47iNBkVjka9Mde1F-Kie-R0sq97MCNKCxt2HwOIhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-Ly4QF_0qvji6xqXrhQEhwJR1W9h5dXV7ZjVCN_NlosKxcgZW6emAfB_KXxEiPgKr_-CZG8CWvTiojEohG1ewF7P368Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYUqJc2VjcDI1NmsxoQIpNRUT6llrXqEbjkAodsZOyWv8fxQkyQtSvH4sg2D7n4hzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-Ly4QCD5D99p36WafgTSxB6kY7D2V1ca71C49J4VWI2c8UZCCPYBvNRWiv0-HxOcbpuUdwPVhyWQCYm1yq2ZH0ukCbQBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYVSJc2VjcDI1NmsxoQJJMSV8iSZ8zvkgbi8cjIGEUVJeekLqT0LQha_co-siT4hzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-KK4QKXJq1QOVWuJAGige4uaT8LRPQGCVRf3lH3pxjaVScMRUfFW1eiiaz8RwOAYvw33D4EX-uASGJ5QVqVCqwccxa-Bi4RldGgykCGm-DYDAABk__________-CaWSCdjSCaXCEM0QnzolzZWNwMjU2azGhAhNvrRkpuK4MWTf3WqiOXSOePL8Zc-wKVpZ9FQx_BDadg3RjcIIjKIN1ZHCCIyg",
		"enr:-LO4QO87Rn2ejN3SZdXkx7kv8m11EZ3KWWqoIN5oXwQ7iXR9CVGd1dmSyWxOL1PGsdIqeMf66OZj4QGEJckSi6okCdWBpIdhdHRuZXRziAAAAABgAAAAhGV0aDKQPr_UhAQAAGT__________4JpZIJ2NIJpcIQj0iX1iXNlY3AyNTZrMaEDd-_eqFlWWJrUfEp8RhKT9NxdYaZoLHvsp3bbejPyOoeDdGNwgiMog3VkcIIjKA",
		"enr:-LK4QIJUAxX9uNgW4ACkq8AixjnSTcs9sClbEtWRq9F8Uy9OEExsr4ecpBTYpxX66cMk6pUHejCSX3wZkK2pOCCHWHEBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpA-v9SEBAAAZP__________gmlkgnY0gmlwhCPSnDuJc2VjcDI1NmsxoQNuaAjFE-ANkH3pbeBdPiEIwjR5kxFuKaBWxHkqFuPz5IN0Y3CCIyiDdWRwgiMo",
	}...)
	ChiadoBootstrapNodes = append(MainnetBootstrapNodes, []string{
		"enr:-L64QOijsdi9aVIawMb5h5PWueaPM9Ai6P17GNPFlHzz7MGJQ8tFMdYrEx8WQitNKLG924g2Q9cCdzg54M0UtKa3QIKCMxaHYXR0bmV0c4j__________4RldGgykDE2cEMCAABv__________-CaWSCdjSCaXCEi5AaWYlzZWNwMjU2azGhA8CjTkD4m1s8FbKCN18LgqlYcE65jrT148vFtwd9U62SiHN5bmNuZXRzD4N0Y3CCIyiDdWRwgiMo",
		"enr:-L64QKYKGQj5ybkfBxyFU5IEVzP7oJkGHJlie4W8BCGAYEi4P0mmMksaasiYF789mVW_AxYVNVFUjg9CyzmdvpyWQ1KCMlmHYXR0bmV0c4j__________4RldGgykDE2cEMCAABv__________-CaWSCdjSCaXCEi5CtNolzZWNwMjU2azGhAuA7BAwIijy1z81AO9nz_MOukA1ER68rGA67PYQ5pF1qiHN5bmNuZXRzD4N0Y3CCIyiDdWRwgiMo",
		"enr:-Ly4QJJUnV9BxP_rw2Bv7E9iyw4sYS2b4OQZIf4Mu_cA6FljJvOeSTQiCUpbZhZjR4R0VseBhdTzrLrlHrAuu_OeZqgJh2F0dG5ldHOI__________-EZXRoMpAxNnBDAgAAb___________gmlkgnY0gmlwhIuQGnOJc2VjcDI1NmsxoQPT_u3IjDtB2r-nveH5DhUmlM8F2IgLyxhmwmqW4L5k3ohzeW5jbmV0cw-DdGNwgiMog3VkcIIjKA",
		"enr:-MK4QCkOyqOTPX1_-F-5XVFjPclDUc0fj3EeR8FJ5-hZjv6ARuGlFspM0DtioHn1r6YPUXkOg2g3x6EbeeKdsrvVBYmGAYQKrixeh2F0dG5ldHOIAAAAAAAAAACEZXRoMpAxNnBDAgAAb___________gmlkgnY0gmlwhIuQGlWJc2VjcDI1NmsxoQKdW3-DgLExBkpLGMRtuM88wW_gZkC7Yeg0stYDTrlynYhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-Ly4QLYLNqrjvSxD3lpAPBUNlxa6cIbe79JqLZLFcZZjWoCjZcw-85agLUErHiygG2weRSCLnd5V460qTbLbwJQsfZkoh2F0dG5ldHOI__________-EZXRoMpAxNnBDAgAAb___________gmlkgnY0gmlwhKq7mu-Jc2VjcDI1NmsxoQP900YAYa9kdvzlSKGjVo-F3XVzATjOYp3BsjLjSophO4hzeW5jbmV0cw-DdGNwgiMog3VkcIIjKA",
		"enr:-Ly4QCGeYvTCNOGKi0mKRUd45rLj96b4pH98qG7B9TCUGXGpHZALtaL2-XfjASQyhbCqENccI4PGXVqYTIehNT9KJMQgh2F0dG5ldHOI__________-EZXRoMpAxNnBDAgAAb___________gmlkgnY0gmlwhIuQrVSJc2VjcDI1NmsxoQP9iDchx2PGl3JyJ29B9fhLCvVMN6n23pPAIIeFV-sHOIhzeW5jbmV0cw-DdGNwgiMog3VkcIIjKA",
		"enr:-Ly4QAtr21x5Ps7HYhdZkIBRBgcBkvlIfEel1YNjtFWf4cV3au2LgBGICz9PtEs9-p2HUl_eME8m1WImxTxSB3AkCMwBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpAxNnBDAgAAb___________gmlkgnY0gmlwhANHhOeJc2VjcDI1NmsxoQNLp1QPV8-pyMCohOtj6xGtSBM_GtVTqzlbvNsCF4ezkYhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-Ly4QLgn8Bx6faigkKUGZQvd1HDToV2FAxZIiENK-lczruzQb90qJK-4E65ADly0s4__dQOW7IkLMW7ZAyJy2vtiLy8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpAxNnBDAgAAb___________gmlkgnY0gmlwhANFIw2Jc2VjcDI1NmsxoQMa-fWEy9UJHfOl_lix3wdY5qust78sHAqZnWwEiyqKgYhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-KG4QF7z4LUdMfgwvh-fS-MDv_1hPSUCqGfyOWGLNJuoBHKFAMSHz8geQn8v3qDDbuSQKud3WIAjKqR4gqJoLBUEJ08ZhGV0aDKQDc1ElgAAAG___________4JpZIJ2NIJpcIQjzq5ciXNlY3AyNTZrMaECt7YO363pV54d3QdgnluL5kxzhCR_k0yM9C-G6bqMGoKDdGNwgiMog3VkcIIjKA",
		"enr:-LK4QCUTEmZrT1AgCKdyVgwnHL5J0VSoxsyjruAtwo-owBTBVEOyAnQRVNXlcW5aL-ycntk5oHDrKCR-DXZAlUAKpjEBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCdM7Z1BAAAb___________gmlkgnY0gmlwhCPSfheJc2VjcDI1NmsxoQNpdf8U9pzsU9m6Hzgd1rmTI-On-QImJnkZBGqDp4org4N0Y3CCIyiDdWRwgiMo",
	}...)
	HoleskyBootstrapNodes = append(MainnetBootstrapNodes, []string{
		"enr:-Ku4QFo-9q73SspYI8cac_4kTX7yF800VXqJW4Lj3HkIkb5CMqFLxciNHePmMt4XdJzHvhrCC5ADI4D_GkAsxGJRLnQBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpAhnTT-AQFwAP__________gmlkgnY0gmlwhLKAiOmJc2VjcDI1NmsxoQORcM6e19T1T9gi7jxEZjk_sjVLGFscUNqAY9obgZaxbIN1ZHCCIyk",
		"enr:-Ku4QPG7F72mbKx3gEQEx07wpYYusGDh-ni6SNkLvOS-hhN-BxIggN7tKlmalb0L5JPoAfqD-akTZ-gX06hFeBEz4WoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpAhnTT-AQFwAP__________gmlkgnY0gmlwhJK-DYCJc2VjcDI1NmsxoQKLVXFOhp2uX6jeT0DvvDpPcU8FWMjQdR4wMuORMhpX24N1ZHCCIyk",
		"enr:-LK4QPxe-mDiSOtEB_Y82ozvxn9aQM07Ui8A-vQHNgYGMMthfsfOabaaTHhhJHFCBQQVRjBww_A5bM1rf8MlkJU_l68Eh2F0dG5ldHOIAADAAAAAAACEZXRoMpBpt9l0BAFwAAABAAAAAAAAgmlkgnY0gmlwhLKAiOmJc2VjcDI1NmsxoQJu6T9pclPObAzEVQ53DpVQqjadmVxdTLL-J3h9NFoCeIN0Y3CCIyiDdWRwgiMo",
		"enr:-Ly4QGbOw4xNel5EhmDsJJ-QhC9XycWtsetnWoZ0uRy381GHdHsNHJiCwDTOkb3S1Ade0SFQkWJX_pgb3g8Jfh93rvMBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpBpt9l0BAFwAAABAAAAAAAAgmlkgnY0gmlwhJK-DYCJc2VjcDI1NmsxoQOxKv9sv3zKF8GDewgFGGHKP5HCZZpPpTrwl9eXKAWGxIhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
		"enr:-LS4QG0uV4qvcpJ-HFDJRGBmnlD3TJo7yc4jwK8iP7iKaTlfQ5kZvIDspLMJhk7j9KapuL9yyHaZmwTEZqr10k9XumyCEcmHYXR0bmV0c4gAAAAABgAAAIRldGgykGm32XQEAXAAAAEAAAAAAACCaWSCdjSCaXCErK4j-YlzZWNwMjU2azGhAgfWRBEJlb7gAhXIB5ePmjj2b8io0UpEenq1Kl9cxStJg3RjcIIjKIN1ZHCCIyg",
		"enr:-Le4QLoE1wFHSlGcm48a9ZESb_MRLqPPu6G0vHqu4MaUcQNDHS69tsy-zkN0K6pglyzX8m24mkb-LtBcbjAYdP1uxm4BhGV0aDKQabfZdAQBcAAAAQAAAAAAAIJpZIJ2NIJpcIQ5gR6Wg2lwNpAgAUHQBwEQAAAAAAAAADR-iXNlY3AyNTZrMaEDPMSNdcL92uNIyCsS177Z6KTXlbZakQqxv3aQcWawNXeDdWRwgiMohHVkcDaCI4I",
	}...)
	HoodiBootstrapNodes = []string{
		"enr:-Mq4QLkmuSwbGBUph1r7iHopzRpdqE-gcm5LNZfcE-6T37OCZbRHi22bXZkaqnZ6XdIyEDTelnkmMEQB8w6NbnJUt9GGAZWaowaYh2F0dG5ldHOIABgAAAAAAACEZXRoMpDS8Zl_YAAJEAAIAAAAAAAAgmlkgnY0gmlwhNEmfKCEcXVpY4IyyIlzZWNwMjU2azGhA0hGa4jZJZYQAS-z6ZFK-m4GCFnWS8wfjO0bpSQn6hyEiHN5bmNuZXRzAIN0Y3CCIyiDdWRwgiMo",
		"enr:-Ku4QLVumWTwyOUVS4ajqq8ZuZz2ik6t3Gtq0Ozxqecj0qNZWpMnudcvTs-4jrlwYRQMQwBS8Pvtmu4ZPP2Lx3i2t7YBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpBd9cEGEAAJEP__________gmlkgnY0gmlwhNEmfKCJc2VjcDI1NmsxoQLdRlI8aCa_ELwTJhVN8k7km7IDc3pYu-FMYBs5_FiigIN1ZHCCIyk",
		"enr:-LK4QAYuLujoiaqCAs0-qNWj9oFws1B4iy-Hff1bRB7wpQCYSS-IIMxLWCn7sWloTJzC1SiH8Y7lMQ5I36ynGV1ASj4Eh2F0dG5ldHOIYAAAAAAAAACEZXRoMpDS8Zl_YAAJEAAIAAAAAAAAgmlkgnY0gmlwhIbRilSJc2VjcDI1NmsxoQOmI5MlAu3f5WEThAYOqoygpS2wYn0XS5NV2aYq7T0a04N0Y3CCIyiDdWRwgiMo",
		"enr:-Ku4QIC89sMC0o-irosD4_23lJJ4qCGOvdUz7SmoShWx0k6AaxCFTKviEHa-sa7-EzsiXpDp0qP0xzX6nKdXJX3X-IQBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpBd9cEGEAAJEP__________gmlkgnY0gmlwhIbRilSJc2VjcDI1NmsxoQK_m0f1DzDc9Cjrspm36zuRa7072HSiMGYWLsKiVSbP34N1ZHCCIyk",
		"enr:-Ku4QNkWjw5tNzo8DtWqKm7CnDdIq_y7xppD6c1EZSwjB8rMOkSFA1wJPLoKrq5UvA7wcxIotH6Usx3PAugEN2JMncIBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpBd9cEGEAAJEP__________gmlkgnY0gmlwhIbHuBeJc2VjcDI1NmsxoQP3FwrhFYB60djwRjAoOjttq6du94DtkQuaN99wvgqaIYN1ZHCCIyk",
		"enr:-OS4QMJGE13xEROqvKN1xnnt7U-noc51VXyM6wFMuL9LMhQDfo1p1dF_zFdS4OsnXz_vIYk-nQWnqJMWRDKvkSK6_CwDh2F0dG5ldHOIAAAAADAAAACGY2xpZW502IpMaWdodGhvdXNljDcuMC4wLWJldGEuM4RldGgykNLxmX9gAAkQAAgAAAAAAACCaWSCdjSCaXCEhse4F4RxdWljgiMqiXNlY3AyNTZrMaECef77P8k5l3PC_raLw42OAzdXfxeQ-58BJriNaqiRGJSIc3luY25ldHMAg3RjcIIjKIN1ZHCCIyg",
	}
)

type NetworkConfig struct {
	GossipMaxSize                   uint64             `yaml:"GOSSIP_MAX_SIZE" json:"GOSSIP_MAX_SIZE,string"`                                       // The maximum allowed size of uncompressed gossip messages.
	GossipMaxSizeBellatrix          uint64             `yaml:"GOSSIP_MAX_SIZE_BELLATRIX" json:"GOSSIP_MAX_SIZE_BELLATRIX,string"`                   // The maximum allowed size of bellatrix uncompressed gossip messages.
	MaxChunkSize                    uint64             `yaml:"MAX_CHUNK_SIZE" json:"MAX_CHUNK_SIZE,string"`                                         // The maximum allowed size of uncompressed req/resp chunked responses.
	AttestationSubnetCount          uint64             `yaml:"ATTESTATION_SUBNET_COUNT" json:"ATTESTATION_SUBNET_COUNT,string"`                     // The number of attestation subnets used in the gossipsub protocol.
	AttestationPropagationSlotRange uint64             `yaml:"ATTESTATION_PROPAGATION_SLOT_RANGE" json:"ATTESTATION_PROPAGATION_SLOT_RANGE,string"` // The maximum number of slots during which an attestation can be propagated.
	AttestationSubnetPrefixBits     uint64             `yaml:"ATTESTATION_SUBNET_PREFIX_BITS" json:"ATTESTATION_SUBNET_PREFIX_BITS,string"`         // The number of bits in the subnet prefix.
	MessageDomainInvalidSnappy      ConfigHex4Bytes    `yaml:"-" json:"MESSAGE_DOMAIN_INVALID_SNAPPY"`                                              // 4-byte domain for gossip message-id isolation of invalid snappy messages
	MessageDomainValidSnappy        ConfigHex4Bytes    `yaml:"-" json:"MESSAGE_DOMAIN_VALID_SNAPPY"`                                                // 4-byte domain for gossip message-id isolation of valid snappy messages
	MaximumGossipClockDisparity     ConfigDurationMSec `yaml:"-" json:"MAXIMUM_GOSSIP_CLOCK_DISPARITY_MILLIS"`                                      // The maximum milliseconds of clock disparity assumed between honest nodes.
	TtfbTimeout                     ConfigDurationSec  `yaml:"-" json:"TTFB_TIMEOUT"`                                                               // The maximum time to wait for first byte of request response (time-to-first-byte).
	RespTimeout                     ConfigDurationSec  `yaml:"-" json:"RESP_TIMEOUT"`                                                               // The maximum time for complete response transfer.

	// DiscoveryV5 Config
	Eth2key                    string `yaml:"-" json:"-"` // ETH2Key is the ENR key of the Ethereum consensus object in an enr.
	AttSubnetKey               string `yaml:"-" json:"-"` // AttSubnetKey is the ENR key of the subnet bitfield in the enr.
	SyncCommsSubnetKey         string `yaml:"-" json:"-"` // SyncCommsSubnetKey is the ENR key of the sync committee subnet bitfield in the enr.
	CgcKey                     string `yaml:"-" json:"-"` // CgcKey is the ENR key of the CGC in the enr.
	NfdKey                     string `yaml:"-" json:"-"` // NfdKey is the ENR key of the NFD in the enr.
	MinimumPeersInSubnetSearch uint64 `yaml:"-" json:"-"` // PeersInSubnetSearch is the required amount of peers that we need to be able to lookup in a subnet search.

	BootNodes   []string `yaml:"-" json:"-"`
	StaticPeers []string `yaml:"-" json:"-"`
}

var NetworkConfigs map[NetworkType]NetworkConfig = map[NetworkType]NetworkConfig{
	chainspec.MainnetChainID: {
		GossipMaxSize:                   10485760,
		GossipMaxSizeBellatrix:          15728640,
		MaxChunkSize:                    MaxChunkSize,
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		AttestationSubnetPrefixBits:     6,
		TtfbTimeout:                     ConfigDurationSec(ReqTimeout),
		RespTimeout:                     ConfigDurationSec(RespTimeout),
		MaximumGossipClockDisparity:     ConfigDurationMSec(500 * time.Millisecond),
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		CgcKey:                          "cgc",
		NfdKey:                          "nfd",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       MainnetBootstrapNodes,
	},

	chainspec.SepoliaChainID: {
		GossipMaxSize:                   10485760,
		GossipMaxSizeBellatrix:          15728640,
		MaxChunkSize:                    15728640,
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		AttestationSubnetPrefixBits:     6,
		TtfbTimeout:                     ConfigDurationSec(ReqTimeout),
		RespTimeout:                     ConfigDurationSec(RespTimeout),
		MaximumGossipClockDisparity:     ConfigDurationMSec(500 * time.Millisecond),
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		CgcKey:                          "cgc",
		NfdKey:                          "nfd",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       SepoliaBootstrapNodes,
	},

	chainspec.GnosisChainID: {
		GossipMaxSize:                   10485760,
		GossipMaxSizeBellatrix:          15728640,
		MaxChunkSize:                    15728640, // 15 MiB
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		AttestationSubnetPrefixBits:     6,
		TtfbTimeout:                     ConfigDurationSec(ReqTimeout),
		RespTimeout:                     ConfigDurationSec(RespTimeout),
		MaximumGossipClockDisparity:     ConfigDurationMSec(500 * time.Millisecond),
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		CgcKey:                          "cgc",
		NfdKey:                          "nfd",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       GnosisBootstrapNodes,
	},

	chainspec.ChiadoChainID: {
		GossipMaxSize:                   10485760,
		GossipMaxSizeBellatrix:          15728640,
		MaxChunkSize:                    15728640, // 15 MiB
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		AttestationSubnetPrefixBits:     6,
		TtfbTimeout:                     ConfigDurationSec(ReqTimeout),
		RespTimeout:                     ConfigDurationSec(RespTimeout),
		MaximumGossipClockDisparity:     ConfigDurationMSec(500 * time.Millisecond),
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		CgcKey:                          "cgc",
		NfdKey:                          "nfd",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       ChiadoBootstrapNodes,
	},

	chainspec.HoleskyChainID: {
		GossipMaxSize:                   10485760,
		GossipMaxSizeBellatrix:          15728640,
		MaxChunkSize:                    15728640, // 15 MiB
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		AttestationSubnetPrefixBits:     6,
		TtfbTimeout:                     ConfigDurationSec(ReqTimeout),
		RespTimeout:                     ConfigDurationSec(RespTimeout),
		MaximumGossipClockDisparity:     ConfigDurationMSec(500 * time.Millisecond),
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		CgcKey:                          "cgc",
		NfdKey:                          "nfd",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       HoleskyBootstrapNodes,
	},

	chainspec.HoodiChainID: {
		GossipMaxSize:                   10485760,
		GossipMaxSizeBellatrix:          15728640,
		MaxChunkSize:                    15728640, // 15 MiB
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		AttestationSubnetPrefixBits:     6,
		TtfbTimeout:                     ConfigDurationSec(ReqTimeout),
		RespTimeout:                     ConfigDurationSec(RespTimeout),
		MaximumGossipClockDisparity:     ConfigDurationMSec(500 * time.Millisecond),
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		CgcKey:                          "cgc",
		NfdKey:                          "nfd",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       HoodiBootstrapNodes,
	},
}

// Trusted checkpoint sync endpoints: https://eth-clients.github.io/checkpoint-sync-endpoints/
var CheckpointSyncEndpoints = map[NetworkType][]string{
	chainspec.MainnetChainID: {
		"https://sync.invis.tools/eth/v2/debug/beacon/states/finalized",
		"https://mainnet-checkpoint-sync.attestant.io/eth/v2/debug/beacon/states/finalized",
		//"https://mainnet.checkpoint.sigp.io/eth/v2/debug/beacon/states/finalized",
		"https://mainnet-checkpoint-sync.stakely.io/eth/v2/debug/beacon/states/finalized",
		"https://checkpointz.pietjepuk.net/eth/v2/debug/beacon/states/finalized",
	},
	chainspec.SepoliaChainID: {
		//"https://beaconstate-sepolia.chainsafe.io/eth/v2/debug/beacon/states/finalized",
		//"https://sepolia.beaconstate.info/eth/v2/debug/beacon/states/finalized",
		"https://checkpoint-sync.sepolia.ethpandaops.io/eth/v2/debug/beacon/states/finalized",
	},
	chainspec.GnosisChainID: {
		//"https://checkpoint.gnosis.gateway.fm/eth/v2/debug/beacon/states/finalized",
		"https://checkpoint.gnosischain.com/eth/v2/debug/beacon/states/finalized",
	},
	chainspec.ChiadoChainID: {
		"https://checkpoint.chiadochain.net/eth/v2/debug/beacon/states/finalized",
	},
	chainspec.HoleskyChainID: {
		"https://beaconstate-holesky.chainsafe.io/eth/v2/debug/beacon/states/finalized",
		"https://holesky.beaconstate.info/eth/v2/debug/beacon/states/finalized",
		"https://checkpoint-sync.holesky.ethpandaops.io/eth/v2/debug/beacon/states/finalized",
	},
	chainspec.HoodiChainID: {
		"https://checkpoint-sync.hoodi.ethpandaops.io/eth/v2/debug/beacon/states/finalized",
		"https://hoodi-checkpoint-sync.attestant.io/eth/v2/debug/beacon/states/finalized",
	},
}

// ConfigurableCheckpointsURLs is customized by the user to specify the checkpoint sync endpoints.
var ConfigurableCheckpointsURLs = []string{}

// MinEpochsForBlockRequests  equal to MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT / 2
func (b *BeaconChainConfig) MinEpochsForBlockRequests() uint64 {
	return b.MinValidatorWithdrawabilityDelay + (b.ChurnLimitQuotient)/2
}

// MinSlotsForBlobRequests  equal to MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS * SLOTS_PER_EPOCH
func (b *BeaconChainConfig) MinSlotsForBlobsSidecarsRequest() uint64 {
	return b.MinEpochsForBlobSidecarsRequests * b.SlotsPerEpoch
}

type ConfigDurationSec time.Duration

func (d *ConfigDurationSec) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, "\"%d\"", int64(time.Duration(*d).Seconds())), nil
}

type ConfigDurationMSec time.Duration

func (d *ConfigDurationMSec) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, "\"%d\"", time.Duration(*d).Milliseconds()), nil
}

type ConfigByte byte

func (b ConfigByte) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, "\"0x%02x\"", b), nil
}

type ConfigForkVersion uint32

func (v ConfigForkVersion) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, "\"0x%08x\"", v), nil
}

type VersionScheduleEntry struct {
	Epoch        uint64 `yaml:"EPOCH" json:"EPOCH,string"`
	StateVersion StateVersion
}

type ConfigHex4Bytes [4]byte

func (b ConfigHex4Bytes) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"0x%s"`, hex.EncodeToString(b[:])), nil
}

type BlobParameters struct {
	Epoch            uint64 `yaml:"EPOCH" json:"EPOCH,string"`
	MaxBlobsPerBlock uint64 `yaml:"MAX_BLOBS_PER_BLOCK" json:"MAX_BLOBS_PER_BLOCK,string"`
}

// BeaconChainConfig contains constant configs for node to participate in beacon chain.
type BeaconChainConfig struct {
	// Constants (non-configurable)
	GenesisSlot              uint64 `yaml:"GENESIS_SLOT" json:"GENESIS_SLOT,string"`                               // GenesisSlot represents the first canonical slot number of the beacon chain.
	GenesisEpoch             uint64 `yaml:"GENESIS_EPOCH" json:"GENESIS_EPOCH,string"`                             // GenesisEpoch represents the first canonical epoch number of the beacon chain.
	FarFutureEpoch           uint64 `yaml:"FAR_FUTURE_EPOCH" json:"FAR_FUTURE_EPOCH,string"`                       // FarFutureEpoch represents a epoch extremely far away in the future used as the default penalization epoch for validators.
	FarFutureSlot            uint64 `yaml:"FAR_FUTURE_SLOT" json:"FAR_FUTURE_SLOT,string"`                         // FarFutureSlot represents a slot extremely far away in the future.
	BaseRewardsPerEpoch      uint64 `yaml:"BASE_REWARDS_PER_EPOCH" json:"BASE_REWARDS_PER_EPOCH,string"`           // BaseRewardsPerEpoch is used to calculate the per epoch rewards.
	DepositContractTreeDepth uint64 `yaml:"DEPOSIT_CONTRACT_TREE_DEPTH" json:"DEPOSIT_CONTRACT_TREE_DEPTH,string"` // DepositContractTreeDepth depth of the Merkle trie of deposits in the validator deposit contract on the PoW chain.
	JustificationBitsLength  uint64 `yaml:"JUSTIFICATION_BITS_LENGTH" json:"JUSTIFICATION_BITS_LENGTH,string"`     // JustificationBitsLength defines number of epochs to track when implementing k-finality in Casper FFG.

	// Misc constants.
	PresetBase                       string     `yaml:"PRESET_BASE" spec:"true" json:"PRESET_BASE"`                                                            // PresetBase represents the underlying spec preset this config is based on.
	ConfigName                       string     `yaml:"CONFIG_NAME" spec:"true" json:"CONFIG_NAME"`                                                            // ConfigName for allowing an easy human-readable way of knowing what chain is being used.
	TargetCommitteeSize              uint64     `yaml:"TARGET_COMMITTEE_SIZE" spec:"true" json:"TARGET_COMMITTEE_SIZE,string"`                                 // TargetCommitteeSize is the number of validators in a committee when the chain is healthy.
	MaxValidatorsPerCommittee        uint64     `yaml:"MAX_VALIDATORS_PER_COMMITTEE" spec:"true" json:"MAX_VALIDATORS_PER_COMMITTEE,string"`                   // MaxValidatorsPerCommittee defines the upper bound of the size of a committee.
	MaxCommitteesPerSlot             uint64     `yaml:"MAX_COMMITTEES_PER_SLOT" spec:"true" json:"MAX_COMMITTEES_PER_SLOT,string"`                             // MaxCommitteesPerSlot defines the max amount of committee in a single slot.
	MinPerEpochChurnLimit            uint64     `yaml:"MIN_PER_EPOCH_CHURN_LIMIT" spec:"true" json:"MIN_PER_EPOCH_CHURN_LIMIT,string"`                         // MinPerEpochChurnLimit is the minimum amount of churn allotted for validator rotations.
	ChurnLimitQuotient               uint64     `yaml:"CHURN_LIMIT_QUOTIENT" spec:"true" json:"CHURN_LIMIT_QUOTIENT,string"`                                   // ChurnLimitQuotient is used to determine the limit of how many validators can rotate per epoch.
	MaxPerEpochActivationChurnLimit  uint64     `yaml:"MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT" spec:"true" json:"MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT,string"`   // MaxPerEpochActivationChurnLimit defines the maximum amount of churn allowed in one epoch from deneb.
	ShuffleRoundCount                uint64     `yaml:"SHUFFLE_ROUND_COUNT" spec:"true" json:"SHUFFLE_ROUND_COUNT,string"`                                     // ShuffleRoundCount is used for retrieving the permuted index.
	MinGenesisActiveValidatorCount   uint64     `yaml:"MIN_GENESIS_ACTIVE_VALIDATOR_COUNT" spec:"true" json:"MIN_GENESIS_ACTIVE_VALIDATOR_COUNT,string"`       // MinGenesisActiveValidatorCount defines how many validator deposits needed to kick off beacon chain.
	MinGenesisTime                   uint64     `yaml:"MIN_GENESIS_TIME" spec:"true" json:"MIN_GENESIS_TIME,string"`                                           // MinGenesisTime is the time that needed to pass before kicking off beacon chain.
	TargetAggregatorsPerCommittee    uint64     `yaml:"TARGET_AGGREGATORS_PER_COMMITTEE" spec:"true" json:"TARGET_AGGREGATORS_PER_COMMITTEE,string"`           // TargetAggregatorsPerCommittee defines the number of aggregators inside one committee.
	HysteresisQuotient               uint64     `yaml:"HYSTERESIS_QUOTIENT" spec:"true" json:"HYSTERESIS_QUOTIENT,string"`                                     // HysteresisQuotient defines the hysteresis quotient for effective balance calculations.
	HysteresisDownwardMultiplier     uint64     `yaml:"HYSTERESIS_DOWNWARD_MULTIPLIER" spec:"true" json:"HYSTERESIS_DOWNWARD_MULTIPLIER,string"`               // HysteresisDownwardMultiplier defines the hysteresis downward multiplier for effective balance calculations.
	HysteresisUpwardMultiplier       uint64     `yaml:"HYSTERESIS_UPWARD_MULTIPLIER" spec:"true" json:"HYSTERESIS_UPWARD_MULTIPLIER,string"`                   // HysteresisUpwardMultiplier defines the hysteresis upward multiplier for effective balance calculations.
	MinEpochsForBlobSidecarsRequests uint64     `yaml:"MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS" spec:"true" json:"MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS,string"` // MinEpochsForBlobSidecarsRequests defines the minimum number of epochs to wait before requesting blobs sidecars.
	FieldElementsPerBlob             uint64     `yaml:"FIELD_ELEMENTS_PER_BLOB" spec:"true" json:"FIELD_ELEMENTS_PER_BLOB,string"`                             // FieldElementsPerBlob defines the number of field elements per blob.
	MaxBytesPerTransaction           uint64     `yaml:"MAX_BYTES_PER_TRANSACTION" spec:"true" json:"MAX_BYTES_PER_TRANSACTION,string"`                         // MaxBytesPerTransaction defines the maximum number of bytes per transaction.
	MaxExtraDataBytes                uint64     `yaml:"MAX_EXTRA_DATA_BYTES" spec:"true" json:"MAX_EXTRA_DATA_BYTES,string"`                                   // MaxExtraDataBytes defines the maximum number of bytes in the extra data field.
	MaxRequestBlobSidecars           uint64     `yaml:"MAX_REQUEST_BLOB_SIDECARS" spec:"true" json:"MAX_REQUEST_BLOB_SIDECARS,string"`                         // MaxRequestBlobSidecars defines the maximum number of blob sidecars to request.
	MaxRequestBlobSidecarsElectra    uint64     `yaml:"MAX_REQUEST_BLOB_SIDECARS_ELECTRA" spec:"true" json:"MAX_REQUEST_BLOB_SIDECARS_ELECTRA,string"`         // MaxRequestBlobSidecarsElectra defines the maximum number of blob sidecars to request in Electra.
	MaxRequestBlocks                 uint64     `yaml:"MAX_REQUEST_BLOCKS" spec:"true" json:"MAX_REQUEST_BLOCKS,string"`                                       // Maximum number of blocks in a single request
	MaxRequestBlocksDeneb            uint64     `yaml:"MAX_REQUEST_BLOCKS_DENEB" spec:"true" json:"MAX_REQUEST_BLOCKS_DENEB,string"`                           // Maximum number of blocks in a single request
	MaxTransactionsPerPayload        uint64     `yaml:"MAX_TRANSACTIONS_PER_PAYLOAD" spec:"true" json:"MAX_TRANSACTIONS_PER_PAYLOAD,string"`                   // MaxTransactionsPerPayload defines the maximum number of transactions in a single payload.
	SubnetsPerNode                   uint64     `yaml:"SUBNETS_PER_NODE" spec:"true" json:"SUBNETS_PER_NODE,string"`                                           // SubnetsPerNode defines the number of subnets a node can subscribe to.
	VersionedHashVersionKzg          ConfigByte `yaml:"VERSIONED_HASH_VERSION_KZG" spec:"true" json:"VERSIONED_HASH_VERSION_KZG"`                              // VersionedHashVersionKzg is the version of the versioned hash used in KZG commitments.

	// Gwei value constants.
	MinDepositAmount           uint64 `yaml:"MIN_DEPOSIT_AMOUNT" spec:"true" json:"MIN_DEPOSIT_AMOUNT,string"`                       // MinDepositAmount is the minimum amount of Gwei a validator can send to the deposit contract at once (lower amounts will be reverted).
	MaxEffectiveBalance        uint64 `yaml:"MAX_EFFECTIVE_BALANCE" spec:"true" json:"MAX_EFFECTIVE_BALANCE,string"`                 // MaxEffectiveBalance is the maximal amount of Gwei that is effective for staking.
	MaxEffectiveBalanceElectra uint64 `yaml:"MAX_EFFECTIVE_BALANCE_ELECTRA" spec:"true" json:"MAX_EFFECTIVE_BALANCE_ELECTRA,string"` // MaxEffectiveBalanceElectra is the maximal amount of Gwei that is effective for staking in Electra.
	MinActivationBalance       uint64 `yaml:"MIN_ACTIVATION_BALANCE" spec:"true" json:"MIN_ACTIVATION_BALANCE,string"`               // MinActivationBalance is the minimal GWei a validator needs to have before activated.
	EjectionBalance            uint64 `yaml:"EJECTION_BALANCE" spec:"true" json:"EJECTION_BALANCE,string"`                           // EjectionBalance is the minimal GWei a validator needs to have before ejected.
	EffectiveBalanceIncrement  uint64 `yaml:"EFFECTIVE_BALANCE_INCREMENT" spec:"true" json:"EFFECTIVE_BALANCE_INCREMENT,string"`     // EffectiveBalanceIncrement is used for converting the high balance into the low balance for validators.

	// Initial value constants.
	BLSWithdrawalPrefixByte         ConfigByte `yaml:"BLS_WITHDRAWAL_PREFIX" spec:"true" json:"BLS_WITHDRAWAL_PREFIX"`                   // BLSWithdrawalPrefixByte is used for BLS withdrawal and it's the first byte.
	ETH1AddressWithdrawalPrefixByte ConfigByte `yaml:"ETH1_ADDRESS_WITHDRAWAL_PREFIX" spec:"true" json:"ETH1_ADDRESS_WITHDRAWAL_PREFIX"` // ETH1_ADDRESS_WITHDRAWAL_PREFIX is used for withdrawals and it's the first byte.

	// Time parameters constants.
	GenesisDelay                              uint64 `yaml:"GENESIS_DELAY" spec:"true" json:"GENESIS_DELAY,string"`                                             // GenesisDelay is the minimum number of seconds to delay starting the Ethereum Beacon Chain genesis. Must be at least 1 second.
	MinAttestationInclusionDelay              uint64 `yaml:"MIN_ATTESTATION_INCLUSION_DELAY" spec:"true" json:"MIN_ATTESTATION_INCLUSION_DELAY,string"`         // MinAttestationInclusionDelay defines how many slots validator has to wait to include attestation for beacon block.
	SecondsPerSlot                            uint64 `yaml:"SECONDS_PER_SLOT" spec:"true" json:"SECONDS_PER_SLOT,string"`                                       // SecondsPerSlot is how many seconds are in a single slot.
	SlotsPerEpoch                             uint64 `yaml:"SLOTS_PER_EPOCH" spec:"true" json:"SLOTS_PER_EPOCH,string"`                                         // SlotsPerEpoch is the number of slots in an epoch.
	MinSeedLookahead                          uint64 `yaml:"MIN_SEED_LOOKAHEAD" spec:"true" json:"MIN_SEED_LOOKAHEAD,string"`                                   // MinSeedLookahead is the duration of randao look ahead seed.
	MaxSeedLookahead                          uint64 `yaml:"MAX_SEED_LOOKAHEAD" spec:"true" json:"MAX_SEED_LOOKAHEAD,string"`                                   // MaxSeedLookahead is the duration a validator has to wait for entry and exit in epoch.
	EpochsPerEth1VotingPeriod                 uint64 `yaml:"EPOCHS_PER_ETH1_VOTING_PERIOD" spec:"true" json:"EPOCHS_PER_ETH1_VOTING_PERIOD,string"`             // EpochsPerEth1VotingPeriod defines how often the merkle root of deposit receipts get updated in beacon node on per epoch basis.
	SlotsPerHistoricalRoot                    uint64 `yaml:"SLOTS_PER_HISTORICAL_ROOT" spec:"true" json:"SLOTS_PER_HISTORICAL_ROOT,string"`                     // SlotsPerHistoricalRoot defines how often the historical root is saved.
	MinValidatorWithdrawabilityDelay          uint64 `yaml:"MIN_VALIDATOR_WITHDRAWABILITY_DELAY" spec:"true" json:"MIN_VALIDATOR_WITHDRAWABILITY_DELAY,string"` // MinValidatorWithdrawabilityDelay is the shortest amount of time a validator has to wait to withdraw.
	ShardCommitteePeriod                      uint64 `yaml:"SHARD_COMMITTEE_PERIOD" spec:"true" json:"SHARD_COMMITTEE_PERIOD,string"`                           // ShardCommitteePeriod is the minimum amount of epochs a validator must participate before exiting.
	MinEpochsToInactivityPenalty              uint64 `yaml:"MIN_EPOCHS_TO_INACTIVITY_PENALTY" spec:"true" json:"MIN_EPOCHS_TO_INACTIVITY_PENALTY,string"`       // MinEpochsToInactivityPenalty defines the minimum amount of epochs since finality to begin penalizing inactivity.
	Eth1FollowDistance                        uint64 `yaml:"ETH1_FOLLOW_DISTANCE" spec:"true" json:"ETH1_FOLLOW_DISTANCE,string"`                               // Eth1FollowDistance is the number of eth1.0 blocks to wait before considering a new deposit for voting. This only applies after the chain as been started.
	SafeSlotsToUpdateJustified                uint64 `yaml:"SAFE_SLOTS_TO_UPDATE_JUSTIFIED" spec:"true" json:"SAFE_SLOTS_TO_UPDATE_JUSTIFIED,string"`           // SafeSlotsToUpdateJustified is the minimal slots needed to update justified check point.
	DeprecatedSafeSlotsToImportOptimistically uint64 `yaml:"SAFE_SLOTS_TO_IMPORT_OPTIMISTICALLY" spec:"true" json:"SAFE_SLOTS_TO_IMPORT_OPTIMISTICALLY,string"` // SafeSlotsToImportOptimistically is the minimal number of slots to wait before importing optimistically a pre-merge block
	SecondsPerETH1Block                       uint64 `yaml:"SECONDS_PER_ETH1_BLOCK" spec:"true" json:"SECONDS_PER_ETH1_BLOCK,string"`                           // SecondsPerETH1Block is the approximate time for a single eth1 block to be produced.

	// Fork choice algorithm constants.
	ProposerScoreBoost uint64 `yaml:"PROPOSER_SCORE_BOOST" spec:"true" json:"PROPOSER_SCORE_BOOST,string"` // ProposerScoreBoost defines a value that is a % of the committee weight for fork-choice boosting.
	IntervalsPerSlot   uint64 `yaml:"INTERVALS_PER_SLOT" spec:"true" json:"INTERVALS_PER_SLOT,string"`     // IntervalsPerSlot defines the number of fork choice intervals in a slot defined in the fork choice spec.

	// Ethereum PoW parameters.
	DepositChainID         uint64 `yaml:"DEPOSIT_CHAIN_ID" spec:"true" json:"DEPOSIT_CHAIN_ID,string"`          // DepositChainID of the eth1 network. This used for replay protection.
	DepositNetworkID       uint64 `yaml:"DEPOSIT_NETWORK_ID" spec:"true" json:"DEPOSIT_NETWORK_ID,string"`      // DepositNetworkID of the eth1 network. This used for replay protection.
	DepositContractAddress string `yaml:"DEPOSIT_CONTRACT_ADDRESS" spec:"true" json:"DEPOSIT_CONTRACT_ADDRESS"` // DepositContractAddress is the address of the deposit contract.

	// Validator parameters.
	RandomSubnetsPerValidator         uint64 `yaml:"RANDOM_SUBNETS_PER_VALIDATOR" spec:"true" json:"RANDOM_SUBNETS_PER_VALIDATOR,string"`                   // RandomSubnetsPerValidator specifies the amount of subnets a validator has to be subscribed to at one time.
	EpochsPerRandomSubnetSubscription uint64 `yaml:"EPOCHS_PER_RANDOM_SUBNET_SUBSCRIPTION" spec:"true" json:"EPOCHS_PER_RANDOM_SUBNET_SUBSCRIPTION,string"` // EpochsPerRandomSubnetSubscription specifies the minimum duration a validator is connected to their subnet.

	// State list lengths
	EpochsPerHistoricalVector uint64 `yaml:"EPOCHS_PER_HISTORICAL_VECTOR" spec:"true" json:"EPOCHS_PER_HISTORICAL_VECTOR,string"` // EpochsPerHistoricalVector defines max length in epoch to store old historical stats in beacon state.
	EpochsPerSlashingsVector  uint64 `yaml:"EPOCHS_PER_SLASHINGS_VECTOR" spec:"true" json:"EPOCHS_PER_SLASHINGS_VECTOR,string"`   // EpochsPerSlashingsVector defines max length in epoch to store old stats to recompute slashing witness.
	HistoricalRootsLimit      uint64 `yaml:"HISTORICAL_ROOTS_LIMIT" spec:"true" json:"HISTORICAL_ROOTS_LIMIT,string"`             // HistoricalRootsLimit defines max historical roots that can be saved in state before roll over.
	ValidatorRegistryLimit    uint64 `yaml:"VALIDATOR_REGISTRY_LIMIT" spec:"true" json:"VALIDATOR_REGISTRY_LIMIT,string"`         // ValidatorRegistryLimit defines the upper bound of validators can participate in eth2.

	// Reward and penalty quotients constants.
	BaseRewardFactor               uint64 `yaml:"BASE_REWARD_FACTOR" spec:"true" json:"BASE_REWARD_FACTOR,string"`                             // BaseRewardFactor is used to calculate validator per-slot interest rate.
	WhistleBlowerRewardQuotient    uint64 `yaml:"WHISTLEBLOWER_REWARD_QUOTIENT" spec:"true" json:"WHISTLEBLOWER_REWARD_QUOTIENT,string"`       // WhistleBlowerRewardQuotient is used to calculate whistle blower reward.
	ProposerRewardQuotient         uint64 `yaml:"PROPOSER_REWARD_QUOTIENT" spec:"true" json:"PROPOSER_REWARD_QUOTIENT,string"`                 // ProposerRewardQuotient is used to calculate the reward for proposers.
	InactivityPenaltyQuotient      uint64 `yaml:"INACTIVITY_PENALTY_QUOTIENT" spec:"true" json:"INACTIVITY_PENALTY_QUOTIENT,string"`           // InactivityPenaltyQuotient is used to calculate the penalty for a validator that is offline.
	MinSlashingPenaltyQuotient     uint64 `yaml:"MIN_SLASHING_PENALTY_QUOTIENT" spec:"true" json:"MIN_SLASHING_PENALTY_QUOTIENT,string"`       // MinSlashingPenaltyQuotient is used to calculate the minimum penalty to prevent DoS attacks.
	ProportionalSlashingMultiplier uint64 `yaml:"PROPORTIONAL_SLASHING_MULTIPLIER" spec:"true" json:"PROPORTIONAL_SLASHING_MULTIPLIER,string"` // ProportionalSlashingMultiplier is used as a multiplier on slashed penalties.

	// Max operations per block constants.
	MaxProposerSlashings             uint64 `yaml:"MAX_PROPOSER_SLASHINGS" spec:"true" json:"MAX_PROPOSER_SLASHINGS,string"`                             // MaxProposerSlashings defines the maximum number of slashings of proposers possible in a block.
	MaxAttesterSlashings             uint64 `yaml:"MAX_ATTESTER_SLASHINGS" spec:"true" json:"MAX_ATTESTER_SLASHINGS,string"`                             // MaxAttesterSlashings defines the maximum number of casper FFG slashings possible in a block.
	MaxAttesterSlashingsElectra      uint64 `yaml:"MAX_ATTESTER_SLASHINGS_ELECTRA" spec:"true" json:"MAX_ATTESTER_SLASHINGS_ELECTRA,string"`             // MaxAttesterSlashingsElectra defines the maximum number of casper FFG slashings possible in a block for Electra.
	MaxAttestations                  uint64 `yaml:"MAX_ATTESTATIONS" spec:"true" json:"MAX_ATTESTATIONS,string"`                                         // MaxAttestations defines the maximum allowed attestations in a beacon block.
	MaxAttestationsElectra           uint64 `yaml:"MAX_ATTESTATIONS_ELECTRA" spec:"true" json:"MAX_ATTESTATIONS_ELECTRA,string"`                         // MaxAttestationsElectra defines the maximum allowed attestations in a beacon block for Electra.
	MaxDeposits                      uint64 `yaml:"MAX_DEPOSITS" spec:"true" json:"MAX_DEPOSITS,string"`                                                 // MaxDeposits defines the maximum number of validator deposits in a block.
	MaxVoluntaryExits                uint64 `yaml:"MAX_VOLUNTARY_EXITS" spec:"true" json:"MAX_VOLUNTARY_EXITS,string"`                                   // MaxVoluntaryExits defines the maximum number of validator exits in a block.
	MaxWithdrawalsPerPayload         uint64 `yaml:"MAX_WITHDRAWALS_PER_PAYLOAD" spec:"true" json:"MAX_WITHDRAWALS_PER_PAYLOAD,string"`                   // MaxWithdrawalsPerPayload defines the maximum number of withdrawals in a block.
	MaxBlsToExecutionChanges         uint64 `yaml:"MAX_BLS_TO_EXECUTION_CHANGES" spec:"true" json:"MAX_BLS_TO_EXECUTION_CHANGES,string"`                 // MaxBlsToExecutionChanges defines the maximum number of BLS-to-execution-change objects in a block.
	MaxValidatorsPerWithdrawalsSweep uint64 `yaml:"MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP" spec:"true" json:"MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP,string"` //MaxValidatorsPerWithdrawalsSweep bounds the size of the sweep searching for withdrawals per slot.
	MaxBlobCommittmentsPerBlock      uint64 `yaml:"MAX_BLOB_COMMITMENTS_PER_BLOCK" spec:"true" json:"MAX_BLOB_COMMITMENTS_PER_BLOCK,string"`             // MaxBlobsCommittmentsPerBlock defines the maximum number of blobs commitments in a block.
	// BLS domain values.
	DomainBeaconProposer              common.Bytes4 `yaml:"DOMAIN_BEACON_PROPOSER" spec:"true" json:"DOMAIN_BEACON_PROPOSER"`                               // DomainBeaconProposer defines the BLS signature domain for beacon proposal verification.
	DomainRandao                      common.Bytes4 `yaml:"DOMAIN_RANDAO" spec:"true" json:"DOMAIN_RANDAO"`                                                 // DomainRandao defines the BLS signature domain for randao verification.
	DomainBeaconAttester              common.Bytes4 `yaml:"DOMAIN_BEACON_ATTESTER" spec:"true" json:"DOMAIN_BEACON_ATTESTER"`                               // DomainBeaconAttester defines the BLS signature domain for attestation verification.
	DomainDeposit                     common.Bytes4 `yaml:"DOMAIN_DEPOSIT" spec:"true" json:"DOMAIN_DEPOSIT"`                                               // DomainDeposit defines the BLS signature domain for deposit verification.
	DomainVoluntaryExit               common.Bytes4 `yaml:"DOMAIN_VOLUNTARY_EXIT" spec:"true" json:"DOMAIN_VOLUNTARY_EXIT"`                                 // DomainVoluntaryExit defines the BLS signature domain for exit verification.
	DomainSelectionProof              common.Bytes4 `yaml:"DOMAIN_SELECTION_PROOF" spec:"true" json:"DOMAIN_SELECTION_PROOF"`                               // DomainSelectionProof defines the BLS signature domain for selection proof.
	DomainAggregateAndProof           common.Bytes4 `yaml:"DOMAIN_AGGREGATE_AND_PROOF" spec:"true" json:"DOMAIN_AGGREGATE_AND_PROOF"`                       // DomainAggregateAndProof defines the BLS signature domain for aggregate and proof.
	DomainSyncCommittee               common.Bytes4 `yaml:"DOMAIN_SYNC_COMMITTEE" spec:"true" json:"DOMAIN_SYNC_COMMITTEE"`                                 // DomainVoluntaryExit defines the BLS signature domain for sync committee.
	DomainSyncCommitteeSelectionProof common.Bytes4 `yaml:"DOMAIN_SYNC_COMMITTEE_SELECTION_PROOF" spec:"true" json:"DOMAIN_SYNC_COMMITTEE_SELECTION_PROOF"` // DomainSelectionProof defines the BLS signature domain for sync committee selection proof.
	DomainContributionAndProof        common.Bytes4 `yaml:"DOMAIN_CONTRIBUTION_AND_PROOF" spec:"true" json:"DOMAIN_CONTRIBUTION_AND_PROOF"`                 // DomainAggregateAndProof defines the BLS signature domain for contribution and proof.
	DomainApplicationMask             common.Bytes4 `yaml:"DOMAIN_APPLICATION_MASK" spec:"true" json:"DOMAIN_APPLICATION_MASK"`                             // DomainApplicationMask defines the BLS signature domain for application mask.
	DomainApplicationBuilder          common.Bytes4 `json:"-"`                                                                                              // DomainApplicationBuilder defines the BLS signature domain for application builder.
	DomainBLSToExecutionChange        common.Bytes4 `json:"-"`                                                                                              // DomainBLSToExecutionChange defines the BLS signature domain to change withdrawal addresses to ETH1 prefix
	DomainBlobSideCar                 common.Bytes4 `yaml:"DOMAIN_BLOB_SIDECAR" spec:"true" json:"DOMAIN_BLOB_SIDECAR"`                                     // DomainBlobSideCar defines the BLS signature domain for blob sidecar verification

	// Slasher constants.
	PruneSlasherStoragePeriod uint64 `json:"-"` // PruneSlasherStoragePeriod defines the time period expressed in number of epochs were proof of stake network should prune attestation and block header store.

	// Slashing protection constants.
	SlashingProtectionPruningEpochs uint64 `json:"-"` // SlashingProtectionPruningEpochs defines a period after which all prior epochs are pruned in the validator database.

	// Fork-related values.
	GenesisForkVersion   ConfigForkVersion `yaml:"GENESIS_FORK_VERSION" spec:"true" json:"GENESIS_FORK_VERSION"`        // GenesisForkVersion is used to track fork version between state transitions.
	AltairForkVersion    ConfigForkVersion `yaml:"ALTAIR_FORK_VERSION" spec:"true" json:"ALTAIR_FORK_VERSION"`          // AltairForkVersion is used to represent the fork version for Altair.
	AltairForkEpoch      uint64            `yaml:"ALTAIR_FORK_EPOCH" spec:"true" json:"ALTAIR_FORK_EPOCH,string"`       // AltairForkEpoch is used to represent the assigned fork epoch for Altair.
	BellatrixForkVersion ConfigForkVersion `yaml:"BELLATRIX_FORK_VERSION" spec:"true" json:"BELLATRIX_FORK_VERSION"`    // BellatrixForkVersion is used to represent the fork version for Bellatrix.
	BellatrixForkEpoch   uint64            `yaml:"BELLATRIX_FORK_EPOCH" spec:"true" json:"BELLATRIX_FORK_EPOCH,string"` // BellatrixForkEpoch is used to represent the assigned fork epoch for Bellatrix.
	CapellaForkVersion   ConfigForkVersion `yaml:"CAPELLA_FORK_VERSION" spec:"true" json:"CAPELLA_FORK_VERSION"`        // CapellaForkVersion is used to represent the fork version for Capella.
	CapellaForkEpoch     uint64            `yaml:"CAPELLA_FORK_EPOCH" spec:"true" json:"CAPELLA_FORK_EPOCH,string"`     // CapellaForkEpoch is used to represent the assigned fork epoch for Capella.
	DenebForkVersion     ConfigForkVersion `yaml:"DENEB_FORK_VERSION" spec:"true" json:"DENEB_FORK_VERSION"`            // DenebForkVersion is used to represent the fork version for Deneb.
	DenebForkEpoch       uint64            `yaml:"DENEB_FORK_EPOCH" spec:"true" json:"DENEB_FORK_EPOCH,string"`         // DenebForkEpoch is used to represent the assigned fork epoch for Deneb.
	ElectraForkVersion   ConfigForkVersion `yaml:"ELECTRA_FORK_VERSION" spec:"true" json:"ELECTRA_FORK_VERSION"`        // ElectraForkVersion is used to represent the fork version for Electra.
	ElectraForkEpoch     uint64            `yaml:"ELECTRA_FORK_EPOCH" spec:"true" json:"ELECTRA_FORK_EPOCH,string"`     // ElectraForkEpoch is used to represent the assigned fork epoch for Electra.
	FuluForkVersion      ConfigForkVersion `yaml:"FULU_FORK_VERSION" spec:"true" json:"FULU_FORK_VERSION"`              // FuluForkVersion is used to represent the fork version for Fulu.
	FuluForkEpoch        uint64            `yaml:"FULU_FORK_EPOCH" spec:"true" json:"FULU_FORK_EPOCH,string"`           // FuluForkEpoch is used to represent the assigned fork epoch for Fulu.

	ForkVersionSchedule map[common.Bytes4]VersionScheduleEntry `json:"-"` // Schedule of fork epochs by version.

	// New values introduced in Altair hard fork 1.
	// Participation flag indices.
	TimelySourceFlagIndex uint8 `yaml:"TIMELY_SOURCE_FLAG_INDEX" spec:"true" json:"TIMELY_SOURCE_FLAG_INDEX,string"` // TimelySourceFlagIndex is the source flag position of the participation bits.
	TimelyTargetFlagIndex uint8 `yaml:"TIMELY_TARGET_FLAG_INDEX" spec:"true" json:"TIMELY_TARGET_FLAG_INDEX,string"` // TimelyTargetFlagIndex is the target flag position of the participation bits.
	TimelyHeadFlagIndex   uint8 `yaml:"TIMELY_HEAD_FLAG_INDEX" spec:"true" json:"TIMELY_HEAD_FLAG_INDEX,string"`     // TimelyHeadFlagIndex is the head flag position of the participation bits.

	// Incentivization weights.
	TimelySourceWeight uint64 `yaml:"TIMELY_SOURCE_WEIGHT" spec:"true" json:"TIMELY_SOURCE_WEIGHT,string"` // TimelySourceWeight is the factor of how much source rewards receives.
	TimelyTargetWeight uint64 `yaml:"TIMELY_TARGET_WEIGHT" spec:"true" json:"TIMELY_TARGET_WEIGHT,string"` // TimelyTargetWeight is the factor of how much target rewards receives.
	TimelyHeadWeight   uint64 `yaml:"TIMELY_HEAD_WEIGHT" spec:"true" json:"TIMELY_HEAD_WEIGHT,string"`     // TimelyHeadWeight is the factor of how much head rewards receives.
	SyncRewardWeight   uint64 `yaml:"SYNC_REWARD_WEIGHT" spec:"true" json:"SYNC_REWARD_WEIGHT,string"`     // SyncRewardWeight is the factor of how much sync committee rewards receives.
	WeightDenominator  uint64 `yaml:"WEIGHT_DENOMINATOR" spec:"true" json:"WEIGHT_DENOMINATOR,string"`     // WeightDenominator accounts for total rewards denomination.
	ProposerWeight     uint64 `yaml:"PROPOSER_WEIGHT" spec:"true" json:"PROPOSER_WEIGHT,string"`           // ProposerWeight is the factor of how much proposer rewards receives.

	// Validator related.
	TargetAggregatorsPerSyncSubcommittee uint64 `yaml:"TARGET_AGGREGATORS_PER_SYNC_SUBCOMMITTEE" spec:"true" json:"TARGET_AGGREGATORS_PER_SYNC_SUBCOMMITTEE,string"` // TargetAggregatorsPerSyncSubcommittee for aggregating in sync committee.
	SyncCommitteeSubnetCount             uint64 `yaml:"SYNC_COMMITTEE_SUBNET_COUNT" spec:"true" json:"SYNC_COMMITTEE_SUBNET_COUNT,string"`                           // SyncCommitteeSubnetCount for sync committee subnet count.

	// Misc.
	SyncCommitteeSize            uint64 `yaml:"SYNC_COMMITTEE_SIZE" spec:"true" json:"SYNC_COMMITTEE_SIZE,string"`                           // SyncCommitteeSize for light client sync committee size.
	InactivityScoreBias          uint64 `yaml:"INACTIVITY_SCORE_BIAS" spec:"true" json:"INACTIVITY_SCORE_BIAS,string"`                       // InactivityScoreBias for calculating score bias penalties during inactivity
	InactivityScoreRecoveryRate  uint64 `yaml:"INACTIVITY_SCORE_RECOVERY_RATE" spec:"true" json:"INACTIVITY_SCORE_RECOVERY_RATE,string"`     // InactivityScoreRecoveryRate for recovering score bias penalties during inactivity.
	EpochsPerSyncCommitteePeriod uint64 `yaml:"EPOCHS_PER_SYNC_COMMITTEE_PERIOD" spec:"true" json:"EPOCHS_PER_SYNC_COMMITTEE_PERIOD,string"` // EpochsPerSyncCommitteePeriod defines how many epochs per sync committee period.
	BytesPerLogsBloom            uint64 `yaml:"BYTES_PER_LOGS_BLOOM" spec:"true" json:"BYTES_PER_LOGS_BLOOM,string"`                         // BytesPerLogsBloom defines the number of bytes in the logs bloom filter.

	// Updated penalty values. This moves penalty parameters toward their final, maximum security values.
	// Note: We do not override previous configuration values but instead creates new values and replaces usage throughout.
	InactivityPenaltyQuotientAltair         uint64 `yaml:"INACTIVITY_PENALTY_QUOTIENT_ALTAIR" spec:"true" json:"INACTIVITY_PENALTY_QUOTIENT_ALTAIR,string"`                 // InactivityPenaltyQuotientAltair for penalties during inactivity post Altair hard fork.
	MinSlashingPenaltyQuotientAltair        uint64 `yaml:"MIN_SLASHING_PENALTY_QUOTIENT_ALTAIR" spec:"true" json:"MIN_SLASHING_PENALTY_QUOTIENT_ALTAIR,string"`             // MinSlashingPenaltyQuotientAltair for slashing penalties post Altair hard fork.
	ProportionalSlashingMultiplierAltair    uint64 `yaml:"PROPORTIONAL_SLASHING_MULTIPLIER_ALTAIR" spec:"true" json:"PROPORTIONAL_SLASHING_MULTIPLIER_ALTAIR,string"`       // ProportionalSlashingMultiplierAltair for slashing penalties' multiplier post Alair hard fork.
	MinSlashingPenaltyQuotientBellatrix     uint64 `yaml:"MIN_SLASHING_PENALTY_QUOTIENT_BELLATRIX" spec:"true" json:"MIN_SLASHING_PENALTY_QUOTIENT_BELLATRIX,string"`       // MinSlashingPenaltyQuotientBellatrix for slashing penalties post Bellatrix hard fork.
	ProportionalSlashingMultiplierBellatrix uint64 `yaml:"PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX" spec:"true" json:"PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX,string"` // ProportionalSlashingMultiplierBellatrix for slashing penalties' multiplier post Bellatrix hard fork.
	InactivityPenaltyQuotientBellatrix      uint64 `yaml:"INACTIVITY_PENALTY_QUOTIENT_BELLATRIX" spec:"true" json:"INACTIVITY_PENALTY_QUOTIENT_BELLATRIX,string"`           // InactivityPenaltyQuotientBellatrix for penalties during inactivity post Bellatrix hard fork.

	// Light client
	MinSyncCommitteeParticipants uint64 `yaml:"MIN_SYNC_COMMITTEE_PARTICIPANTS" spec:"true" json:"MIN_SYNC_COMMITTEE_PARTICIPANTS,string"` // MinSyncCommitteeParticipants defines the minimum amount of sync committee participants for which the light client acknowledges the signature.

	// Bellatrix
	TerminalBlockHash                common.Hash `yaml:"TERMINAL_BLOCK_HASH" spec:"true" json:"TERMINAL_BLOCK_HASH"`                                          // TerminalBlockHash of beacon chain.
	TerminalBlockHashActivationEpoch uint64      `yaml:"TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH" spec:"true" json:"TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH,string"` // TerminalBlockHashActivationEpoch of beacon chain.
	TerminalTotalDifficulty          string      `yaml:"TERMINAL_TOTAL_DIFFICULTY" spec:"true"  json:"TERMINAL_TOTAL_DIFFICULTY"`                             // TerminalTotalDifficulty is part of the experimental Bellatrix spec. This value is type is currently TBD.
	DefaultBuilderGasLimit           uint64      `json:"-"`                                                                                                   // DefaultBuilderGasLimit is the default used to set the gaslimit for the Builder APIs, typically at around 30M wei.

	// Mev-boost circuit breaker
	MaxBuilderConsecutiveMissedSlots uint64 `json:"-"` // MaxBuilderConsecutiveMissedSlots defines the number of consecutive skip slot to fallback from using relay/builder to local execution engine for block construction.
	MaxBuilderEpochMissedSlots       uint64 `json:"-"` // MaxBuilderEpochMissedSlots is defines the number of total skip slot (per epoch rolling windows) to fallback from using relay/builder to local execution engine for block construction.

	MaxBlobGasPerBlock     uint64 `yaml:"MAX_BLOB_GAS_PER_BLOCK" json:"MAX_BLOB_GAS_PER_BLOCK,string"`       // MaxBlobGasPerBlock defines the maximum gas limit for blob sidecar per block.
	MaxBlobsPerBlock       uint64 `yaml:"MAX_BLOBS_PER_BLOCK" json:"MAX_BLOBS_PER_BLOCK,string"`             // MaxBlobsPerBlock defines the maximum number of blobs per block.
	BlobSidecarSubnetCount uint64 `yaml:"BLOB_SIDECAR_SUBNET_COUNT" json:"BLOB_SIDECAR_SUBNET_COUNT,string"` // BlobSidecarSubnetCount defines the number of sidecars in the blob subnet.

	// Whisk
	WhiskEpochsPerShufflingPhase uint64 `yaml:"WHISK_EPOCHS_PER_SHUFFLING_PHASE" spec:"true" json:"WHISK_EPOCHS_PER_SHUFFLING_PHASE,string"` // WhiskEpochsPerShufflingPhase defines the number of epochs per shuffling phase.
	WhiskProposerSelectionGap    uint64 `yaml:"WHISK_PROPOSER_SELECTION_GAP" spec:"true" json:"WHISK_PROPOSER_SELECTION_GAP,string"`         // WhiskProposerSelectionGap defines the proposer selection gap.

	// EIP7594
	NumberOfColumns                        uint64 `yaml:"NUMBER_OF_COLUMNS" spec:"true" json:"NUMBER_OF_COLUMNS,string"`                                                       // NumberOfColumns defines the number of columns in the extended matrix.
	MaxCellsInExtendedMatrix               uint64 `yaml:"MAX_CELLS_IN_EXTENDED_MATRIX" spec:"true" json:"MAX_CELLS_IN_EXTENDED_MATRIX,string"`                                 // MaxCellsInExtendedMatrix defines the maximum number of cells in the extended matrix.
	DataColumnSidecarSubnetCount           uint64 `yaml:"DATA_COLUMN_SIDECAR_SUBNET_COUNT" spec:"true" json:"DATA_COLUMN_SIDECAR_SUBNET_COUNT,string"`                         // DataColumnSidecarSubnetCount defines the number of sidecars in the data column subnet.
	MaxRequestDataColumnSidecars           uint64 `yaml:"MAX_REQUEST_DATA_COLUMN_SIDECARS" spec:"true" json:"MAX_REQUEST_DATA_COLUMN_SIDECARS,string"`                         // MaxRequestDataColumnSidecars defines the maximum number of data column sidecars that can be requested.
	SamplesPerSlot                         uint64 `yaml:"SAMPLES_PER_SLOT" spec:"true" json:"SAMPLES_PER_SLOT,string"`                                                         // SamplesPerSlot defines the number of samples per slot.
	CustodyRequirement                     uint64 `yaml:"CUSTODY_REQUIREMENT" spec:"true" json:"CUSTODY_REQUIREMENT,string"`                                                   // CustodyRequirement defines the custody requirement.
	TargetNumberOfPeers                    uint64 `yaml:"TARGET_NUMBER_OF_PEERS" spec:"true" json:"TARGET_NUMBER_OF_PEERS,string"`                                             // TargetNumberOfPeers defines the target number of peers.
	NumberOfCustodyGroups                  uint64 `yaml:"NUMBER_OF_CUSTODY_GROUPS" spec:"true" json:"NUMBER_OF_CUSTODY_GROUPS,string"`                                         // NumberOfCustodyGroups defines the number of custody groups.
	MinEpochsForDataColumnSidecarsRequests uint64 `yaml:"MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS" spec:"true" json:"MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS,string"` // MinEpochsForDataColumnSidecarsRequests defines the minimum number of epochs for data column sidecars requests.

	// Electra
	MinPerEpochChurnLimitElectra          uint64 `yaml:"MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA" spec:"true" json:"MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA,string"`                   // MinPerEpochChurnLimitElectra defines the minimum per epoch churn limit for Electra.
	MaxPerEpochActivationExitChurnLimit   uint64 `yaml:"MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT" spec:"true" json:"MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT,string"`   // MaxPerEpochActivationExitChurnLimit defines the maximum per epoch activation exit churn limit for Electra.
	MaxDepositRequestsPerPayload          uint64 `yaml:"MAX_DEPOSIT_REQUESTS_PER_PAYLOAD" spec:"true" json:"MAX_DEPOSIT_REQUESTS_PER_PAYLOAD,string"`                     // MaxDepositRequestsPerPayload defines the maximum number of deposit requests in a block.
	MaxWithdrawalRequestsPerPayload       uint64 `yaml:"MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD" spec:"true" json:"MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD,string"`               // MaxWithdrawalRequestsPerPayload defines the maximum number of withdrawal requests in a block.
	MaxConsolidationRequestsPerPayload    uint64 `yaml:"MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD" spec:"true" json:"MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD,string"`         // MaxConsolidationRequestsPerPayload defines the maximum number of consolidation requests in a block.
	MinSlashingPenaltyQuotientElectra     uint64 `yaml:"MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA" spec:"true" json:"MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA,string"`           // MinSlashingPenaltyQuotientElectra for slashing penalties post Electra hard fork.
	WhistleBlowerRewardQuotientElectra    uint64 `yaml:"WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA" spec:"true" json:"WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA,string"`           // WhistleBlowerRewardQuotientElectra is used to calculate whistle blower reward post Electra hard fork.
	MaxPendingPartialsPerWithdrawalsSweep uint64 `yaml:"MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP" spec:"true" json:"MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP,string"` // MaxPendingPartialsPerWithdrawalsSweep bounds the size of the sweep searching for pending partials per slot.
	MaxPendingDepositsPerEpoch            uint64 `yaml:"MAX_PENDING_DEPOSITS_PER_EPOCH" spec:"true" json:"MAX_PENDING_DEPOSITS_PER_EPOCH,string"`                         // MaxPendingDepositsPerEpoch defines the maximum number of pending deposits per epoch.
	PendingDepositLimits                  uint64 `yaml:"PENDING_DEPOSITS_LIMIT" spec:"true" json:"PENDING_DEPOSIT_LIMIT,string"`                                          // PendingDepositLimit defines the maximum number of pending deposits.
	PendingPartialWithdrawalsLimit        uint64 `yaml:"PENDING_PARTIAL_WITHDRAWALS_LIMIT" spec:"true" json:"PENDING_PARTIAL_WITHDRAWALS_LIMIT,string"`                   // PendingPartialWithdrawalsLimit defines the maximum number of pending partial withdrawals.
	PendingConsolidationsLimit            uint64 `yaml:"PENDING_CONSOLIDATIONS_LIMIT" spec:"true" json:"PENDING_CONSOLIDATIONS_LIMIT,string"`                             // PendingConsolidationsLimit defines the maximum number of pending consolidations.
	MaxBlobsPerBlockElectra               uint64 `yaml:"MAX_BLOBS_PER_BLOCK_ELECTRA" spec:"true" json:"MAX_BLOBS_PER_BLOCK_ELECTRA,string"`                               // MaxBlobsPerBlockElectra defines the maximum number of blobs per block for Electra.
	BlobSidecarSubnetCountElectra         uint64 `yaml:"BLOB_SIDECAR_SUBNET_COUNT_ELECTRA" spec:"true" json:"BLOB_SIDECAR_SUBNET_COUNT_ELECTRA,string"`                   // BlobSidecarSubnetCountElectra defines the number of sidecars in the blob subnet for Electra.

	// Constants for the Electra fork.
	UnsetDepositRequestsStartIndex uint64     `yaml:"UNSET_DEPOSIT_REQUESTS_START_INDEX" spec:"true" json:"UNSET_DEPOSIT_REQUESTS_START_INDEX,string"` // UnsetDepositRequestsStartIndex defines the start index for unset deposit requests.
	FullExitRequestAmount          uint64     `yaml:"FULL_EXIT_REQUEST_AMOUNT" spec:"true" json:"FULL_EXIT_REQUEST_AMOUNT,string"`                     // FullExitRequestAmount defines the amount for a full exit request.
	CompoundingWithdrawalPrefix    ConfigByte `yaml:"COMPOUNDING_WITHDRAWAL_PREFIX" spec:"true" json:"COMPOUNDING_WITHDRAWAL_PREFIX"`                  // CompoundingWithdrawalPrefix is the prefix for compounding withdrawals.
	DepositRequestType             ConfigByte `yaml:"DEPOSIT_REQUEST_TYPE" spec:"true" json:"DEPOSIT_REQUEST_TYPE"`                                    // DepositRequestType is the type for deposit requests.
	WithdrawalRequestType          ConfigByte `yaml:"WITHDRAWAL_REQUEST_TYPE" spec:"true" json:"WITHDRAWAL_REQUEST_TYPE"`                              // WithdrawalRequestType is the type for withdrawal requests.
	ConsolidationRequestType       ConfigByte `yaml:"CONSOLIDATION_REQUEST_TYPE" spec:"true" json:"CONSOLIDATION_REQUEST_TYPE"`                        // ConsolidationRequestType is the type for consolidation requests.

	// EIP7892 - Blob Schedule
	BlobSchedule []BlobParameters `yaml:"BLOB_SCHEDULE" spec:"true" json:"BLOB_SCHEDULE"` // Schedule of blob limits per epoch
	// Fulu
	ValidatorCustodyRequirement      uint64 `yaml:"VALIDATOR_CUSTODY_REQUIREMENT" spec:"true" json:"VALIDATOR_CUSTODY_REQUIREMENT,string"`               // ValidatorCustodyRequirement defines the custody requirement for validators.
	BalancePerAdditionalCustodyGroup uint64 `yaml:"BALANCE_PER_ADDITIONAL_CUSTODY_GROUP" spec:"true" json:"BALANCE_PER_ADDITIONAL_CUSTODY_GROUP,string"` // BalancePerAdditionalCustodyGroup defines the balance required per additional custody group.
}

// GetBlobParameters returns the blob parameters at a given epoch
func (b *BeaconChainConfig) GetBlobParameters(epoch uint64) BlobParameters {
	// Iterate through schedule in desceding order
	for i := range b.BlobSchedule {
		entry := b.BlobSchedule[i]
		if epoch >= entry.Epoch {
			return entry
		}
	}
	// Default to Electra parameters if no matching schedule entry
	return BlobParameters{
		Epoch:            b.ElectraForkEpoch,
		MaxBlobsPerBlock: b.MaxBlobsPerBlockElectra,
	}
}

func (b *BeaconChainConfig) RoundSlotToEpoch(slot uint64) uint64 {
	return slot - (slot % b.SlotsPerEpoch)
}

func (b *BeaconChainConfig) RoundSlotToSyncCommitteePeriod(slot uint64) uint64 {
	slotsPerSyncCommitteePeriod := b.SlotsPerEpoch * b.EpochsPerSyncCommitteePeriod
	return slot - (slot % slotsPerSyncCommitteePeriod)
}

func (b *BeaconChainConfig) SyncCommitteePeriod(slot uint64) uint64 {
	return slot / (b.SlotsPerEpoch * b.EpochsPerSyncCommitteePeriod)
}

func (b *BeaconChainConfig) RoundSlotToVotePeriod(slot uint64) uint64 {
	p := b.SlotsPerEpoch * b.EpochsPerEth1VotingPeriod
	return slot - (slot % p)
}

func (b *BeaconChainConfig) GetCurrentStateVersion(epoch uint64) StateVersion {
	forkEpochList := []uint64{
		b.AltairForkEpoch,
		b.BellatrixForkEpoch,
		b.CapellaForkEpoch,
		b.DenebForkEpoch,
		b.ElectraForkEpoch,
		b.FuluForkEpoch,
	}
	stateVersion := Phase0Version
	for _, forkEpoch := range forkEpochList {
		if forkEpoch > epoch {
			return stateVersion
		}
		stateVersion++
	}
	return stateVersion
}

// InitializeForkSchedule initializes the schedules forks baked into the config.
func (b *BeaconChainConfig) InitializeForkSchedule() {
	b.ForkVersionSchedule = configForkSchedule(b)
	// sort blob schedule by epoch in descending order
	sort.Slice(b.BlobSchedule, func(i, j int) bool {
		return b.BlobSchedule[i].Epoch > b.BlobSchedule[j].Epoch
	})
}

func configForkSchedule(b *BeaconChainConfig) map[common.Bytes4]VersionScheduleEntry {
	fvs := map[common.Bytes4]VersionScheduleEntry{}
	fvs[utils.Uint32ToBytes4(uint32(b.GenesisForkVersion))] = VersionScheduleEntry{b.GenesisSlot / b.SlotsPerEpoch, Phase0Version}
	fvs[utils.Uint32ToBytes4(uint32(b.AltairForkVersion))] = VersionScheduleEntry{b.AltairForkEpoch, AltairVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.BellatrixForkVersion))] = VersionScheduleEntry{b.BellatrixForkEpoch, BellatrixVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.CapellaForkVersion))] = VersionScheduleEntry{b.CapellaForkEpoch, CapellaVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.DenebForkVersion))] = VersionScheduleEntry{b.DenebForkEpoch, DenebVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.ElectraForkVersion))] = VersionScheduleEntry{b.ElectraForkEpoch, ElectraVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.FuluForkVersion))] = VersionScheduleEntry{b.FuluForkEpoch, FuluVersion}
	return fvs
}

func (b *BeaconChainConfig) ParticipationWeights() []uint64 {
	return []uint64{
		b.TimelySourceWeight,
		b.TimelyTargetWeight,
		b.TimelyHeadWeight,
	}
}

var MainnetBeaconConfig BeaconChainConfig = BeaconChainConfig{
	// Constants (Non-configurable)
	FarFutureEpoch:           math.MaxUint64,
	FarFutureSlot:            math.MaxUint64,
	BaseRewardsPerEpoch:      4,
	DepositContractTreeDepth: 32,
	GenesisDelay:             604800, // 1 week.

	// Misc constant.
	TargetCommitteeSize:              128,
	MaxValidatorsPerCommittee:        2048,
	MaxCommitteesPerSlot:             64,
	MinPerEpochChurnLimit:            4,
	ChurnLimitQuotient:               1 << 16,
	MaxPerEpochActivationChurnLimit:  8,
	ShuffleRoundCount:                90,
	MinGenesisActiveValidatorCount:   16384,
	MinGenesisTime:                   1606824000, // Dec 1, 2020, 12pm UTC.
	TargetAggregatorsPerCommittee:    16,
	HysteresisQuotient:               4,
	HysteresisDownwardMultiplier:     1,
	HysteresisUpwardMultiplier:       5,
	MinEpochsForBlobSidecarsRequests: 4096,
	FieldElementsPerBlob:             4096,
	MaxBytesPerTransaction:           1073741824, // 1GB
	MaxExtraDataBytes:                32,
	MaxRequestBlobSidecars:           768,
	MaxRequestBlobSidecarsElectra:    1152, // MAX_REQUEST_BLOCKS_DENEB * MAX_BLOBS_PER_BLOCK_ELECTRA
	MaxRequestBlocks:                 1024,
	MaxRequestBlocksDeneb:            128,
	MaxTransactionsPerPayload:        1048576,
	SubnetsPerNode:                   2,
	VersionedHashVersionKzg:          ConfigByte(1),

	// Gwei value constants.
	MinDepositAmount:           1 * 1e9,
	MaxEffectiveBalance:        32 * 1e9,
	MinActivationBalance:       32 * 1e9,
	MaxEffectiveBalanceElectra: 2048 * 1e9,
	EjectionBalance:            16 * 1e9,
	EffectiveBalanceIncrement:  1 * 1e9,

	// Initial value constants.
	BLSWithdrawalPrefixByte:         ConfigByte(0),
	ETH1AddressWithdrawalPrefixByte: ConfigByte(1),

	// Time parameter constants.
	MinAttestationInclusionDelay:     1,
	SecondsPerSlot:                   12,
	SlotsPerEpoch:                    32,
	MinSeedLookahead:                 1,
	MaxSeedLookahead:                 4,
	EpochsPerEth1VotingPeriod:        64,
	SlotsPerHistoricalRoot:           8192,
	MinValidatorWithdrawabilityDelay: 256,
	ShardCommitteePeriod:             256,
	MinEpochsToInactivityPenalty:     4,
	Eth1FollowDistance:               2048,
	SafeSlotsToUpdateJustified:       8,

	// Fork choice algorithm constants.
	ProposerScoreBoost: 40,
	IntervalsPerSlot:   3,

	// Ethereum PoW parameters.
	DepositChainID:         1, // Chain ID of eth1 mainnet.
	DepositNetworkID:       1, // Network ID of eth1 mainnet.
	DepositContractAddress: "0x00000000219ab540356cBB839Cbe05303d7705Fa",

	// Validator params.
	RandomSubnetsPerValidator:         1 << 0,
	EpochsPerRandomSubnetSubscription: 1 << 8,

	// While eth1 mainnet block times are closer to 13s, we must conform with other clients in
	// order to vote on the correct eth1 blocks.
	//
	// Additional context: https://github.com/ethereum/consensus-specs/issues/2132
	// Bug prompting this change: https://github.com/prysmaticlabs/prysm/issues/7856
	// Future optimization: https://github.com/prysmaticlabs/prysm/issues/7739
	SecondsPerETH1Block: 14,

	// State list length constants.
	EpochsPerHistoricalVector: 65536,
	EpochsPerSlashingsVector:  8192,
	HistoricalRootsLimit:      16777216,
	ValidatorRegistryLimit:    1099511627776,

	// Reward and penalty quotients constants.
	BaseRewardFactor:               64,
	WhistleBlowerRewardQuotient:    512,
	ProposerRewardQuotient:         8,
	InactivityPenaltyQuotient:      67108864,
	MinSlashingPenaltyQuotient:     128,
	ProportionalSlashingMultiplier: 1,

	// Max operations per block constants.
	MaxProposerSlashings:             16,
	MaxAttesterSlashings:             2,
	MaxAttesterSlashingsElectra:      1,
	MaxAttestations:                  128,
	MaxAttestationsElectra:           8,
	MaxDeposits:                      16,
	MaxVoluntaryExits:                16,
	MaxWithdrawalsPerPayload:         16,
	MaxBlsToExecutionChanges:         16,
	MaxValidatorsPerWithdrawalsSweep: 16384,
	MaxBlobCommittmentsPerBlock:      4096,

	// BLS domain values.
	DomainBeaconProposer:              utils.Uint32ToBytes4(0x00000000),
	DomainBeaconAttester:              utils.Uint32ToBytes4(0x01000000),
	DomainRandao:                      utils.Uint32ToBytes4(0x02000000),
	DomainDeposit:                     utils.Uint32ToBytes4(0x03000000),
	DomainVoluntaryExit:               utils.Uint32ToBytes4(0x04000000),
	DomainSelectionProof:              utils.Uint32ToBytes4(0x05000000),
	DomainAggregateAndProof:           utils.Uint32ToBytes4(0x06000000),
	DomainSyncCommittee:               utils.Uint32ToBytes4(0x07000000),
	DomainSyncCommitteeSelectionProof: utils.Uint32ToBytes4(0x08000000),
	DomainContributionAndProof:        utils.Uint32ToBytes4(0x09000000),
	DomainApplicationMask:             utils.Uint32ToBytes4(0x00000001),
	DomainApplicationBuilder:          utils.Uint32ToBytes4(0x00000001),
	DomainBLSToExecutionChange:        utils.Uint32ToBytes4(0x0A000000),

	// Prysm constants.
	ConfigName: "mainnet",
	PresetBase: "mainnet",

	// Slasher related values.
	PruneSlasherStoragePeriod:       10,
	SlashingProtectionPruningEpochs: 512,

	// Fork related values.
	GenesisForkVersion:   0,
	AltairForkVersion:    0x01000000,
	AltairForkEpoch:      74240,
	BellatrixForkVersion: 0x02000000,
	BellatrixForkEpoch:   144896,
	CapellaForkVersion:   0x03000000,
	CapellaForkEpoch:     194048,
	DenebForkVersion:     0x04000000,
	DenebForkEpoch:       269568,
	ElectraForkVersion:   0x05000000,
	ElectraForkEpoch:     364032,
	FuluForkVersion:      0x06000000,
	FuluForkEpoch:        math.MaxUint64,

	// New values introduced in Altair hard fork 1.
	// Participation flag indices.
	TimelySourceFlagIndex: 0,
	TimelyTargetFlagIndex: 1,
	TimelyHeadFlagIndex:   2,

	// Incentivization weight values.
	TimelySourceWeight: 14,
	TimelyTargetWeight: 26,
	TimelyHeadWeight:   14,
	SyncRewardWeight:   2,
	ProposerWeight:     8,
	WeightDenominator:  64,

	// Validator related values.
	TargetAggregatorsPerSyncSubcommittee: 16,
	SyncCommitteeSubnetCount:             4,

	// Misc values.
	SyncCommitteeSize:            512,
	InactivityScoreBias:          4,
	InactivityScoreRecoveryRate:  16,
	EpochsPerSyncCommitteePeriod: 256,
	BytesPerLogsBloom:            256,

	// Updated penalty values.
	InactivityPenaltyQuotientAltair:         3 * 1 << 24, //50331648
	MinSlashingPenaltyQuotientAltair:        64,
	ProportionalSlashingMultiplierAltair:    2,
	MinSlashingPenaltyQuotientBellatrix:     32,
	ProportionalSlashingMultiplierBellatrix: 3,
	InactivityPenaltyQuotientBellatrix:      1 << 24,

	// Light client
	MinSyncCommitteeParticipants: 1,

	// Bellatrix
	TerminalBlockHashActivationEpoch: 18446744073709551615,
	TerminalBlockHash:                [32]byte{},
	TerminalTotalDifficulty:          "58750000000000000000000", // Estimated: Sept 15, 2022
	DefaultBuilderGasLimit:           uint64(36000000),

	// Mevboost circuit breaker
	MaxBuilderConsecutiveMissedSlots: 3,
	MaxBuilderEpochMissedSlots:       8,

	MaxBlobGasPerBlock:     786432,
	MaxBlobsPerBlock:       6,
	BlobSidecarSubnetCount: 6,

	WhiskEpochsPerShufflingPhase: 256,
	WhiskProposerSelectionGap:    2,

	// EIP-7594
	NumberOfColumns:                        128,
	MaxCellsInExtendedMatrix:               768,
	DataColumnSidecarSubnetCount:           128,
	MaxRequestDataColumnSidecars:           16384, // MAX_REQUEST_BLOCKS_DENEB * NUMBER_OF_COLUMNS
	SamplesPerSlot:                         8,
	CustodyRequirement:                     4,
	TargetNumberOfPeers:                    70,
	NumberOfCustodyGroups:                  128,
	MinEpochsForDataColumnSidecarsRequests: 1 << 12, // 4096 epochs

	// Electra
	MinPerEpochChurnLimitElectra:          128_000_000_000,
	MaxPerEpochActivationExitChurnLimit:   256_000_000_000,
	MaxDepositRequestsPerPayload:          8192,
	MaxWithdrawalRequestsPerPayload:       16,
	MaxConsolidationRequestsPerPayload:    2,
	MinSlashingPenaltyQuotientElectra:     4096,
	WhistleBlowerRewardQuotientElectra:    4096,
	MaxPendingPartialsPerWithdrawalsSweep: 8,
	MaxPendingDepositsPerEpoch:            16,
	PendingDepositLimits:                  1 << 27,
	PendingPartialWithdrawalsLimit:        1 << 27,
	PendingConsolidationsLimit:            1 << 18,
	MaxBlobsPerBlockElectra:               9,
	BlobSidecarSubnetCountElectra:         9,
	// Electra constants.
	UnsetDepositRequestsStartIndex: ^uint64(0), // 2**64 - 1
	FullExitRequestAmount:          0,
	CompoundingWithdrawalPrefix:    0x02,
	DepositRequestType:             0x00,
	WithdrawalRequestType:          0x01,
	ConsolidationRequestType:       0x02,

	// Fulu
	ValidatorCustodyRequirement:      8,
	BalancePerAdditionalCustodyGroup: 32_000_000_000,
	BlobSchedule:                     []BlobParameters{},
}

func mainnetConfig() BeaconChainConfig {
	cfg := MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	return cfg
}

func CustomConfig(configFile string) (BeaconChainConfig, NetworkConfig, error) {
	networkConfig, beaconCfg := GetConfigsByNetwork(chainspec.MainnetChainID)
	b, err := os.ReadFile(configFile) // just pass the file name
	if err != nil {
		return BeaconChainConfig{}, NetworkConfig{}, err
	}

	// setup beacon chain config
	if err := yaml.Unmarshal(b, &beaconCfg); err != nil {
		return BeaconChainConfig{}, NetworkConfig{}, err
	}
	beaconCfg.InitializeForkSchedule()

	// setup network config
	if err := yaml.Unmarshal(b, &networkConfig); err != nil {
		return BeaconChainConfig{}, NetworkConfig{}, err
	}
	return *beaconCfg, *networkConfig, nil
}

func sepoliaConfig() BeaconChainConfig {
	cfg := MainnetBeaconConfig
	cfg.MinGenesisTime = 1655647200
	cfg.GenesisDelay = 86400
	cfg.MinGenesisActiveValidatorCount = 1300
	cfg.ConfigName = "sepolia"

	cfg.GenesisForkVersion = 0x90000069
	cfg.SecondsPerETH1Block = 14
	cfg.DepositChainID = chainspec.SepoliaChainID
	cfg.DepositNetworkID = chainspec.SepoliaChainID
	cfg.AltairForkEpoch = 50
	cfg.AltairForkVersion = 0x90000070
	cfg.BellatrixForkEpoch = 100
	cfg.BellatrixForkVersion = 0x90000071
	cfg.CapellaForkEpoch = 56832
	cfg.CapellaForkVersion = 0x90000072
	cfg.DenebForkEpoch = 132608
	cfg.DenebForkVersion = 0x90000073
	cfg.ElectraForkEpoch = 222464
	cfg.ElectraForkVersion = 0x90000074
	cfg.FuluForkEpoch = math.MaxUint64
	cfg.FuluForkVersion = 0x90000075
	cfg.TerminalTotalDifficulty = "17000000000000000"
	cfg.DepositContractAddress = "0x7f02C3E3c98b133055B8B348B2Ac625669Ed295D"
	cfg.InitializeForkSchedule()
	return cfg
}

func holeskyConfig() BeaconChainConfig {
	cfg := MainnetBeaconConfig
	cfg.ConfigName = "holesky"
	cfg.MinGenesisActiveValidatorCount = 16384
	cfg.MinGenesisTime = 1695902100
	cfg.GenesisForkVersion = 0x01017000
	cfg.GenesisDelay = 300
	cfg.SecondsPerSlot = 12
	cfg.Eth1FollowDistance = 2048
	cfg.DepositChainID = chainspec.HoleskyChainID
	cfg.DepositNetworkID = chainspec.HoleskyChainID

	cfg.AltairForkEpoch = 0
	cfg.AltairForkVersion = 0x02017000
	cfg.BellatrixForkEpoch = 0
	cfg.BellatrixForkVersion = 0x03017000
	cfg.CapellaForkEpoch = 256
	cfg.CapellaForkVersion = 0x04017000
	cfg.DenebForkEpoch = 29696
	cfg.DenebForkVersion = 0x05017000
	cfg.ElectraForkEpoch = 115968
	cfg.ElectraForkVersion = 0x06017000
	cfg.FuluForkEpoch = math.MaxUint64
	cfg.FuluForkVersion = 0x07017000
	cfg.TerminalTotalDifficulty = "0"
	cfg.TerminalBlockHash = [32]byte{}
	cfg.TerminalBlockHashActivationEpoch = math.MaxUint64
	cfg.DepositContractAddress = "0x4242424242424242424242424242424242424242"
	cfg.BaseRewardFactor = 64
	cfg.SlotsPerEpoch = 32
	cfg.EpochsPerSyncCommitteePeriod = 256
	cfg.InactivityScoreBias = 4
	cfg.InactivityScoreRecoveryRate = 16
	cfg.EjectionBalance = 28000000000
	cfg.MinPerEpochChurnLimit = 4
	cfg.ChurnLimitQuotient = 1 << 16
	cfg.ProposerScoreBoost = 40

	cfg.InitializeForkSchedule()
	return cfg

}

func hoodiConfig() BeaconChainConfig {
	cfg := MainnetBeaconConfig
	cfg.ConfigName = "hoodi"
	cfg.MinGenesisActiveValidatorCount = 16384
	cfg.MinGenesisTime = 1742212800
	cfg.GenesisForkVersion = 0x10000910
	cfg.GenesisDelay = 600

	// Time parameters
	cfg.SecondsPerSlot = 12
	cfg.Eth1FollowDistance = 2048

	// Forking
	cfg.AltairForkEpoch = 0
	cfg.AltairForkVersion = 0x20000910
	cfg.BellatrixForkEpoch = 0
	cfg.BellatrixForkVersion = 0x30000910
	cfg.CapellaForkEpoch = 0
	cfg.CapellaForkVersion = 0x40000910
	cfg.DenebForkEpoch = 0
	cfg.DenebForkVersion = 0x50000910
	cfg.ElectraForkEpoch = 2048
	cfg.ElectraForkVersion = 0x60000910
	cfg.FuluForkEpoch = math.MaxUint64
	cfg.FuluForkVersion = 0x70000910
	cfg.TerminalTotalDifficulty = "0"
	cfg.TerminalBlockHash = [32]byte{}
	cfg.TerminalBlockHashActivationEpoch = math.MaxUint64

	// Deposit contract
	cfg.DepositContractAddress = "0x00000000219ab540356cBB839Cbe05303d7705Fa"
	cfg.DepositChainID = chainspec.HoodiChainID
	cfg.DepositNetworkID = chainspec.HoodiChainID

	cfg.MaxBlobsPerBlockElectra = 9
	cfg.BlobSidecarSubnetCountElectra = 9

	cfg.SlotsPerEpoch = 32
	cfg.EpochsPerSyncCommitteePeriod = 256
	cfg.MinPerEpochChurnLimit = 4

	cfg.InitializeForkSchedule()
	return cfg

}

func gnosisConfig() BeaconChainConfig {
	cfg := MainnetBeaconConfig
	cfg.PresetBase = "gnosis"
	cfg.MinGenesisTime = 1638968400
	cfg.MinGenesisActiveValidatorCount = 4096
	cfg.GenesisDelay = 6000
	cfg.SecondsPerSlot = 5
	cfg.Eth1FollowDistance = 1024
	cfg.ConfigName = "gnosis"
	cfg.ChurnLimitQuotient = 1 << 12
	cfg.GenesisForkVersion = 0x00000064
	cfg.SecondsPerETH1Block = 6
	cfg.DepositChainID = chainspec.GnosisChainID
	cfg.DepositNetworkID = chainspec.GnosisChainID
	cfg.AltairForkEpoch = 512
	cfg.AltairForkVersion = 0x01000064
	cfg.BellatrixForkEpoch = 385536
	cfg.BellatrixForkVersion = 0x02000064
	cfg.CapellaForkEpoch = 648704
	cfg.CapellaForkVersion = 0x03000064
	cfg.DenebForkEpoch = 889856
	cfg.DenebForkVersion = 0x04000064
	cfg.ElectraForkEpoch = 1337856
	cfg.ElectraForkVersion = 0x05000064
	cfg.FuluForkEpoch = math.MaxUint64
	cfg.FuluForkVersion = 0x06000064
	cfg.TerminalTotalDifficulty = "8626000000000000000000058750000000000000000000"
	cfg.DepositContractAddress = "0x0B98057eA310F4d31F2a452B414647007d1645d9"
	cfg.BaseRewardFactor = 25
	cfg.SlotsPerEpoch = 16
	cfg.EpochsPerSyncCommitteePeriod = 512
	cfg.InactivityScoreRecoveryRate = 16
	cfg.InactivityScoreBias = 4
	cfg.MaxWithdrawalsPerPayload = 8
	cfg.MaxValidatorsPerWithdrawalsSweep = 8192
	cfg.MaxBlobsPerBlock = 2
	cfg.MaxBlobsPerBlockElectra = 2
	cfg.BlobSidecarSubnetCountElectra = 2
	cfg.MinEpochsForBlobSidecarsRequests = 16384
	cfg.MaxPerEpochActivationChurnLimit = 2
	cfg.MaxPerEpochActivationExitChurnLimit = 64_000_000_000
	cfg.MaxRequestBlobSidecarsElectra = 256
	cfg.InitializeForkSchedule()
	return cfg
}

func chiadoConfig() BeaconChainConfig {
	cfg := MainnetBeaconConfig
	cfg.PresetBase = "gnosis"
	cfg.MinGenesisTime = 1665396000
	cfg.MinGenesisActiveValidatorCount = 6000
	cfg.GenesisDelay = 300
	cfg.SecondsPerSlot = 5
	cfg.Eth1FollowDistance = 1024
	cfg.ConfigName = "chiado"
	cfg.ChurnLimitQuotient = 1 << 12
	cfg.GenesisForkVersion = 0x0000006f
	cfg.SecondsPerETH1Block = 6
	cfg.DepositChainID = chainspec.ChiadoChainID
	cfg.DepositNetworkID = chainspec.ChiadoChainID
	cfg.AltairForkEpoch = 90
	cfg.AltairForkVersion = 0x0100006f
	cfg.BellatrixForkEpoch = 180
	cfg.BellatrixForkVersion = 0x0200006f
	cfg.CapellaForkEpoch = 244224
	cfg.CapellaForkVersion = 0x0300006f
	cfg.DenebForkEpoch = 516608
	cfg.DenebForkVersion = 0x0400006f
	cfg.ElectraForkEpoch = 948224
	cfg.ElectraForkVersion = 0x0500006f
	cfg.FuluForkEpoch = math.MaxUint64
	cfg.FuluForkVersion = 0x0600006f
	cfg.TerminalTotalDifficulty = "231707791542740786049188744689299064356246512"
	cfg.DepositContractAddress = "0xb97036A26259B7147018913bD58a774cf91acf25"
	cfg.BaseRewardFactor = 25
	cfg.SlotsPerEpoch = 16
	cfg.EpochsPerSyncCommitteePeriod = 512
	cfg.MaxWithdrawalsPerPayload = 8
	cfg.MaxValidatorsPerWithdrawalsSweep = 8192
	cfg.MaxBlobsPerBlock = 2
	cfg.MaxBlobsPerBlockElectra = 2
	cfg.BlobSidecarSubnetCountElectra = 2
	cfg.MinEpochsForBlobSidecarsRequests = 16384
	cfg.MaxPerEpochActivationChurnLimit = 2
	cfg.MaxPerEpochActivationExitChurnLimit = 64_000_000_000
	cfg.MaxRequestBlobSidecarsElectra = 256
	cfg.InitializeForkSchedule()
	return cfg
}

func (b *BeaconChainConfig) GetMinSlashingPenaltyQuotient(version StateVersion) uint64 {
	switch version {
	case Phase0Version:
		return b.MinSlashingPenaltyQuotient
	case AltairVersion:
		return b.MinSlashingPenaltyQuotientAltair
	case BellatrixVersion:
		return b.MinSlashingPenaltyQuotientBellatrix
	case CapellaVersion:
		return b.MinSlashingPenaltyQuotientBellatrix
	case DenebVersion:
		return b.MinSlashingPenaltyQuotientBellatrix
	case ElectraVersion, FuluVersion:
		return b.MinSlashingPenaltyQuotientElectra
	default:
		panic("not implemented")
	}
}

func (b *BeaconChainConfig) GetWhistleBlowerRewardQuotient(version StateVersion) uint64 {
	if version >= ElectraVersion {
		return b.WhistleBlowerRewardQuotientElectra
	}
	return b.WhistleBlowerRewardQuotient
}

func (b *BeaconChainConfig) GetProportionalSlashingMultiplier(version StateVersion) uint64 {
	switch version {
	case Phase0Version:
		return b.ProportionalSlashingMultiplier
	case AltairVersion:
		return b.ProportionalSlashingMultiplierAltair
	case BellatrixVersion, CapellaVersion, DenebVersion, ElectraVersion, FuluVersion:
		return b.ProportionalSlashingMultiplierBellatrix
	default:
		panic("not implemented")
	}
}

func (b *BeaconChainConfig) GetPenaltyQuotient(version StateVersion) uint64 {
	switch version {
	case Phase0Version:
		return b.InactivityPenaltyQuotient
	case AltairVersion:
		return b.InactivityPenaltyQuotientAltair
	case BellatrixVersion:
		return b.InactivityPenaltyQuotientBellatrix
	case CapellaVersion:
		return b.InactivityPenaltyQuotientBellatrix
	case DenebVersion:
		return b.InactivityPenaltyQuotientBellatrix
	case ElectraVersion, FuluVersion:
		return b.InactivityPenaltyQuotientBellatrix
	default:
		panic("not implemented")
	}
}

// Beacon configs
var BeaconConfigs map[NetworkType]BeaconChainConfig = map[NetworkType]BeaconChainConfig{
	chainspec.MainnetChainID: mainnetConfig(),
	chainspec.SepoliaChainID: sepoliaConfig(),
	chainspec.HoleskyChainID: holeskyConfig(),
	chainspec.HoodiChainID:   hoodiConfig(),
	chainspec.GnosisChainID:  gnosisConfig(),
	chainspec.ChiadoChainID:  chiadoConfig(),
}

// Eth1DataVotesLength returns the maximum length of the votes on the Eth1 data,
// computed from the parameters in BeaconChainConfig.
func (b *BeaconChainConfig) Eth1DataVotesLength() uint64 {
	return b.EpochsPerEth1VotingPeriod * b.SlotsPerEpoch
}

// PreviousEpochAttestationsLength returns the maximum length of the pending
// attestation list for the previous epoch, computed from the parameters in
// BeaconChainConfig.
func (b *BeaconChainConfig) PreviousEpochAttestationsLength() uint64 {
	return b.SlotsPerEpoch * b.MaxAttestations
}

// CurrentEpochAttestationsLength returns the maximum length of the pending
// attestation list for the current epoch, computed from the parameters in
// BeaconChainConfig.
func (b *BeaconChainConfig) CurrentEpochAttestationsLength() uint64 {
	return b.SlotsPerEpoch * b.MaxAttestations
}

func (b *BeaconChainConfig) MaxEffectiveBalanceForVersion(version StateVersion) uint64 {
	switch version {
	case Phase0Version, AltairVersion, BellatrixVersion, CapellaVersion, DenebVersion:
		return b.MaxEffectiveBalance
	case ElectraVersion, FuluVersion:
		return b.MaxEffectiveBalanceElectra
	default:
		panic("invalid version")
	}
}

func (b *BeaconChainConfig) MaxBlobsPerBlockByVersion(v StateVersion) uint64 {
	switch v {
	case Phase0Version, AltairVersion, BellatrixVersion, CapellaVersion, DenebVersion:
		return b.MaxBlobsPerBlock
	case ElectraVersion, FuluVersion:
		return b.MaxBlobsPerBlockElectra
	}
	panic("invalid version")
}

func (b *BeaconChainConfig) MaxRequestBlobSidecarsByVersion(v StateVersion) int {
	switch v {
	case DenebVersion:
		return int(b.MaxRequestBlobSidecars)
	case ElectraVersion, FuluVersion:
		return int(b.MaxRequestBlobSidecarsElectra)
	}
	panic("invalid version")
}

func (b *BeaconChainConfig) BlobSidecarSubnetCountByVersion(v StateVersion) uint64 {
	switch v {
	case Phase0Version, AltairVersion, BellatrixVersion, CapellaVersion, DenebVersion:
		return b.BlobSidecarSubnetCount
	case ElectraVersion, FuluVersion:
		return b.BlobSidecarSubnetCountElectra
	}
	panic("invalid version")
}

func (b *BeaconChainConfig) GetForkVersionByVersion(v StateVersion) uint32 {
	switch v {
	case Phase0Version:
		return uint32(b.GenesisForkVersion)
	case AltairVersion:
		return uint32(b.AltairForkVersion)
	case BellatrixVersion:
		return uint32(b.BellatrixForkVersion)
	case CapellaVersion:
		return uint32(b.CapellaForkVersion)
	case DenebVersion:
		return uint32(b.DenebForkVersion)
	case ElectraVersion:
		return uint32(b.ElectraForkVersion)
	case FuluVersion:
		return uint32(b.FuluForkVersion)
	}
	panic("invalid version")
}

func (b *BeaconChainConfig) GetForkEpochByVersion(v StateVersion) uint64 {
	switch v {
	case Phase0Version:
		return 0
	case AltairVersion:
		return b.AltairForkEpoch
	case BellatrixVersion:
		return b.BellatrixForkEpoch
	case CapellaVersion:
		return b.CapellaForkEpoch
	case DenebVersion:
		return b.DenebForkEpoch
	case ElectraVersion:
		return b.ElectraForkEpoch
	case FuluVersion:
		return b.FuluForkEpoch
	}
	panic("invalid version")
}

func GetConfigsByNetwork(net NetworkType) (*NetworkConfig, *BeaconChainConfig) {
	networkConfig := NetworkConfigs[net]
	beaconConfig := BeaconConfigs[net]
	return &networkConfig, &beaconConfig
}

func GetConfigsByNetworkName(net string) (*NetworkConfig, *BeaconChainConfig, NetworkType, error) {
	switch net {
	case networkname.Mainnet:
		networkCfg, beaconCfg := GetConfigsByNetwork(chainspec.MainnetChainID)
		return networkCfg, beaconCfg, chainspec.MainnetChainID, nil
	case networkname.Sepolia:
		networkCfg, beaconCfg := GetConfigsByNetwork(chainspec.SepoliaChainID)
		return networkCfg, beaconCfg, chainspec.SepoliaChainID, nil
	case networkname.Gnosis:
		networkCfg, beaconCfg := GetConfigsByNetwork(chainspec.GnosisChainID)
		return networkCfg, beaconCfg, chainspec.GnosisChainID, nil
	case networkname.Chiado:
		networkCfg, beaconCfg := GetConfigsByNetwork(chainspec.ChiadoChainID)
		return networkCfg, beaconCfg, chainspec.ChiadoChainID, nil
	case networkname.Holesky:
		networkCfg, beaconCfg := GetConfigsByNetwork(chainspec.HoleskyChainID)
		return networkCfg, beaconCfg, chainspec.HoleskyChainID, nil
	case networkname.Hoodi:
		networkCfg, beaconCfg := GetConfigsByNetwork(chainspec.HoodiChainID)
		return networkCfg, beaconCfg, chainspec.HoodiChainID, nil
	default:
		return nil, nil, chainspec.MainnetChainID, errors.New("chain not found")
	}
}
func GetAllCheckpointSyncEndpoints(net NetworkType) []string {
	shuffle := func(urls []string) []string {
		if len(urls) <= 1 {
			return urls
		}
		retUrls := make([]string, len(urls))
		perm := mathrand.Perm(len(urls))
		for i, v := range perm {
			retUrls[v] = urls[i]
		}
		return retUrls
	}
	// shuffle the list of URLs to avoid always using the same one
	// order: custom URLs -> default URLs
	urls := []string{}
	urls = append(urls, shuffle(ConfigurableCheckpointsURLs)...)
	if len(ConfigurableCheckpointsURLs) > 0 {
		return urls
	}
	if checkpoints, ok := CheckpointSyncEndpoints[net]; ok {
		urls = append(urls, shuffle(checkpoints)...)
	}
	return urls
}

func GetCheckpointSyncEndpoint(net NetworkType) string {
	randomOne := func(checkpoints []string) string {
		if len(checkpoints) == 1 {
			return checkpoints[0]
		}
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(checkpoints))))
		if err != nil {
			panic(err)
		}
		return checkpoints[n.Int64()]
	}
	// Check if the user has configured a custom checkpoint sync endpoint
	if len(ConfigurableCheckpointsURLs) > 0 {
		return randomOne(ConfigurableCheckpointsURLs)
	}

	// Otherwise, use the default endpoints
	checkpoints, ok := CheckpointSyncEndpoints[net]
	if !ok {
		return ""
	}
	return randomOne(checkpoints)
}

// Check if chain with a specific ID is supported or not
func EmbeddedSupported(id uint64) bool {
	return id == chainspec.MainnetChainID ||
		id == chainspec.HoleskyChainID ||
		id == chainspec.SepoliaChainID ||
		id == chainspec.GnosisChainID ||
		id == chainspec.ChiadoChainID ||
		id == chainspec.HoodiChainID
}

func SupportBackfilling(networkId uint64) bool {
	return networkId == chainspec.MainnetChainID ||
		networkId == chainspec.SepoliaChainID ||
		networkId == chainspec.GnosisChainID ||
		networkId == chainspec.ChiadoChainID ||
		networkId == chainspec.HoleskyChainID ||
		networkId == chainspec.HoodiChainID
}

func EpochToPaths(slot uint64, config *BeaconChainConfig, suffix string) (string, string) {
	folderPath := path.Clean(strconv.FormatUint(slot/SubDivisionFolderSize, 10))
	return folderPath, path.Clean(fmt.Sprintf("%s/%d.%s.sz", folderPath, slot, suffix))
}

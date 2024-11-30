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
	"errors"
	"fmt"
	"math"
	"math/big"
	mathrand "math/rand"
	"os"
	"path"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/erigontech/erigon-lib/chain/networkname"
	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/utils"
)

var LatestStateFileName = "latest.ssz_snappy"

type CaplinConfig struct {
	Backfilling               bool
	BlobBackfilling           bool
	BlobPruningDisabled       bool
	Archive                   bool
	SnapshotGenerationEnabled bool
	NetworkId                 NetworkType
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
	CaplinDiscoveryAddr    string
	CaplinDiscoveryPort    uint64
	CaplinDiscoveryTCPPort uint64
	SentinelAddr           string
	SentinelPort           uint64
	SubscribeAllTopics     bool
	MaxPeerCount           uint64
	// Erigon Sync
	LoopBlockLimit uint64
	// Beacon API router configuration
	BeaconAPIRouter beacon_router_configuration.RouterConfiguration

	BootstrapNodes []string
	StaticPeers    []string
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

const (
	MainnetNetwork NetworkType = 1
	HoleskyNetwork NetworkType = 17000
	SepoliaNetwork NetworkType = 11155111
	GnosisNetwork  NetworkType = 100
	ChiadoNetwork  NetworkType = 10200

	CustomNetwork NetworkType = -1
)

const (
	MaxDialTimeout               = 15 * time.Second
	VersionLength  int           = 4
	MaxChunkSize   uint64        = 1 << 20 // 1 MiB
	ReqTimeout     time.Duration = 10 * time.Second
	RespTimeout    time.Duration = 15 * time.Second
)

const (
	SubDivisionFolderSize = 10_000
	SlotsPerDump          = 1536
)

var (
	MainnetBootstrapNodes = []string{
		// Teku team's bootnode
		"enr:-KG4QOtcP9X1FbIMOe17QNMKqDxCpm14jcX5tiOE4_TyMrFqbmhPZHK_ZPG2Gxb1GE2xdtodOfx9-cgvNtxnRyHEmC0ghGV0aDKQ9aX9QgAAAAD__________4JpZIJ2NIJpcIQDE8KdiXNlY3AyNTZrMaEDhpehBDbZjM_L9ek699Y7vhUJ-eAdMyQW_Fil522Y0fODdGNwgiMog3VkcIIjKA",
		"enr:-KG4QL-eqFoHy0cI31THvtZjpYUu_Jdw_MO7skQRJxY1g5HTN1A0epPCU6vi0gLGUgrzpU-ygeMSS8ewVxDpKfYmxMMGhGV0aDKQtTA_KgAAAAD__________4JpZIJ2NIJpcIQ2_DUbiXNlY3AyNTZrMaED8GJ2vzUqgL6-KD1xalo1CsmY4X1HaDnyl6Y_WayCo9GDdGNwgiMog3VkcIIjKA",
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
)

type NetworkConfig struct {
	GossipMaxSize                   uint64        `json:"gossip_max_size"`                    // The maximum allowed size of uncompressed gossip messages.
	GossipMaxSizeBellatrix          uint64        `json:"gossip_max_size_bellatrix"`          // The maximum allowed size of bellatrix uncompressed gossip messages.
	MaxRequestBlocks                uint64        `json:"max_request_blocks"`                 // Maximum number of blocks in a single request
	MaxChunkSize                    uint64        `json:"max_chunk_size"`                     // The maximum allowed size of uncompressed req/resp chunked responses.
	AttestationSubnetCount          uint64        `json:"attestation_subnet_count"`           // The number of attestation subnets used in the gossipsub protocol.
	TtfbTimeout                     time.Duration `json:"ttfbt_timeout"`                      // The maximum time to wait for first byte of request response (time-to-first-byte).
	RespTimeout                     time.Duration `json:"resp_timeout"`                       // The maximum time for complete response transfer.
	AttestationPropagationSlotRange uint64        `json:"attestation_propagation_slot_range"` // The maximum number of slots during which an attestation can be propagated.
	MaximumGossipClockDisparity     time.Duration `json:"maximum_gossip_clock_disparity"`     // The maximum milliseconds of clock disparity assumed between honest nodes.
	MessageDomainInvalidSnappy      [4]byte       `json:"message_domain_invalid_snappy"`      // 4-byte domain for gossip message-id isolation of invalid snappy messages
	MessageDomainValidSnappy        [4]byte       `json:"message_domain_valid_snappy"`        // 4-byte domain for gossip message-id isolation of valid snappy messages

	// DiscoveryV5 Config
	Eth2key                    string // ETH2Key is the ENR key of the Ethereum consensus object in an enr.
	AttSubnetKey               string // AttSubnetKey is the ENR key of the subnet bitfield in the enr.
	SyncCommsSubnetKey         string // SyncCommsSubnetKey is the ENR key of the sync committee subnet bitfield in the enr.
	MinimumPeersInSubnetSearch uint64 // PeersInSubnetSearch is the required amount of peers that we need to be able to lookup in a subnet search.

	BootNodes   []string
	StaticPeers []string
}

var NetworkConfigs map[NetworkType]NetworkConfig = map[NetworkType]NetworkConfig{
	MainnetNetwork: {
		GossipMaxSize:                   1 << 20, // 1 MiB
		GossipMaxSizeBellatrix:          10485760,
		MaxChunkSize:                    MaxChunkSize,
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		MaxRequestBlocks:                1 << 10, // 1024
		TtfbTimeout:                     ReqTimeout,
		RespTimeout:                     RespTimeout,
		MaximumGossipClockDisparity:     500 * time.Millisecond,
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       MainnetBootstrapNodes,
	},

	SepoliaNetwork: {
		GossipMaxSize:                   1 << 20, // 1 MiB
		GossipMaxSizeBellatrix:          10485760,
		MaxChunkSize:                    1 << 20, // 1 MiB
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		MaxRequestBlocks:                1 << 10, // 1024
		TtfbTimeout:                     ReqTimeout,
		RespTimeout:                     RespTimeout,
		MaximumGossipClockDisparity:     500 * time.Millisecond,
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       SepoliaBootstrapNodes,
	},

	GnosisNetwork: {
		GossipMaxSize:                   1 << 20, // 1 MiB
		GossipMaxSizeBellatrix:          10485760,
		MaxChunkSize:                    1 << 20, // 1 MiB
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		MaxRequestBlocks:                1 << 10, // 1024
		TtfbTimeout:                     ReqTimeout,
		RespTimeout:                     RespTimeout,
		MaximumGossipClockDisparity:     500 * time.Millisecond,
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       GnosisBootstrapNodes,
	},

	ChiadoNetwork: {
		GossipMaxSize:                   1 << 20, // 1 MiB
		GossipMaxSizeBellatrix:          10485760,
		MaxChunkSize:                    1 << 20, // 1 MiB
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		MaxRequestBlocks:                1 << 10, // 1024
		TtfbTimeout:                     ReqTimeout,
		RespTimeout:                     RespTimeout,
		MaximumGossipClockDisparity:     500 * time.Millisecond,
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       ChiadoBootstrapNodes,
	},

	HoleskyNetwork: {
		GossipMaxSize:                   1 << 20, // 1 MiB
		GossipMaxSizeBellatrix:          10485760,
		MaxChunkSize:                    1 << 20, // 1 MiB
		AttestationSubnetCount:          64,
		AttestationPropagationSlotRange: 32,
		MaxRequestBlocks:                1 << 10, // 1024
		TtfbTimeout:                     ReqTimeout,
		RespTimeout:                     RespTimeout,
		MaximumGossipClockDisparity:     500 * time.Millisecond,
		MessageDomainInvalidSnappy:      [4]byte{00, 00, 00, 00},
		MessageDomainValidSnappy:        [4]byte{01, 00, 00, 00},
		Eth2key:                         "eth2",
		AttSubnetKey:                    "attnets",
		SyncCommsSubnetKey:              "syncnets",
		MinimumPeersInSubnetSearch:      20,
		BootNodes:                       HoleskyBootstrapNodes,
	},
}

// Trusted checkpoint sync endpoints: https://eth-clients.github.io/checkpoint-sync-endpoints/
var CheckpointSyncEndpoints = map[NetworkType][]string{
	MainnetNetwork: {
		"https://sync.invis.tools/eth/v2/debug/beacon/states/finalized",
		"https://mainnet-checkpoint-sync.attestant.io/eth/v2/debug/beacon/states/finalized",
		//"https://mainnet.checkpoint.sigp.io/eth/v2/debug/beacon/states/finalized",
		"https://mainnet-checkpoint-sync.stakely.io/eth/v2/debug/beacon/states/finalized",
		"https://checkpointz.pietjepuk.net/eth/v2/debug/beacon/states/finalized",
	},
	SepoliaNetwork: {
		//"https://beaconstate-sepolia.chainsafe.io/eth/v2/debug/beacon/states/finalized",
		//"https://sepolia.beaconstate.info/eth/v2/debug/beacon/states/finalized",
		"https://checkpoint-sync.sepolia.ethpandaops.io/eth/v2/debug/beacon/states/finalized",
	},
	GnosisNetwork: {
		//"https://checkpoint.gnosis.gateway.fm/eth/v2/debug/beacon/states/finalized",
		"https://checkpoint.gnosischain.com/eth/v2/debug/beacon/states/finalized",
	},
	ChiadoNetwork: {
		"https://checkpoint.chiadochain.net/eth/v2/debug/beacon/states/finalized",
	},
	HoleskyNetwork: {
		"https://holesky.beaconstate.ethstaker.cc/eth/v2/debug/beacon/states/finalized",
		"https://beaconstate-holesky.chainsafe.io/eth/v2/debug/beacon/states/finalized",
		"https://holesky.beaconstate.info/eth/v2/debug/beacon/states/finalized",
		"https://checkpoint-sync.holesky.ethpandaops.io/eth/v2/debug/beacon/states/finalized",
	},
}

// ConfigurableCheckpointsURLs is customized by the user to specify the checkpoint sync endpoints.
var ConfigurableCheckpointsURLs = []string{}

// MinEpochsForBlockRequests  equal to MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT / 2
func (b *BeaconChainConfig) MinEpochsForBlockRequests() uint64 {
	return b.MinValidatorWithdrawabilityDelay + (b.ChurnLimitQuotient)/2

}

type ConfigByte byte

func (b ConfigByte) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"0x%x\"", b)), nil
}

type ConfigForkVersion uint32

func (v ConfigForkVersion) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"0x%08x\"", v)), nil
}

type VersionScheduleEntry struct {
	Epoch        uint64 `yaml:"EPOCH" json:"EPOCH,string"`
	StateVersion StateVersion
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
	PresetBase                       string `yaml:"PRESET_BASE" spec:"true" json:"PRESET_BASE"`                                                            // PresetBase represents the underlying spec preset this config is based on.
	ConfigName                       string `yaml:"CONFIG_NAME" spec:"true" json:"CONFIG_NAME"`                                                            // ConfigName for allowing an easy human-readable way of knowing what chain is being used.
	TargetCommitteeSize              uint64 `yaml:"TARGET_COMMITTEE_SIZE" spec:"true" json:"TARGET_COMMITTEE_SIZE,string"`                                 // TargetCommitteeSize is the number of validators in a committee when the chain is healthy.
	MaxValidatorsPerCommittee        uint64 `yaml:"MAX_VALIDATORS_PER_COMMITTEE" spec:"true" json:"MAX_VALIDATORS_PER_COMMITTEE,string"`                   // MaxValidatorsPerCommittee defines the upper bound of the size of a committee.
	MaxCommitteesPerSlot             uint64 `yaml:"MAX_COMMITTEES_PER_SLOT" spec:"true" json:"MAX_COMMITTEES_PER_SLOT,string"`                             // MaxCommitteesPerSlot defines the max amount of committee in a single slot.
	MinPerEpochChurnLimit            uint64 `yaml:"MIN_PER_EPOCH_CHURN_LIMIT" spec:"true" json:"MIN_PER_EPOCH_CHURN_LIMIT,string"`                         // MinPerEpochChurnLimit is the minimum amount of churn allotted for validator rotations.
	ChurnLimitQuotient               uint64 `yaml:"CHURN_LIMIT_QUOTIENT" spec:"true" json:"CHURN_LIMIT_QUOTIENT,string"`                                   // ChurnLimitQuotient is used to determine the limit of how many validators can rotate per epoch.
	MaxPerEpochActivationChurnLimit  uint64 `yaml:"MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT" spec:"true" json:"MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT,string"`   // MaxPerEpochActivationChurnLimit defines the maximum amount of churn allowed in one epoch from deneb.
	ShuffleRoundCount                uint64 `yaml:"SHUFFLE_ROUND_COUNT" spec:"true" json:"SHUFFLE_ROUND_COUNT,string"`                                     // ShuffleRoundCount is used for retrieving the permuted index.
	MinGenesisActiveValidatorCount   uint64 `yaml:"MIN_GENESIS_ACTIVE_VALIDATOR_COUNT" spec:"true" json:"MIN_GENESIS_ACTIVE_VALIDATOR_COUNT,string"`       // MinGenesisActiveValidatorCount defines how many validator deposits needed to kick off beacon chain.
	MinGenesisTime                   uint64 `yaml:"MIN_GENESIS_TIME" spec:"true" json:"MIN_GENESIS_TIME,string"`                                           // MinGenesisTime is the time that needed to pass before kicking off beacon chain.
	TargetAggregatorsPerCommittee    uint64 `yaml:"TARGET_AGGREGATORS_PER_COMMITTEE" spec:"true" json:"TARGET_AGGREGATORS_PER_COMMITTEE,string"`           // TargetAggregatorsPerCommittee defines the number of aggregators inside one committee.
	HysteresisQuotient               uint64 `yaml:"HYSTERESIS_QUOTIENT" spec:"true" json:"HYSTERESIS_QUOTIENT,string"`                                     // HysteresisQuotient defines the hysteresis quotient for effective balance calculations.
	HysteresisDownwardMultiplier     uint64 `yaml:"HYSTERESIS_DOWNWARD_MULTIPLIER" spec:"true" json:"HYSTERESIS_DOWNWARD_MULTIPLIER,string"`               // HysteresisDownwardMultiplier defines the hysteresis downward multiplier for effective balance calculations.
	HysteresisUpwardMultiplier       uint64 `yaml:"HYSTERESIS_UPWARD_MULTIPLIER" spec:"true" json:"HYSTERESIS_UPWARD_MULTIPLIER,string"`                   // HysteresisUpwardMultiplier defines the hysteresis upward multiplier for effective balance calculations.
	MinEpochsForBlobsSidecarsRequest uint64 `yaml:"MIN_EPOCHS_FOR_BLOBS_SIDECARS_REQUEST" spec:"true" json:"MIN_EPOCHS_FOR_BLOBS_SIDECARS_REQUEST,string"` // MinEpochsForBlobsSidecarsRequest defines the minimum number of epochs to wait before requesting blobs sidecars.

	// Gwei value constants.
	MinDepositAmount          uint64 `yaml:"MIN_DEPOSIT_AMOUNT" spec:"true" json:"MIN_DEPOSIT_AMOUNT,string"`                   // MinDepositAmount is the minimum amount of Gwei a validator can send to the deposit contract at once (lower amounts will be reverted).
	MaxEffectiveBalance       uint64 `yaml:"MAX_EFFECTIVE_BALANCE" spec:"true" json:"MAX_EFFECTIVE_BALANCE,string"`             // MaxEffectiveBalance is the maximal amount of Gwei that is effective for staking.
	EjectionBalance           uint64 `yaml:"EJECTION_BALANCE" spec:"true" json:"EJECTION_BALANCE,string"`                       // EjectionBalance is the minimal GWei a validator needs to have before ejected.
	EffectiveBalanceIncrement uint64 `yaml:"EFFECTIVE_BALANCE_INCREMENT" spec:"true" json:"EFFECTIVE_BALANCE_INCREMENT,string"` // EffectiveBalanceIncrement is used for converting the high balance into the low balance for validators.

	// Initial value constants.
	BLSWithdrawalPrefixByte         ConfigByte `yaml:"BLS_WITHDRAWAL_PREFIX" spec:"true" json:"BLS_WITHDRAWAL_PREFIX"`                    // BLSWithdrawalPrefixByte is used for BLS withdrawal and it's the first byte.
	ETH1AddressWithdrawalPrefixByte ConfigByte `yaml:"ETH1_ADDRESS_WITHDRAWAL_PREFIX" spec:"true" json:"ETH1AddressWithdrawalPrefixByte"` // ETH1AddressWithdrawalPrefixByte is used for withdrawals and it's the first byte.

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

	// BLS domain values.
	DomainBeaconProposer              libcommon.Bytes4 `yaml:"DOMAIN_BEACON_PROPOSER" spec:"true" json:"DOMAIN_BEACON_PROPOSER"`                               // DomainBeaconProposer defines the BLS signature domain for beacon proposal verification.
	DomainRandao                      libcommon.Bytes4 `yaml:"DOMAIN_RANDAO" spec:"true" json:"DOMAIN_RANDAO"`                                                 // DomainRandao defines the BLS signature domain for randao verification.
	DomainBeaconAttester              libcommon.Bytes4 `yaml:"DOMAIN_BEACON_ATTESTER" spec:"true" json:"DOMAIN_BEACON_ATTESTER"`                               // DomainBeaconAttester defines the BLS signature domain for attestation verification.
	DomainDeposit                     libcommon.Bytes4 `yaml:"DOMAIN_DEPOSIT" spec:"true" json:"DOMAIN_DEPOSIT"`                                               // DomainDeposit defines the BLS signature domain for deposit verification.
	DomainVoluntaryExit               libcommon.Bytes4 `yaml:"DOMAIN_VOLUNTARY_EXIT" spec:"true" json:"DOMAIN_VOLUNTARY_EXIT"`                                 // DomainVoluntaryExit defines the BLS signature domain for exit verification.
	DomainSelectionProof              libcommon.Bytes4 `yaml:"DOMAIN_SELECTION_PROOF" spec:"true" json:"DOMAIN_SELECTION_PROOF"`                               // DomainSelectionProof defines the BLS signature domain for selection proof.
	DomainAggregateAndProof           libcommon.Bytes4 `yaml:"DOMAIN_AGGREGATE_AND_PROOF" spec:"true" json:"DOMAIN_AGGREGATE_AND_PROOF"`                       // DomainAggregateAndProof defines the BLS signature domain for aggregate and proof.
	DomainSyncCommittee               libcommon.Bytes4 `yaml:"DOMAIN_SYNC_COMMITTEE" spec:"true" json:"DOMAIN_SYNC_COMMITTEE"`                                 // DomainVoluntaryExit defines the BLS signature domain for sync committee.
	DomainSyncCommitteeSelectionProof libcommon.Bytes4 `yaml:"DOMAIN_SYNC_COMMITTEE_SELECTION_PROOF" spec:"true" json:"DOMAIN_SYNC_COMMITTEE_SELECTION_PROOF"` // DomainSelectionProof defines the BLS signature domain for sync committee selection proof.
	DomainContributionAndProof        libcommon.Bytes4 `yaml:"DOMAIN_CONTRIBUTION_AND_PROOF" spec:"true" json:"DOMAIN_CONTRIBUTION_AND_PROOF"`                 // DomainAggregateAndProof defines the BLS signature domain for contribution and proof.
	DomainApplicationMask             libcommon.Bytes4 `yaml:"DOMAIN_APPLICATION_MASK" spec:"true" json:"DOMAIN_APPLICATION_MASK"`                             // DomainApplicationMask defines the BLS signature domain for application mask.
	DomainApplicationBuilder          libcommon.Bytes4 `json:"-"`                                                                                              // DomainApplicationBuilder defines the BLS signature domain for application builder.
	DomainBLSToExecutionChange        libcommon.Bytes4 `json:"-"`                                                                                              // DomainBLSToExecutionChange defines the BLS signature domain to change withdrawal addresses to ETH1 prefix
	DomainBlobSideCar                 libcommon.Bytes4 `yaml:"DOMAIN_BLOB_SIDECAR" spec:"true" json:"DOMAIN_BLOB_SIDECAR"`                                     // DomainBlobSideCar defines the BLS signature domain for blob sidecar verification

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

	ForkVersionSchedule map[libcommon.Bytes4]VersionScheduleEntry `json:"-"` // Schedule of fork epochs by version.

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
	TerminalBlockHash                libcommon.Hash    `yaml:"TERMINAL_BLOCK_HASH" spec:"true" json:"TERMINAL_BLOCK_HASH"`                                          // TerminalBlockHash of beacon chain.
	TerminalBlockHashActivationEpoch uint64            `yaml:"TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH" spec:"true" json:"TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH,string"` // TerminalBlockHashActivationEpoch of beacon chain.
	TerminalTotalDifficulty          string            `yaml:"TERMINAL_TOTAL_DIFFICULTY" spec:"true"  json:"TERMINAL_TOTAL_DIFFICULTY"`                             // TerminalTotalDifficulty is part of the experimental Bellatrix spec. This value is type is currently TBD.
	DefaultFeeRecipient              libcommon.Address `json:"-"`                                                                                                   // DefaultFeeRecipient where the transaction fee goes to.
	DefaultBuilderGasLimit           uint64            `json:"-"`                                                                                                   // DefaultBuilderGasLimit is the default used to set the gaslimit for the Builder APIs, typically at around 30M wei.

	// Mev-boost circuit breaker
	MaxBuilderConsecutiveMissedSlots uint64 `json:"-"` // MaxBuilderConsecutiveMissedSlots defines the number of consecutive skip slot to fallback from using relay/builder to local execution engine for block construction.
	MaxBuilderEpochMissedSlots       uint64 `json:"-"` // MaxBuilderEpochMissedSlots is defines the number of total skip slot (per epoch rolling windows) to fallback from using relay/builder to local execution engine for block construction.

	MaxBlobGasPerBlock uint64 `yaml:"MAX_BLOB_GAS_PER_BLOCK" json:"MAX_BLOB_GAS_PER_BLOCK,string"` // MaxBlobGasPerBlock defines the maximum gas limit for blob sidecar per block.
	MaxBlobsPerBlock   uint64 `yaml:"MAX_BLOBS_PER_BLOCK" json:"MAX_BLOBS_PER_BLOCK,string"`       // MaxBlobsPerBlock defines the maximum number of blobs per block.
	// Whisk
	WhiskEpochsPerShufflingPhase uint64 `yaml:"WHISK_EPOCHS_PER_SHUFFLING_PHASE" spec:"true" json:"WHISK_EPOCHS_PER_SHUFFLING_PHASE,string"` // WhiskEpochsPerShufflingPhase defines the number of epochs per shuffling phase.
	WhiskProposerSelectionGap    uint64 `yaml:"WHISK_PROPOSER_SELECTION_GAP" spec:"true" json:"WHISK_PROPOSER_SELECTION_GAP,string"`         // WhiskProposerSelectionGap defines the proposer selection gap.

	// EIP7594
	NumberOfColumns              uint64 `yaml:"NUMBER_OF_COLUMNS" spec:"true" json:"NUMBER_OF_COLUMNS,string"`                               // NumberOfColumns defines the number of columns in the extended matrix.
	MaxCellsInExtendedMatrix     uint64 `yaml:"MAX_CELLS_IN_EXTENDED_MATRIX" spec:"true" json:"MAX_CELLS_IN_EXTENDED_MATRIX,string"`         // MaxCellsInExtendedMatrix defines the maximum number of cells in the extended matrix.
	DataColumnSidecarSubnetCount uint64 `yaml:"DATA_COLUMN_SIDECAR_SUBNET_COUNT" spec:"true" json:"DATA_COLUMN_SIDECAR_SUBNET_COUNT,string"` // DataColumnSidecarSubnetCount defines the number of sidecars in the data column subnet.
	MaxRequestDataColumnSidecars uint64 `yaml:"MAX_REQUEST_DATA_COLUMN_SIDECARS" spec:"true" json:"MAX_REQUEST_DATA_COLUMN_SIDECARS,string"` // MaxRequestDataColumnSidecars defines the maximum number of data column sidecars that can be requested.
	SamplesPerSlot               uint64 `yaml:"SAMPLES_PER_SLOT" spec:"true" json:"SAMPLES_PER_SLOT,string"`                                 // SamplesPerSlot defines the number of samples per slot.
	CustodyRequirement           uint64 `yaml:"CUSTODY_REQUIREMENT" spec:"true" json:"CUSTODY_REQUIREMENT,string"`                           // CustodyRequirement defines the custody requirement.
	TargetNumberOfPeers          uint64 `yaml:"TARGET_NUMBER_OF_PEERS" spec:"true" json:"TARGET_NUMBER_OF_PEERS,string"`                     // TargetNumberOfPeers defines the target number of peers.

	// Electra
	MinPerEpochChurnLimitElectra        uint64 `yaml:"MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA" spec:"true" json:"MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA,string"`                 // MinPerEpochChurnLimitElectra defines the minimum per epoch churn limit for Electra.
	MaxPerEpochActivationExitChurnLimit uint64 `yaml:"MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT" spec:"true" json:"MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT,string"` // MaxPerEpochActivationExitChurnLimit defines the maximum per epoch activation exit churn limit for Electra.
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
	forkEpochList := []uint64{b.AltairForkEpoch, b.BellatrixForkEpoch, b.CapellaForkEpoch, b.DenebForkEpoch}
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
}

func configForkSchedule(b *BeaconChainConfig) map[libcommon.Bytes4]VersionScheduleEntry {
	fvs := map[libcommon.Bytes4]VersionScheduleEntry{}
	fvs[utils.Uint32ToBytes4(uint32(b.GenesisForkVersion))] = VersionScheduleEntry{b.GenesisSlot / b.SlotsPerEpoch, Phase0Version}
	fvs[utils.Uint32ToBytes4(uint32(b.AltairForkVersion))] = VersionScheduleEntry{b.AltairForkEpoch, AltairVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.BellatrixForkVersion))] = VersionScheduleEntry{b.BellatrixForkEpoch, BellatrixVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.CapellaForkVersion))] = VersionScheduleEntry{b.CapellaForkEpoch, CapellaVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.DenebForkVersion))] = VersionScheduleEntry{b.DenebForkEpoch, DenebVersion}
	fvs[utils.Uint32ToBytes4(uint32(b.ElectraForkVersion))] = VersionScheduleEntry{b.ElectraForkEpoch, ElectraVersion}
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
	MinEpochsForBlobsSidecarsRequest: 4096,

	// Gwei value constants.
	MinDepositAmount:          1 * 1e9,
	MaxEffectiveBalance:       32 * 1e9,
	EjectionBalance:           16 * 1e9,
	EffectiveBalanceIncrement: 1 * 1e9,

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
	// ElectraForkVersion:   Not Set,
	ElectraForkEpoch: math.MaxUint64,

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
	DefaultBuilderGasLimit:           uint64(30000000),

	// Mevboost circuit breaker
	MaxBuilderConsecutiveMissedSlots: 3,
	MaxBuilderEpochMissedSlots:       8,

	MaxBlobGasPerBlock: 786432,
	MaxBlobsPerBlock:   6,

	WhiskEpochsPerShufflingPhase: 256,
	WhiskProposerSelectionGap:    2,

	NumberOfColumns:              128,
	MaxCellsInExtendedMatrix:     768,
	DataColumnSidecarSubnetCount: 32,
	MaxRequestDataColumnSidecars: 16384,
	SamplesPerSlot:               8,
	CustodyRequirement:           1,
	TargetNumberOfPeers:          70,

	MinPerEpochChurnLimitElectra:        128000000000,
	MaxPerEpochActivationExitChurnLimit: 256000000000,
}

func mainnetConfig() BeaconChainConfig {
	cfg := MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	return cfg
}

func CustomConfig(configFile string) (BeaconChainConfig, error) {
	cfg := MainnetBeaconConfig
	b, err := os.ReadFile(configFile) // just pass the file name
	if err != nil {
		return BeaconChainConfig{}, nil
	}
	err = yaml.Unmarshal(b, &cfg)
	cfg.InitializeForkSchedule()
	return cfg, err
}

func sepoliaConfig() BeaconChainConfig {
	cfg := MainnetBeaconConfig
	cfg.MinGenesisTime = 1655647200
	cfg.GenesisDelay = 86400
	cfg.MinGenesisActiveValidatorCount = 1300
	cfg.ConfigName = "sepolia"

	cfg.GenesisForkVersion = 0x90000069
	cfg.SecondsPerETH1Block = 14
	cfg.DepositChainID = uint64(SepoliaNetwork)
	cfg.DepositNetworkID = uint64(SepoliaNetwork)
	cfg.AltairForkEpoch = 50
	cfg.AltairForkVersion = 0x90000070
	cfg.BellatrixForkEpoch = 100
	cfg.BellatrixForkVersion = 0x90000071
	cfg.CapellaForkEpoch = 56832
	cfg.CapellaForkVersion = 0x90000072
	cfg.DenebForkEpoch = 132608
	cfg.DenebForkVersion = 0x90000073
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
	cfg.DepositChainID = uint64(HoleskyNetwork)
	cfg.DepositNetworkID = uint64(HoleskyNetwork)

	cfg.AltairForkEpoch = 0
	cfg.AltairForkVersion = 0x02017000
	cfg.BellatrixForkEpoch = 0
	cfg.BellatrixForkVersion = 0x03017000
	cfg.CapellaForkEpoch = 256
	cfg.CapellaForkVersion = 0x04017000
	cfg.DenebForkEpoch = 29696
	cfg.DenebForkVersion = 0x05017000
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
	cfg.DepositChainID = uint64(GnosisNetwork)
	cfg.DepositNetworkID = uint64(GnosisNetwork)
	cfg.AltairForkEpoch = 512
	cfg.AltairForkVersion = 0x01000064
	cfg.BellatrixForkEpoch = 385536
	cfg.BellatrixForkVersion = 0x02000064
	cfg.CapellaForkEpoch = 648704
	cfg.CapellaForkVersion = 0x03000064
	cfg.TerminalTotalDifficulty = "8626000000000000000000058750000000000000000000"
	cfg.DepositContractAddress = "0x0B98057eA310F4d31F2a452B414647007d1645d9"
	cfg.BaseRewardFactor = 25
	cfg.SlotsPerEpoch = 16
	cfg.EpochsPerSyncCommitteePeriod = 512
	cfg.DenebForkEpoch = 889856
	cfg.DenebForkVersion = 0x04000064
	cfg.InactivityScoreRecoveryRate = 16
	cfg.InactivityScoreBias = 4
	cfg.MaxWithdrawalsPerPayload = 8
	cfg.MaxValidatorsPerWithdrawalsSweep = 8192
	cfg.MaxBlobsPerBlock = 2
	cfg.MinEpochsForBlobsSidecarsRequest = 16384
	cfg.MaxPerEpochActivationChurnLimit = 2
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
	cfg.DepositChainID = uint64(ChiadoNetwork)
	cfg.DepositNetworkID = uint64(ChiadoNetwork)
	cfg.AltairForkEpoch = 90
	cfg.AltairForkVersion = 0x0100006f
	cfg.BellatrixForkEpoch = 180
	cfg.BellatrixForkVersion = 0x0200006f
	cfg.CapellaForkEpoch = 244224
	cfg.CapellaForkVersion = 0x0300006f
	cfg.DenebForkEpoch = 516608
	cfg.DenebForkVersion = 0x0400006f
	cfg.TerminalTotalDifficulty = "231707791542740786049188744689299064356246512"
	cfg.DepositContractAddress = "0xb97036A26259B7147018913bD58a774cf91acf25"
	cfg.BaseRewardFactor = 25
	cfg.SlotsPerEpoch = 16
	cfg.EpochsPerSyncCommitteePeriod = 512
	cfg.MaxWithdrawalsPerPayload = 8
	cfg.MaxValidatorsPerWithdrawalsSweep = 8192
	cfg.MaxBlobsPerBlock = 2
	cfg.MinEpochsForBlobsSidecarsRequest = 16384
	cfg.MaxPerEpochActivationChurnLimit = 2
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
	default:
		panic("not implemented")
	}
}

// Beacon configs
var BeaconConfigs map[NetworkType]BeaconChainConfig = map[NetworkType]BeaconChainConfig{
	MainnetNetwork: mainnetConfig(),
	SepoliaNetwork: sepoliaConfig(),
	HoleskyNetwork: holeskyConfig(),
	GnosisNetwork:  gnosisConfig(),
	ChiadoNetwork:  chiadoConfig(),
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
		networkCfg, beaconCfg := GetConfigsByNetwork(MainnetNetwork)
		return networkCfg, beaconCfg, MainnetNetwork, nil
	case networkname.Sepolia:
		networkCfg, beaconCfg := GetConfigsByNetwork(SepoliaNetwork)
		return networkCfg, beaconCfg, SepoliaNetwork, nil
	case networkname.Gnosis:
		networkCfg, beaconCfg := GetConfigsByNetwork(GnosisNetwork)
		return networkCfg, beaconCfg, GnosisNetwork, nil
	case networkname.Chiado:
		networkCfg, beaconCfg := GetConfigsByNetwork(ChiadoNetwork)
		return networkCfg, beaconCfg, ChiadoNetwork, nil
	case networkname.Holesky:
		networkCfg, beaconCfg := GetConfigsByNetwork(HoleskyNetwork)
		return networkCfg, beaconCfg, HoleskyNetwork, nil
	default:
		return nil, nil, MainnetNetwork, errors.New("chain not found")
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
// 1 is Ethereum Mainnet
// 11155111 is Sepolia Testnet
// 100 is Gnosis Mainnet
// 10200 is Chiado Testnet
func EmbeddedSupported(id uint64) bool {
	return id == 1 ||
		id == 17000 ||
		id == 11155111 ||
		id == 100 ||
		id == 10200
}

// Subset of supported networks where embedded CL is stable enough
// (sufficient number of light-client peers) as to be enabled by default
func EmbeddedEnabledByDefault(id uint64) bool {
	return id == 1 || id == 5 || id == 11155111
}

func SupportBackfilling(networkId uint64) bool {
	return networkId == uint64(MainnetNetwork) ||
		networkId == uint64(SepoliaNetwork) ||
		networkId == uint64(GnosisNetwork) ||
		networkId == uint64(ChiadoNetwork) ||
		networkId == uint64(HoleskyNetwork)
}

func EpochToPaths(slot uint64, config *BeaconChainConfig, suffix string) (string, string) {
	folderPath := path.Clean(strconv.FormatUint(slot/SubDivisionFolderSize, 10))
	return folderPath, path.Clean(fmt.Sprintf("%s/%d.%s.sz", folderPath, slot, suffix))
}

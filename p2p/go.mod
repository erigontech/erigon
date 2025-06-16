module github.com/erigontech/erigon-p2p

go 1.23.0

toolchain go1.23.6

replace (
	github.com/erigontech/erigon-db => ../erigon-db
	github.com/erigontech/erigon-lib => ../erigon-lib
)

require (
	github.com/erigontech/erigon-db v0.0.0-00010101000000-000000000000
	github.com/erigontech/erigon-lib v0.0.0-00010101000000-000000000000
)

replace (
	github.com/anacrolix/torrent => github.com/erigontech/torrent v1.54.3-alpha-1
	github.com/crate-crypto/go-kzg-4844 => github.com/erigontech/go-kzg-4844 v0.0.0-20250130131058-ce13be60bc86
	github.com/holiman/bloomfilter/v2 => github.com/AskAlexSharov/bloomfilter/v2 v2.0.9
)

require (
	github.com/c2h5oh/datasize v0.0.0-20231215233829-aa82cc1e6500
	github.com/davecgh/go-spew v1.1.1
	github.com/deckarep/golang-set/v2 v2.8.0
	github.com/erigontech/mdbx-go v0.39.8
	github.com/erigontech/secp256k1 v1.2.0
	github.com/golang/snappy v0.0.1
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/holiman/uint256 v1.3.2
	github.com/huin/goupnp v1.3.0
	github.com/jackpal/go-nat-pmp v1.0.2
	github.com/pion/stun v0.3.5
	github.com/stretchr/testify v1.10.0
	go.uber.org/mock v0.5.0
	golang.org/x/crypto v0.39.0
	golang.org/x/sync v0.15.0
	golang.org/x/time v0.12.0
	google.golang.org/grpc v1.72.1
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/RoaringBitmap/roaring/v2 v2.5.0 // indirect
	github.com/anacrolix/missinggo v1.3.0 // indirect
	github.com/anacrolix/missinggo/v2 v2.7.3 // indirect
	github.com/anacrolix/torrent v1.52.6-0.20231201115409-7ea994b6bbd8 // indirect
	github.com/benesch/cgosymbolizer v0.0.0-20190515212042-bec6fe6e597b // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.20.0 // indirect
	github.com/bradfitz/iter v0.0.0-20191230175014-e8f45d346db8 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cilium/ebpf v0.11.0 // indirect
	github.com/consensys/bavard v0.1.29 // indirect
	github.com/consensys/gnark-crypto v0.17.0 // indirect
	github.com/containerd/cgroups/v3 v3.0.3 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/crate-crypto/go-eth-kzg v1.3.0 // indirect
	github.com/crate-crypto/go-ipa v0.0.0-20221111143132-9aa5d42120bc // indirect
	github.com/crate-crypto/go-kzg-4844 v1.1.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.3.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/edsrzf/mmap-go v1.2.0 // indirect
	github.com/elastic/go-freelru v0.16.0 // indirect
	github.com/erigontech/erigon-snapshot v1.3.1-0.20250501041114-4a48ac232c83 // indirect
	github.com/erigontech/speedtest v0.0.2 // indirect
	github.com/gballet/go-verkle v0.0.0-20221121182333-31427a1f2d35 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/godbus/dbus/v5 v5.0.4 // indirect
	github.com/gofrs/flock v0.12.1 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/holiman/bloomfilter/v2 v2.0.3 // indirect
	github.com/huandu/xstrings v1.4.0 // indirect
	github.com/ianlancetaylor/cgosymbolizer v0.0.0-20241129212102-9c50ad6b591e // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mmcloughlin/addchain v0.4.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nyaosorg/go-windows-shortcut v0.0.0-20220529122037-8b0c89bca4c4 // indirect
	github.com/opencontainers/runtime-spec v1.2.0 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.22.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/protolambda/ztyp v0.2.2 // indirect
	github.com/shirou/gopsutil/v4 v4.24.8 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/tidwall/btree v1.6.0 // indirect
	github.com/tklauser/go-sysconf v0.3.14 // indirect
	github.com/tklauser/numcpus v0.8.0 // indirect
	github.com/ugorji/go/codec v1.2.13 // indirect
	github.com/valyala/fastjson v1.6.4 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/exp v0.0.0-20250606033433-dcc06ee1d476 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	rsc.io/tmplfunc v0.0.3 // indirect
)

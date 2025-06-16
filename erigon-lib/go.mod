module github.com/erigontech/erigon-lib

go 1.24

replace (
	github.com/crate-crypto/go-kzg-4844 => github.com/erigontech/go-kzg-4844 v0.0.0-20250130131058-ce13be60bc86
	github.com/holiman/bloomfilter/v2 => github.com/AskAlexSharov/bloomfilter/v2 v2.0.9
)

require (
	github.com/erigontech/erigon-snapshot v1.3.1-0.20250501041114-4a48ac232c83
	github.com/erigontech/interfaces v0.0.0-20250602082224-daf6311709c1
	github.com/erigontech/mdbx-go v0.39.8
	github.com/erigontech/secp256k1 v1.2.0
)

require (
	github.com/RoaringBitmap/roaring/v2 v2.5.0
	github.com/anacrolix/chansync v0.6.0
	github.com/anacrolix/dht/v2 v2.21.1
	github.com/anacrolix/generics v0.0.3-0.20250526144502-593be7092deb
	github.com/anacrolix/go-libutp v1.3.2
	github.com/anacrolix/log v0.16.1-0.20250526073428-5cb74e15092b
	github.com/anacrolix/missinggo/v2 v2.8.1-0.20250604020133-83210197e79c
	github.com/anacrolix/torrent v1.58.2-0.20250604010703-7c29c120a504
	github.com/benesch/cgosymbolizer v0.0.0-20190515212042-bec6fe6e597b
	github.com/c2h5oh/datasize v0.0.0-20231215233829-aa82cc1e6500
	github.com/containerd/cgroups/v3 v3.0.3
	github.com/crate-crypto/go-eth-kzg v1.3.0
	github.com/crate-crypto/go-ipa v0.0.0-20221111143132-9aa5d42120bc
	github.com/crate-crypto/go-kzg-4844 v1.1.0
	github.com/davecgh/go-spew v1.1.1
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.3.0
	github.com/edsrzf/mmap-go v1.2.0
	github.com/elastic/go-freelru v0.16.0
	github.com/erigontech/speedtest v0.0.2
	github.com/gballet/go-verkle v0.0.0-20221121182333-31427a1f2d35
	github.com/go-quicktest/qt v1.101.0
	github.com/go-stack/stack v1.8.1
	github.com/go-test/deep v1.1.1
	github.com/gofrs/flock v0.12.1
	github.com/golang-jwt/jwt/v4 v4.5.2
	github.com/google/btree v1.1.3
	github.com/gorilla/websocket v1.5.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/hashicorp/go-retryablehttp v0.7.7
	github.com/holiman/bloomfilter/v2 v2.0.3
	github.com/holiman/uint256 v1.3.2
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.18.0
	github.com/mattn/go-colorable v0.1.13
	github.com/mattn/go-isatty v0.0.20
	github.com/nyaosorg/go-windows-shortcut v0.0.0-20220529122037-8b0c89bca4c4
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58
	github.com/pelletier/go-toml/v2 v2.2.4
	github.com/prometheus/client_golang v1.22.0
	github.com/prometheus/client_model v0.6.1
	github.com/protolambda/ztyp v0.2.2
	github.com/quasilyte/go-ruleguard/dsl v0.3.22
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417
	github.com/shirou/gopsutil/v4 v4.24.8
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.10.0
	github.com/tidwall/btree v1.6.0
	github.com/ugorji/go/codec v1.2.12
	github.com/valyala/fastjson v1.6.4
	go.uber.org/mock v0.5.0
	golang.org/x/crypto v0.39.0
	golang.org/x/exp v0.0.0-20250606033433-dcc06ee1d476
	golang.org/x/net v0.41.0
	golang.org/x/sync v0.15.0
	golang.org/x/sys v0.33.0
	golang.org/x/time v0.12.0
	google.golang.org/grpc v1.72.1
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/RoaringBitmap/roaring v1.9.4 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/ianlancetaylor/cgosymbolizer v0.0.0-20241129212102-9c50ad6b591e // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/runtime-spec v1.2.0 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel/metric v1.34.0 // indirect
	golang.org/x/mod v0.25.0 // indirect
	golang.org/x/tools v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
	modernc.org/libc v1.55.3 // indirect
	modernc.org/memory v1.8.0 // indirect
	modernc.org/sqlite v1.33.1 // indirect
)

require (
	github.com/ajwerner/btree v0.0.0-20211221152037-f427b3e689c0 // indirect
	github.com/alecthomas/assert/v2 v2.8.1 // indirect
	github.com/alecthomas/atomic v0.1.0-alpha2 // indirect
	github.com/anacrolix/envpprof v1.3.0 // indirect
	github.com/anacrolix/missinggo v1.3.0 // indirect
	github.com/anacrolix/missinggo/perf v1.0.0 // indirect
	github.com/anacrolix/mmsg v1.0.1 // indirect
	github.com/anacrolix/multiless v0.4.0 // indirect
	github.com/anacrolix/stm v0.4.1-0.20221221005312-96d17df0e496 // indirect
	github.com/anacrolix/sync v0.5.4 // indirect
	github.com/anacrolix/upnp v0.1.4 // indirect
	github.com/anacrolix/utp v0.1.0 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/benbjohnson/immutable v0.4.1-0.20221220213129-8932b999621d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.20.0 // indirect
	github.com/bradfitz/iter v0.0.0-20191230175014-e8f45d346db8 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cilium/ebpf v0.11.0 // indirect
	github.com/consensys/bavard v0.1.29 // indirect
	github.com/consensys/gnark-crypto v0.17.0 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-llsqlite/adapter v0.0.0-20230927005056-7f5ce7f0c916 // indirect
	github.com/go-llsqlite/crawshaw v0.5.6-0.20250312230104-194977a03421 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/godbus/dbus/v5 v5.0.4 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/huandu/xstrings v1.4.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.3 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20220913051719-115f729f3c8c // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mmcloughlin/addchain v0.4.0 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/multiformats/go-multihash v0.2.3 // indirect
	github.com/multiformats/go-varint v0.0.6 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/pion/datachannel v1.5.9 // indirect
	github.com/pion/dtls/v3 v3.0.3 // indirect
	github.com/pion/ice/v4 v4.0.2 // indirect
	github.com/pion/interceptor v0.1.40 // indirect
	github.com/pion/logging v0.2.3 // indirect
	github.com/pion/mdns/v2 v2.0.7 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtcp v1.2.15 // indirect
	github.com/pion/rtp v1.8.18 // indirect
	github.com/pion/sctp v1.8.33 // indirect
	github.com/pion/sdp/v3 v3.0.9 // indirect
	github.com/pion/srtp/v3 v3.0.4 // indirect
	github.com/pion/stun/v3 v3.0.0 // indirect
	github.com/pion/transport/v3 v3.0.7 // indirect
	github.com/pion/turn/v4 v4.0.0 // indirect
	github.com/pion/webrtc/v4 v4.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/protolambda/ctxlock v0.1.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/tklauser/go-sysconf v0.3.14 // indirect
	github.com/tklauser/numcpus v0.8.0 // indirect
	github.com/wlynxg/anet v0.0.3 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.opentelemetry.io/otel v1.34.0 // indirect
	go.opentelemetry.io/otel/trace v1.34.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/blake3 v1.1.6 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	rsc.io/tmplfunc v0.0.3 // indirect
	zombiezen.com/go/sqlite v0.13.1 // indirect
)

replace github.com/erigontech/interfaces => ../../interfaces

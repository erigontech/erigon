module github.com/ledgerwatch/erigon

go 1.18

require (
	github.com/ledgerwatch/erigon-lib v0.0.0-20221214024523-b25a130fed5f
	github.com/ledgerwatch/erigon-snapshot v1.1.1-0.20221117232719-cf68648bf146
	github.com/ledgerwatch/log/v3 v3.6.0
	github.com/ledgerwatch/secp256k1 v1.0.0
)

require (
	github.com/Giulio2002/bls v0.0.0-20221130111651-ff2d397f3b99
	github.com/RoaringBitmap/roaring v1.2.1
	github.com/VictoriaMetrics/fastcache v1.12.0
	github.com/VictoriaMetrics/metrics v1.23.0
	github.com/anacrolix/sync v0.4.0
	github.com/anacrolix/torrent v1.47.0
	github.com/benesch/cgosymbolizer v0.0.0-20190515212042-bec6fe6e597b
	github.com/btcsuite/btcd v0.22.0-beta
	github.com/btcsuite/btcd/btcec/v2 v2.2.1
	github.com/c2h5oh/datasize v0.0.0-20220606134207-859f65c6625b
	github.com/consensys/gnark-crypto v0.4.1-0.20210426202927-39ac3d4b3f1f
	github.com/crate-crypto/go-ipa v0.0.0-20221111143132-9aa5d42120bc
	github.com/davecgh/go-spew v1.1.1
	github.com/deckarep/golang-set v1.8.0
	github.com/docker/docker v20.10.17+incompatible
	github.com/dop251/goja v0.0.0-20220405120441-9037c2b61cbf
	github.com/edsrzf/mmap-go v1.1.0
	github.com/emicklei/dot v1.0.0
	github.com/emirpasic/gods v1.18.1
	github.com/ferranbt/fastssz v0.0.0-20210120143747-11b9eff30ea9
	github.com/fjl/gencodec v0.0.0-20220412091415-8bb9e558978c
	github.com/gballet/go-verkle v0.0.0-20221121182333-31427a1f2d35
	github.com/goccy/go-json v0.9.7
	github.com/gofrs/flock v0.8.1
	github.com/golang-jwt/jwt/v4 v4.4.3
	github.com/golang/snappy v0.0.4
	github.com/google/btree v1.1.2
	github.com/google/go-cmp v0.5.9
	github.com/google/gofuzz v1.1.1-0.20200604201612-c04b05f3adfa
	github.com/gorilla/websocket v1.5.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/holiman/uint256 v1.2.1
	github.com/huandu/xstrings v1.3.2
	github.com/huin/goupnp v1.0.3
	github.com/jackpal/go-nat-pmp v1.0.2
	github.com/json-iterator/go v1.1.12
	github.com/julienschmidt/httprouter v1.3.0
	github.com/kevinburke/go-bindata v3.21.0+incompatible
	github.com/libp2p/go-libp2p v0.23.2
	github.com/libp2p/go-libp2p-core v0.20.1
	github.com/libp2p/go-libp2p-pubsub v0.8.1
	github.com/multiformats/go-multiaddr v0.7.0
	github.com/nxadm/tail v1.4.9-0.20211216163028-4472660a31a6
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58
	github.com/pelletier/go-toml v1.9.5
	github.com/pelletier/go-toml/v2 v2.0.6
	github.com/pion/stun v0.3.5
	github.com/pkg/errors v0.9.1
	github.com/prysmaticlabs/fastssz v0.0.0-20220628121656-93dfe28febab
	github.com/prysmaticlabs/go-bitfield v0.0.0-20210809151128-385d8c5e3fb7
	github.com/prysmaticlabs/gohashtree v0.0.0-20220517220438-192ee5ae6982
	github.com/quasilyte/go-ruleguard/dsl v0.3.21
	github.com/rs/cors v1.8.2
	github.com/spf13/cobra v1.6.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.1
	github.com/tendermint/go-amino v0.14.1
	github.com/tendermint/tendermint v0.31.12
	github.com/tidwall/btree v1.5.0
	github.com/torquem-ch/mdbx-go v0.27.0
	github.com/ugorji/go/codec v1.1.13
	github.com/ugorji/go/codec/codecgen v1.1.13
	github.com/urfave/cli/v2 v2.23.5
	github.com/valyala/fastjson v1.6.3
	github.com/xsleonard/go-merkle v1.1.0
	go.uber.org/atomic v1.10.0
	go.uber.org/zap v1.23.0
	golang.org/x/crypto v0.3.0
	golang.org/x/exp v0.0.0-20221126150942-6ab00d035af9
	golang.org/x/net v0.2.0
	golang.org/x/sync v0.1.0
	golang.org/x/sys v0.2.0
	golang.org/x/time v0.2.0
	google.golang.org/grpc v1.51.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.2.0
	google.golang.org/protobuf v1.28.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/yaml.v2 v2.4.0
	modernc.org/sqlite v1.20.0
	pgregory.net/rapid v0.5.3
)

require (
	crawshaw.io/sqlite v0.3.3-0.20210127221821-98b1f83c5508 // indirect
	github.com/ajwerner/btree v0.0.0-20211221152037-f427b3e689c0 // indirect
	github.com/alecthomas/atomic v0.1.0-alpha2 // indirect
	github.com/anacrolix/chansync v0.3.0 // indirect
	github.com/anacrolix/dht/v2 v2.19.0 // indirect
	github.com/anacrolix/envpprof v1.2.1 // indirect
	github.com/anacrolix/generics v0.0.0-20220618083756-f99e35403a60 // indirect
	github.com/anacrolix/go-libutp v1.2.0 // indirect
	github.com/anacrolix/log v0.13.2-0.20220711050817-613cb738ef30 // indirect
	github.com/anacrolix/missinggo v1.3.0 // indirect
	github.com/anacrolix/missinggo/perf v1.0.0 // indirect
	github.com/anacrolix/missinggo/v2 v2.7.0 // indirect
	github.com/anacrolix/mmsg v1.0.0 // indirect
	github.com/anacrolix/multiless v0.3.0 // indirect
	github.com/anacrolix/stm v0.4.0 // indirect
	github.com/anacrolix/upnp v0.1.3-0.20220123035249-922794e51c96 // indirect
	github.com/anacrolix/utp v0.1.0 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/benbjohnson/immutable v0.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.2 // indirect
	github.com/bradfitz/iter v0.0.0-20191230175014-e8f45d346db8 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/containerd/cgroups v1.0.4 // indirect
	github.com/coreos/go-systemd/v22 v22.4.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/davidlazar/go-crypto v0.0.0-20200604182044-b73af7476f6c // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/dlclark/regexp2 v1.4.1-0.20201116162257-a2a8dda75c91 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/elastic/gosigar v0.14.2 // indirect
	github.com/flynn/noise v1.0.0 // indirect
	github.com/francoispqt/gojay v1.2.13 // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/garslo/gogen v0.0.0-20170307003452-d6ebae628c7c // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/go-task/slim-sprig v0.0.0-20210107165309-348f09dbbbc0 // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/ianlancetaylor/cgosymbolizer v0.0.0-20220405231054-a1ae3e4bba26 // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/ipfs/go-cid v0.3.2 // indirect
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/ipfs/go-log/v2 v2.5.1 // indirect
	github.com/jbenet/go-temp-err-catcher v0.1.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/klauspost/compress v1.15.10 // indirect
	github.com/klauspost/cpuid/v2 v2.1.1 // indirect
	github.com/koron/go-ssdp v0.0.3 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/ledgerwatch/trackerslist v1.0.0 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/libp2p/go-cidranger v1.1.0 // indirect
	github.com/libp2p/go-flow-metrics v0.1.0 // indirect
	github.com/libp2p/go-libp2p-asn-util v0.2.0 // indirect
	github.com/libp2p/go-mplex v0.7.0 // indirect
	github.com/libp2p/go-msgio v0.2.0 // indirect
	github.com/libp2p/go-nat v0.1.0 // indirect
	github.com/libp2p/go-netroute v0.2.0 // indirect
	github.com/libp2p/go-openssl v0.1.0 // indirect
	github.com/libp2p/go-reuseport v0.2.0 // indirect
	github.com/libp2p/go-yamux/v4 v4.0.0 // indirect
	github.com/lispad/go-generics-tools v1.1.0 // indirect
	github.com/lucas-clemente/quic-go v0.29.1 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/marten-seemann/qtls-go1-18 v0.1.2 // indirect
	github.com/marten-seemann/qtls-go1-19 v0.1.0 // indirect
	github.com/marten-seemann/tcp v0.0.0-20210406111302-dfbc87cc63fd // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mattn/go-pointer v0.0.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/miekg/dns v1.1.50 // indirect
	github.com/mikioh/tcpinfo v0.0.0-20190314235526-30a79bb1804b // indirect
	github.com/mikioh/tcpopt v0.0.0-20190314235656-172688c1accc // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.3.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.1.0 // indirect
	github.com/multiformats/go-multiaddr-dns v0.3.1 // indirect
	github.com/multiformats/go-multiaddr-fmt v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.1.1 // indirect
	github.com/multiformats/go-multicodec v0.6.0 // indirect
	github.com/multiformats/go-multihash v0.2.1 // indirect
	github.com/multiformats/go-multistream v0.3.3 // indirect
	github.com/multiformats/go-varint v0.0.6 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/opencontainers/runtime-spec v1.0.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pion/datachannel v1.5.2 // indirect
	github.com/pion/dtls/v2 v2.1.5 // indirect
	github.com/pion/ice/v2 v2.2.6 // indirect
	github.com/pion/interceptor v0.1.11 // indirect
	github.com/pion/logging v0.2.2 // indirect
	github.com/pion/mdns v0.0.5 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtcp v1.2.9 // indirect
	github.com/pion/rtp v1.7.13 // indirect
	github.com/pion/sctp v1.8.2 // indirect
	github.com/pion/sdp/v3 v3.0.5 // indirect
	github.com/pion/srtp/v2 v2.0.9 // indirect
	github.com/pion/transport v0.13.1 // indirect
	github.com/pion/turn/v2 v2.0.8 // indirect
	github.com/pion/udp v0.1.1 // indirect
	github.com/pion/webrtc/v3 v3.1.42 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/raulk/go-watchdog v1.3.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spacemonkeygo/spacelog v0.0.0-20180420211403-2296661a0572 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/supranational/blst v0.3.10 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/whyrusleeping/timecache v0.0.0-20160911033111-cfcb2f1abfee // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.opentelemetry.io/otel v1.8.0 // indirect
	go.opentelemetry.io/otel/trace v1.8.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	golang.org/x/tools v0.3.0 // indirect
	google.golang.org/genproto v0.0.0-20211118181313-81c1377c94b1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	gotest.tools/v3 v3.3.0 // indirect
	lukechampine.com/blake3 v1.1.7 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.40.0 // indirect
	modernc.org/ccgo/v3 v3.16.13 // indirect
	modernc.org/libc v1.21.5 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.4.0 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.0.1 // indirect
)

replace github.com/tendermint/tendermint => github.com/bnb-chain/tendermint v0.31.12

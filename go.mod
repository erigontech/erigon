module github.com/ledgerwatch/erigon

go 1.18

require (
	github.com/ledgerwatch/erigon-lib v0.0.0-20220804053117-2be46669d514
	github.com/ledgerwatch/erigon-snapshot v1.0.0
	github.com/ledgerwatch/log/v3 v3.4.1
	github.com/ledgerwatch/secp256k1 v1.0.0
	github.com/ledgerwatch/trackerslist v1.0.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/RoaringBitmap/roaring v1.2.1
	github.com/VictoriaMetrics/fastcache v1.10.0
	github.com/VictoriaMetrics/metrics v1.20.1
	github.com/anacrolix/go-libutp v1.2.0
	github.com/anacrolix/log v0.13.2-0.20220427063716-a4894bb521c6
	github.com/anacrolix/torrent v1.46.1-0.20220713100403-caa9400c52fe
	github.com/benesch/cgosymbolizer v0.0.0-20190515212042-bec6fe6e597b
	github.com/btcsuite/btcd v0.22.0-beta
	github.com/c2h5oh/datasize v0.0.0-20220606134207-859f65c6625b
	github.com/consensys/gnark-crypto v0.4.0
	github.com/davecgh/go-spew v1.1.1
	github.com/deckarep/golang-set v0.0.0-20180603214616-504e848d77ea
	github.com/dop251/goja v0.0.0-20211011172007-d99e4b8cbf48
	github.com/edsrzf/mmap-go v1.1.0
	github.com/emicklei/dot v1.0.0
	github.com/emirpasic/gods v1.18.1
	github.com/fjl/gencodec v0.0.0-20220412091415-8bb9e558978c
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/goccy/go-json v0.9.7
	github.com/gofrs/flock v0.8.1
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/golang/snappy v0.0.4
	github.com/google/btree v1.1.2
	github.com/google/gofuzz v1.1.1-0.20200604201612-c04b05f3adfa
	github.com/gorilla/websocket v1.5.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/holiman/uint256 v1.2.0
	github.com/huin/goupnp v1.0.3
	github.com/jackpal/go-nat-pmp v1.0.2
	github.com/json-iterator/go v1.1.12
	github.com/julienschmidt/httprouter v1.3.0
	github.com/kevinburke/go-bindata v3.21.0+incompatible
	github.com/nxadm/tail v1.4.9-0.20211216163028-4472660a31a6
	github.com/pelletier/go-toml v1.9.5
	github.com/pelletier/go-toml/v2 v2.0.2
	github.com/pion/stun v0.3.5
	github.com/quasilyte/go-ruleguard/dsl v0.3.21
	github.com/rs/cors v1.8.2
	github.com/spf13/cobra v1.5.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.0
	github.com/tendermint/go-amino v0.14.1
	github.com/tendermint/tendermint v0.31.11
	github.com/torquem-ch/mdbx-go v0.25.2
	github.com/ugorji/go/codec v1.1.13
	github.com/ugorji/go/codec/codecgen v1.1.13
	github.com/urfave/cli v1.22.9
	github.com/valyala/fastjson v1.6.3
	github.com/xsleonard/go-merkle v1.1.0
	go.uber.org/atomic v1.9.0
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d
	golang.org/x/exp v0.0.0-20220722155223-a9213eeb770e
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f
	golang.org/x/time v0.0.0-20220609170525-579cf78fd858
	google.golang.org/grpc v1.48.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.2.0
	google.golang.org/protobuf v1.28.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	modernc.org/sqlite v1.17.3
	pgregory.net/rapid v0.4.7
)

require (
	crawshaw.io/sqlite v0.3.3-0.20210127221821-98b1f83c5508 // indirect
	github.com/ajwerner/btree v0.0.0-20211221152037-f427b3e689c0 // indirect
	github.com/anacrolix/chansync v0.3.0 // indirect
	github.com/anacrolix/dht/v2 v2.18.0 // indirect
	github.com/anacrolix/envpprof v1.2.1 // indirect
	github.com/anacrolix/generics v0.0.0-20220618083756-f99e35403a60 // indirect
	github.com/anacrolix/missinggo v1.3.0 // indirect
	github.com/anacrolix/missinggo/perf v1.0.0 // indirect
	github.com/anacrolix/missinggo/v2 v2.7.0 // indirect
	github.com/anacrolix/mmsg v1.0.0 // indirect
	github.com/anacrolix/multiless v0.3.0 // indirect
	github.com/anacrolix/stm v0.4.0 // indirect
	github.com/anacrolix/sync v0.4.0 // indirect
	github.com/anacrolix/upnp v0.1.3-0.20220123035249-922794e51c96 // indirect
	github.com/anacrolix/utp v0.1.0 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/benbjohnson/immutable v0.3.0 // indirect
	github.com/bits-and-blooms/bitset v1.2.2 // indirect
	github.com/bradfitz/iter v0.0.0-20191230175014-e8f45d346db8 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/dlclark/regexp2 v1.4.1-0.20201116162257-a2a8dda75c91 // indirect
	github.com/docker/docker v20.10.17+incompatible
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/flanglet/kanzi-go v1.9.1-0.20211212184056-72dda96261ee // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/garslo/gogen v0.0.0-20170306192744-1d203ffc1f61 // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/go-logfmt/logfmt v0.5.0 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/ianlancetaylor/cgosymbolizer v0.0.0-20220405231054-a1ae3e4bba26 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lispad/go-generics-tools v1.1.0 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
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
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/tidwall/btree v1.3.1 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	golang.org/x/mod v0.6.0-dev.0.20220106191415-9b9b3d81d5e3 // indirect
	golang.org/x/net v0.0.0-20220630215102-69896b714898 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.10 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
	google.golang.org/genproto v0.0.0-20200825200019-8632dd797987 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/uint128 v1.1.1 // indirect
	modernc.org/cc/v3 v3.36.0 // indirect
	modernc.org/ccgo/v3 v3.16.6 // indirect
	modernc.org/libc v1.16.7 // indirect
	modernc.org/mathutil v1.4.1 // indirect
	modernc.org/memory v1.1.1 // indirect
	modernc.org/opt v0.1.1 // indirect
	modernc.org/strutil v1.1.1 // indirect
	modernc.org/token v1.0.0 // indirect
)

require (
	github.com/alecthomas/atomic v0.1.0-alpha2 // indirect
	gotest.tools/v3 v3.3.0 // indirect
)

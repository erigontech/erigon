module github.com/ledgerwatch/erigon

go 1.18

replace github.com/etcd-io/bbolt => go.etcd.io/bbolt v1.3.5

require (
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/VictoriaMetrics/fastcache v1.10.0
	github.com/VictoriaMetrics/metrics v1.18.1
	github.com/anacrolix/go-libutp v1.2.0
	github.com/anacrolix/log v0.13.1
	github.com/anacrolix/torrent v1.8.2
	github.com/btcsuite/btcd v0.22.0-beta
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2
	github.com/consensys/gnark-crypto v0.4.0
	github.com/davecgh/go-spew v1.1.1
	github.com/deckarep/golang-set v0.0.0-20180603214616-504e848d77ea
	github.com/edsrzf/mmap-go v1.0.0
	github.com/emicklei/dot v0.16.0
	github.com/emirpasic/gods v1.12.0
	github.com/fjl/gencodec v0.0.0-20191126094850-e283372f291f
	github.com/goccy/go-json v0.9.7
	github.com/gofrs/flock v0.8.1
	github.com/golang-jwt/jwt/v4 v4.3.0
	github.com/golang/snappy v0.0.4
	github.com/google/btree v1.0.1
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
	github.com/ledgerwatch/erigon-lib v0.0.0-20220426111915-6745c226947e
	github.com/ledgerwatch/log/v3 v3.4.1
	github.com/ledgerwatch/secp256k1 v1.0.0
	github.com/pelletier/go-toml v1.9.5
	github.com/pelletier/go-toml/v2 v2.0.0-beta.8
	github.com/quasilyte/go-ruleguard/dsl v0.3.19
	github.com/rs/cors v1.8.2
	github.com/shirou/gopsutil/v3 v3.22.2
	github.com/spf13/cobra v1.4.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.1
	github.com/tendermint/go-amino v0.14.1
	github.com/tendermint/iavl v0.12.0
	github.com/tendermint/tendermint v0.31.11
	github.com/torquem-ch/mdbx-go v0.23.2
	github.com/ugorji/go/codec v1.1.13
	github.com/ugorji/go/codec/codecgen v1.1.13
	github.com/urfave/cli v1.22.5
	github.com/valyala/fastjson v1.6.3
	github.com/wcharczuk/go-chart/v2 v2.1.0
	github.com/xsleonard/go-merkle v1.1.0
	go.uber.org/atomic v1.9.0
	golang.org/x/crypto v0.0.0-20220411220226-7b82a4e95df4
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220422013727-9388b58f7150
	golang.org/x/time v0.0.0-20220411224347-583f2d630306
	google.golang.org/grpc v1.45.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.2.0
	google.golang.org/protobuf v1.28.0
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b
	gopkg.in/olebedev/go-duktape.v3 v3.0.0-20200619000410-60c24ae608a6
	modernc.org/sqlite v1.14.2-0.20211125151325-d4ed92c0a70f
	pgregory.net/rapid v0.4.7
)

require (
	github.com/VividCortex/gohistogram v1.0.0 // indirect
	github.com/anacrolix/dht/v2 v2.2.0 // indirect
	github.com/anacrolix/envpprof v1.2.1 // indirect
	github.com/anacrolix/missinggo v1.3.0 // indirect
	github.com/anacrolix/missinggo/perf v1.0.0 // indirect
	github.com/anacrolix/missinggo/v2 v2.5.4-0.20220317032254-8c5ea4947a0b // indirect
	github.com/anacrolix/mmsg v1.0.0 // indirect
	github.com/anacrolix/sync v0.4.0 // indirect
	github.com/anacrolix/upnp v0.1.3-0.20220123035249-922794e51c96 // indirect
	github.com/anacrolix/utp v0.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/bradfitz/iter v0.0.0-20191230175014-e8f45d346db8 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/etcd-io/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/flanglet/kanzi-go v1.9.1-0.20211212184056-72dda96261ee // indirect
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/garslo/gogen v0.0.0-20170306192744-1d203ffc1f61 // indirect
	github.com/go-kit/kit v0.9.0 // indirect
	github.com/go-logfmt/logfmt v0.4.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jmhodges/levigo v1.0.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/kr/logfmt v0.0.0-20140226030751-b84e30acd515 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-sqlite3 v1.14.9 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.17.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.5.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.9.1 // indirect
	github.com/prometheus/procfs v0.0.11 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	github.com/willf/bloom v2.0.3+incompatible // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	golang.org/x/image v0.0.0-20200927104501-e162460cd6b5 // indirect
	golang.org/x/mod v0.6.0-dev.0.20220106191415-9b9b3d81d5e3 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.10 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
	lukechampine.com/uint128 v1.1.1 // indirect
	modernc.org/cc/v3 v3.35.18 // indirect
	modernc.org/ccgo/v3 v3.12.73 // indirect
	modernc.org/libc v1.11.82 // indirect
	modernc.org/mathutil v1.4.1 // indirect
	modernc.org/memory v1.0.5 // indirect
	modernc.org/opt v0.1.1 // indirect
	modernc.org/strutil v1.1.1 // indirect
	modernc.org/token v1.0.0 // indirect
)

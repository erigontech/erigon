module github.com/ledgerwatch/erigon

go 1.18

replace github.com/etcd-io/bbolt => go.etcd.io/bbolt v1.3.5

require (
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/VictoriaMetrics/fastcache v1.10.0
	github.com/VictoriaMetrics/metrics v1.18.1
	github.com/anacrolix/go-libutp v1.2.0
	github.com/anacrolix/log v0.13.1
	github.com/anacrolix/torrent v1.42.0
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
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/jmhodges/levigo v1.0.0 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
)

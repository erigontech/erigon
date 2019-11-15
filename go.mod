module github.com/ledgerwatch/turbo-geth

go 1.13

require (
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/go-autorest/autorest/adal v0.8.0 // indirect
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/allegro/bigcache v0.0.0-20181022200625-bff00e20c68d
	github.com/apilayer/freegeoip v3.5.0+incompatible
	github.com/aristanetworks/goarista v0.0.0-20170210015632-ea17b1a17847
	github.com/blend/go-sdk v2.0.0+incompatible // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/btcsuite/btcd v0.0.0-20171128150713-2e60448ffcc6
	github.com/cespare/cp v0.1.0
	github.com/cloudflare/cloudflare-go v0.10.6
	github.com/davecgh/go-spew v1.1.1
	github.com/deckarep/golang-set v0.0.0-20180603214616-504e848d77ea
	github.com/dgraph-io/badger v1.6.0
	github.com/docker/docker v0.0.0-20180625184442-8e610b2b55bf
	github.com/edsrzf/mmap-go v0.0.0-20160512033002-935e0e8a636c
	github.com/elastic/gosigar v0.0.0-20180330100440-37f05ff46ffa
	github.com/fatih/color v1.3.0
	github.com/fjl/memsize v0.0.0-20180418122429-ca190fb6ffbc
	github.com/gballet/go-libpcsclite v0.0.0-20190607065134-2772fd86a8ff
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-stack/stack v1.8.0
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/gorilla/websocket v1.4.1
	github.com/graph-gophers/graphql-go v0.0.0-20190830041028-33de425462b0
	github.com/hashicorp/golang-lru v0.0.0-20160813221303-0a025b7e63ad
	github.com/howeyc/fsnotify v0.9.0 // indirect
	github.com/huin/goupnp v0.0.0-20161224104101-679507af18f3
	github.com/influxdata/influxdb v0.0.0-20180221223340-01288bdb0883
	github.com/jackpal/go-nat-pmp v0.0.0-20160603034137-1fa385a6f458
	github.com/julienschmidt/httprouter v1.2.0
	github.com/karalabe/usb v0.0.0-20190919080040-51dc0efba356
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/ledgerwatch/bolt v1.4.1
	github.com/llgcode/draw2d v0.0.0-20180825133448-f52c8a71aff0
	github.com/mattn/go-colorable v0.1.0
	github.com/mattn/go-isatty v0.0.0-20180830101745-3fb116b82035
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826
	github.com/naoina/go-stringutil v0.1.0 // indirect
	github.com/naoina/toml v0.0.0-20170918210437-9fafd6967416
	github.com/olekukonko/tablewriter v0.0.1
	github.com/oschwald/maxminddb-golang v1.5.0 // indirect
	github.com/pborman/uuid v0.0.0-20170112150404-1b00554d8222
	github.com/petar/GoLLRB v0.0.0-20190514000832-33fb24c13b99
	github.com/peterh/liner v0.0.0-20190123174540-a2c9a5303de7
	github.com/prometheus/tsdb v0.10.0
	github.com/rjeczalik/notify v0.9.1
	github.com/robertkrimen/otto v0.0.0-20170205013659-6a77b7cbc37d
	github.com/rs/cors v0.0.0-20160617231935-a62a804a8a00
	github.com/rs/xhandler v0.0.0-20170707052532-1eb70cf1520d // indirect
	github.com/status-im/keycard-go v0.0.0-20190424133014-d95853db0f48
	github.com/steakknife/bloomfilter v0.0.0-20180922174646-6819c0d2a570
	github.com/steakknife/hamming v0.0.0-20180906055917-c99c65617cd3 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/tyler-smith/go-bip39 v1.0.2
	github.com/ugorji/go/codec v1.1.7
	github.com/urfave/cli v1.22.1
	github.com/valyala/bytebufferpool v1.0.0
	github.com/wcharczuk/go-chart v2.0.1+incompatible
	github.com/wsddn/go-ecdh v0.0.0-20161211032359-48726bab9208
	golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2
	golang.org/x/net v0.0.0-20191014212845-da9a3fd4c582
	golang.org/x/sys v0.0.0-20191105231009-c1f44814a5cd
	golang.org/x/text v0.3.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/natefinch/npipe.v2 v2.0.0-20160621034901-c1b8fa8bdcce
	gopkg.in/olebedev/go-duktape.v3 v3.0.0-20180302121509-abf0ba0be5d5
	gopkg.in/sourcemap.v1 v1.0.5 // indirect
	gotest.tools v2.2.0+incompatible // indirect
)

replace github.com/rjeczalik/notify => github.com/JekaMas/notify v0.9.4

module github.com/ledgerwatch/turbo-geth

go 1.13

require (
	github.com/Azure/azure-pipeline-go v0.2.2 // indirect
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/go-autorest/autorest/adal v0.8.3 // indirect
	github.com/JekaMas/notify v0.9.4
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/VictoriaMetrics/fastcache v1.5.7
	github.com/aristanetworks/goarista v0.0.0-20170210015632-ea17b1a17847
	github.com/aws/aws-sdk-go v1.28.9
	github.com/blend/go-sdk v2.0.0+incompatible // indirect
	github.com/btcsuite/btcd v0.0.0-20171128150713-2e60448ffcc6
	github.com/c2h5oh/datasize v0.0.0-20200112174442-28bbd4740fee
	github.com/cespare/cp v0.1.0
	github.com/cloudflare/cloudflare-go v0.10.6
	github.com/davecgh/go-spew v1.1.1
	github.com/deckarep/golang-set v0.0.0-20180603214616-504e848d77ea
	github.com/dlclark/regexp2 v1.2.0 // indirect
	github.com/docker/docker v1.4.2-0.20180625184442-8e610b2b55bf
	github.com/dop251/goja v0.0.0-20200219165308-d1232e640a87
	github.com/edsrzf/mmap-go v0.0.0-20160512033002-935e0e8a636c
	github.com/emicklei/dot v0.11.0
	github.com/ethereum/evmc/v7 v7.3.0
	github.com/fatih/color v1.7.0
	github.com/fjl/gencodec v0.0.0-20191126094850-e283372f291f
	github.com/gballet/go-libpcsclite v0.0.0-20190607065134-2772fd86a8ff
	github.com/gin-gonic/gin v1.6.2
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/go-stack/stack v1.8.0
	github.com/golang/protobuf v1.4.2
	github.com/golang/snappy v0.0.2-0.20200707131729-196ae77b8a26
	github.com/gorilla/websocket v1.4.1
	github.com/graph-gophers/graphql-go v0.0.0-20191115155744-f33e81362277
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/holiman/uint256 v1.1.1
	github.com/huin/goupnp v1.0.0
	github.com/influxdata/influxdb v1.2.3-0.20180221223340-01288bdb0883
	github.com/jackpal/go-nat-pmp v1.0.2-0.20160603034137-1fa385a6f458
	github.com/julienschmidt/httprouter v1.2.0
	github.com/karalabe/usb v0.0.0-20190919080040-51dc0efba356
	github.com/kevinburke/go-bindata v3.21.0+incompatible
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/ledgerwatch/bolt v1.4.6-0.20200605053542-69293d8f1d33
	github.com/ledgerwatch/lmdb-go v1.13.1-0.20200829020305-221d50cfedab
	github.com/llgcode/draw2d v0.0.0-20180825133448-f52c8a71aff0
	github.com/logrusorgru/aurora v2.0.3+incompatible
	github.com/mattn/go-colorable v0.1.2
	github.com/mattn/go-isatty v0.0.12
	github.com/mitchellh/hashstructure v1.0.0
	github.com/olekukonko/tablewriter v0.0.2-0.20190409134802-7e037d187b0c
	github.com/pborman/uuid v0.0.0-20170112150404-1b00554d8222
	github.com/petar/GoLLRB v0.0.0-20190514000832-33fb24c13b99
	github.com/peterh/liner v1.1.1-0.20190123174540-a2c9a5303de7
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.10.0
	github.com/prometheus/tsdb v0.10.0
	github.com/rs/cors v0.0.0-20160617231935-a62a804a8a00
	github.com/rs/xhandler v0.0.0-20170707052532-1eb70cf1520d // indirect
	github.com/shirou/gopsutil v2.20.5+incompatible
	github.com/spf13/cobra v1.0.0
	github.com/status-im/keycard-go v0.0.0-20190424133014-d95853db0f48
	github.com/stretchr/testify v1.6.1
	github.com/tyler-smith/go-bip39 v1.0.2
	github.com/ugorji/go/codec v1.1.7
	github.com/urfave/cli v1.22.1
	github.com/wcharczuk/go-chart v2.0.1+incompatible
	github.com/wsddn/go-ecdh v0.0.0-20161211032359-48726bab9208
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/net v0.0.0-20200625001655-4c5254603344
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200615200032-f1bc736245b1
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0
	golang.org/x/tools v0.0.0-20191126055441-b0650ceb63d9
	google.golang.org/grpc v1.30.1
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v0.0.0-20200825174526-e13e057332a8
	google.golang.org/protobuf v1.25.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
	gopkg.in/natefinch/npipe.v2 v2.0.0-20160621034901-c1b8fa8bdcce
	gopkg.in/olebedev/go-duktape.v3 v3.0.0-20200619000410-60c24ae608a6
	gotest.tools v2.2.0+incompatible // indirect
)

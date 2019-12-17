install:
	go get -u github.com/rs/jplot
	go get -u github.com/rs/jaggr
	go get -u github.com/tsenart/vegeta

mac-environment:
	echo 'for Mac sertup follow https://gist.github.com/tombigel/d503800a282fcadbee14b537735d202c'

tmpDir = "/var/folders/x_/1mnbt25s3291zr5_fxhjfnq9n86kng/T/"

run-vegeta:
	cat $(tmpDir)/turbo_geth_stress_test/vegeta_geth_debug_storageRangeAt.csv | vegeta attack -rate=600 -format=json -duration=20s | vegeta plot > plot.html
	open plot.html

run-rpctest:
 	go run ./cmd/rpctest --action=bench1

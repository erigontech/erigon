.PHONY: clean setup example run clean


tests:
	wget https://github.com/ethereum/consensus-spec-tests/releases/download/v1.5.0/mainnet.tar.gz
	tar xf mainnet.tar.gz --no-xattrs 
	rm mainnet.tar.gz
	# not needed for now
	rm -rf tests/mainnet/eip6110
	rm -rf tests/mainnet/whisk
	rm -rf tests/mainnet/fulu
	rm -rf tests/mainnet/eip7441
	rm -rf tests/mainnet/eip7732

clean:
	rm -rf tests

mainnet:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet/ -v --timeout 30m

whisk:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet/whisk/ -v --timeout 30m

eip7594:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet/eip7594/ -v --timeout 30m

capella:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet/capella/ -v --timeout 30m

deneb:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet/deneb/ -v --timeout 30m

.PHONY: clean setup example run clean


tests:
	wget https://github.com/ethereum/consensus-spec-tests/releases/download/v1.6.0-alpha.4/mainnet.tar.gz
	tar xf mainnet.tar.gz
	rm mainnet.tar.gz
	# not needed for now
	rm -rf tests/mainnet/eip6110
	rm -rf tests/mainnet/whisk
	rm -rf tests/mainnet/eip7441
	rm -rf tests/mainnet/eip7732
	rm -rf tests/mainnet/eip7805

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

electra:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet/electra/ -v --timeout 30m

fulu:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet/fulu/ -v --timeout 30m

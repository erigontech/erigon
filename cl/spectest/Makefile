.PHONY: clean setup example run clean


tests:
	wget https://github.com/ethereum/consensus-spec-tests/releases/download/v1.4.0-beta.5/mainnet.tar.gz
	tar xf mainnet.tar.gz
	rm mainnet.tar.gz
	# not needed for now
	rm -rf tests/mainnet/eip6110
clean:
	rm -rf tests

mainnet:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet -v --timeout 30m

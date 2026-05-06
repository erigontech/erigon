.PHONY: clean setup example run clean

tests:
	../../tools/test-fixtures.sh ../../test-fixtures.json ../../test-fixtures-cache
	# not needed for now (run after every fixture refresh in case the
	# tarball was re-extracted)
	rm -rf ../../test-fixtures-cache/cl_mainnet/tests/mainnet/eip6110
	rm -rf ../../test-fixtures-cache/cl_mainnet/tests/mainnet/whisk
	rm -rf ../../test-fixtures-cache/cl_mainnet/tests/mainnet/eip7441
	rm -rf ../../test-fixtures-cache/cl_mainnet/tests/mainnet/eip7732
	rm -rf ../../test-fixtures-cache/cl_mainnet/tests/mainnet/eip7805
	rm -rf ../../test-fixtures-cache/cl_mainnet/tests/mainnet/gloas

clean:
	rm -rf ../../test-fixtures-cache/cl_mainnet

mainnet:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go test -run=/mainnet/ -v --timeout 30m

whisk:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go test -run=/mainnet/whisk/ -v --timeout 30m

eip7594:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go test -run=/mainnet/eip7594/ -v --timeout 30m

capella:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go test -run=/mainnet/capella/ -v --timeout 30m

deneb:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go test -run=/mainnet/deneb/ -v --timeout 30m

electra:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go test -run=/mainnet/electra/ -v --timeout 30m

fulu:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go test -run=/mainnet/fulu/ -v --timeout 30m

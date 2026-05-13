.PHONY: clean setup example run clean

tests:
	$(MAKE) -C ../.. test-fixtures-cl

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

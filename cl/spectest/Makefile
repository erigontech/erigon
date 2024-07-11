.PHONY: clean setup example run clean


tests:
	GIT_LFS_SKIP_SMUDGE=1 GIT_CLONE_PROTECTION_ACTIVE=false git clone https://github.com/ethereum/consensus-spec-tests
	cd consensus-spec-tests && git checkout 080c96fbbf3be58e75947debfeb9ba3b2b7c9748 && git lfs pull --exclude=tests/general,tests/minimal && cd ..
	mv consensus-spec-tests/tests .
	rm -rf consensus-spec-tests
	rm -rf tests/minimal
	# not needed for now
	rm -rf tests/mainnet/eip6110
clean:
	rm -rf tests

mainnet:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet -failfast -v --timeout 30m

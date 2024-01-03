.PHONY: clean setup example run clean


tests:
	GIT_LFS_SKIP_SMUDGE=1 git clone https://github.com/ethereum/consensus-spec-tests
	cd consensus-spec-tests && git checkout 99549a414c10baa9e69abcb08eb256fc1a8d54f6 && git lfs pull --exclude=tests/general,tests/minimal && cd ..
	mv consensus-spec-tests/tests .
	rm -rf consensus-spec-tests
	rm -rf tests/minimal
	# not needed for now
	rm -rf tests/mainnet/eip6110
	# will not implement until i see it on a testnet
	rm -rf tests/mainnet/deneb

clean:
	rm -rf tests

mainnet:
	CGO_CFLAGS=-D__BLST_PORTABLE__ go  test -tags=spectest -run=/mainnet -failfast -v --timeout 30m
